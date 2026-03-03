from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime
from urllib.parse import urlencode, urljoin

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.cache import OpdsCache
from app.db_migrations import run_migrations
from app.harvest import run_incremental_for_all_checkpoints
from app.scheduler import IncrementalHarvestScheduler
from app.sources import extract_json_records, iter_json_records, iter_json_records_from_url, load_json_payload_from_url, load_oai_dc_records
from app.store import IngestResult, PublicationStore, RepositoryConfig
from app.transform import (
    first_valid_publisher,
    native_name_for_language,
    normalize_json_record,
    normalize_language_value,
    normalize_oai_record,
    primary_publisher_name,
)
from app.validation import validate_palace_opds_feed

DEFAULT_REPOSITORY_ID = "default"
DEFAULT_REPOSITORY_NAME = "Default OPDS Repository"

app = FastAPI(title="OAPEN OPDS Feed Generator", version="0.2.0")
store = PublicationStore(os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db"))
opds_cache = OpdsCache()
harvest_scheduler = IncrementalHarvestScheduler(
    store=store,
    hour_utc=int(os.getenv("SCHEDULER_DAILY_UTC_HOUR", "2")),
    minute_utc=int(os.getenv("SCHEDULER_DAILY_UTC_MINUTE", "0")),
)


class JsonIngestRequest(BaseModel):
    path: str = Field(description="Absolute or workspace-relative path to JSON metadata file.")


class JsonUrlIngestRequest(BaseModel):
    url: str = Field(description="HTTP(S) URL to a JSON metadata file.")


class OpdsJsonIngestRequest(BaseModel):
    url: str = Field(description="HTTP(S) URL to an OPDS-like JSON feed.")
    max_records: int | None = Field(default=None, ge=1)
    max_pages: int | None = Field(default=None, ge=1)
    follow_next: bool = True
    timeout_seconds: int = Field(default=120, ge=1, le=600)
    incremental: bool = True
    checkpoint_key: str | None = None


class IngestJobRequest(BaseModel):
    url: str = Field(description="HTTP(S) URL to a JSON metadata file.")


class OaiIngestRequest(BaseModel):
    base_url: str
    metadata_prefix: str = "oai_dc"
    set_name: str | None = None
    from_date: str | None = None
    until_date: str | None = None
    max_records: int | None = None
    incremental: bool = True
    checkpoint_key: str | None = None


class ManualHarvestRequest(BaseModel):
    max_records: int | None = None


class RepositoryUpsertRequest(BaseModel):
    source_type: str
    name: str
    config: dict = Field(default_factory=dict)
    is_active: bool = True


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


ingest_jobs: dict[str, dict] = {}
ingest_jobs_lock = threading.Lock()


def _cache_namespace(repository_id: str) -> str:
    return f"repo:{repository_id}"


def _checkpoint_key(repository_id: str, base_url: str, metadata_prefix: str, set_name: str | None) -> str:
    return f"{repository_id}|{base_url}|{metadata_prefix}|{set_name or 'default'}"


def _today_ymd() -> str:
    return datetime.now(UTC).date().isoformat()


def _max_date_string(values: list[str | None]) -> str | None:
    candidates = sorted([value for value in values if value])
    return candidates[-1] if candidates else None


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _cache_invalidate_every_n_upserts() -> int:
    raw = os.getenv("OPDS_CACHE_INVALIDATE_EVERY_N_UPSERTS", "0").strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(value, 0)


def _ensure_default_repository() -> None:
    existing = store.get_repository(DEFAULT_REPOSITORY_ID)
    if existing is not None:
        return
    store.upsert_repository(
        RepositoryConfig(
            repository_id=DEFAULT_REPOSITORY_ID,
            source_type="mixed",
            name=DEFAULT_REPOSITORY_NAME,
            config={},
            is_active=True,
            updated_at="",
            created_at="",
        )
    )


def _set_repository_on_publication(publication, repository_id: str):
    publication.repository_id = repository_id
    if not publication.source_publication_id:
        publication.source_publication_id = publication.publication_id
    return publication


def _maybe_invalidate_opds_cache_during_ingest(
    accepted_count: int,
    last_invalidation_count: int,
    repository_id: str,
) -> int:
    invalidate_every = _cache_invalidate_every_n_upserts()
    if invalidate_every <= 0:
        return last_invalidation_count
    if accepted_count - last_invalidation_count < invalidate_every:
        return last_invalidation_count
    _invalidate_opds_cache(repository_id)
    return accepted_count


def _ingest_json(path: str, repository_id: str = DEFAULT_REPOSITORY_ID) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    last_invalidation_count = 0
    for raw in iter_json_records(path):
        normalized = normalize_json_record(raw)
        if normalized is None:
            result.rejected += 1
            continue
        store.upsert(_set_repository_on_publication(normalized, repository_id))
        result.accepted += 1
        last_invalidation_count = _maybe_invalidate_opds_cache_during_ingest(
            accepted_count=result.accepted,
            last_invalidation_count=last_invalidation_count,
            repository_id=repository_id,
        )
    return result


def _ingest_json_url(url: str, repository_id: str = DEFAULT_REPOSITORY_ID) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    last_invalidation_count = 0
    for raw in iter_json_records_from_url(url):
        normalized = normalize_json_record(raw)
        if normalized is None:
            result.rejected += 1
            continue
        store.upsert(_set_repository_on_publication(normalized, repository_id))
        result.accepted += 1
        last_invalidation_count = _maybe_invalidate_opds_cache_during_ingest(
            accepted_count=result.accepted,
            last_invalidation_count=last_invalidation_count,
            repository_id=repository_id,
        )
    return result


def _opds_json_checkpoint_key(repository_id: str, url: str) -> str:
    return f"opds-json|{repository_id}|{url}"


def _extract_opds_next_url(payload: object, current_url: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    links = payload.get("links")
    if not isinstance(links, list):
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        rel = link.get("rel")
        rels = rel if isinstance(rel, list) else [rel]
        normalized_rels = {str(item).strip().casefold() for item in rels if isinstance(item, str) and item.strip()}
        if "next" not in normalized_rels:
            continue
        href = link.get("href")
        if isinstance(href, str) and href.strip():
            return urljoin(current_url, href.strip())
    return None


def _ingest_opds_json(request: OpdsJsonIngestRequest, repository_id: str = DEFAULT_REPOSITORY_ID) -> tuple[IngestResult, dict]:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    last_invalidation_count = 0
    checkpoint_key = request.checkpoint_key or _opds_json_checkpoint_key(repository_id, request.url)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    effective_url = request.url
    if checkpoint and isinstance(checkpoint.state, dict):
        next_url = checkpoint.state.get("next_url")
        if isinstance(next_url, str) and next_url.strip():
            effective_url = next_url.strip()

    current_url = effective_url
    pages_crawled = 0
    records_processed = 0
    last_url = effective_url
    next_url_to_store = request.url

    while current_url:
        payload = load_json_payload_from_url(current_url, timeout_seconds=request.timeout_seconds)
        last_url = current_url
        pages_crawled += 1
        for raw in extract_json_records(payload):
            normalized = normalize_json_record(raw)
            records_processed += 1
            if normalized is None:
                result.rejected += 1
            else:
                store.upsert(_set_repository_on_publication(normalized, repository_id))
                result.accepted += 1
                last_invalidation_count = _maybe_invalidate_opds_cache_during_ingest(
                    accepted_count=result.accepted,
                    last_invalidation_count=last_invalidation_count,
                    repository_id=repository_id,
                )
            if request.max_records and records_processed >= request.max_records:
                break

        next_url = _extract_opds_next_url(payload, current_url) if request.follow_next else None
        if request.max_records and records_processed >= request.max_records:
            next_url_to_store = next_url or request.url
            break
        if request.max_pages and pages_crawled >= request.max_pages:
            next_url_to_store = next_url or request.url
            break
        if not request.follow_next or not next_url:
            next_url_to_store = request.url
            break
        next_url_to_store = next_url
        current_url = next_url

    if request.incremental:
        store.upsert_checkpoint(
            checkpoint_key=checkpoint_key,
            repository_id=repository_id,
            source_type="opds-json",
            base_url=request.url,
            metadata_prefix="opds-json",
            set_name=None,
            last_from_date=None,
            last_until_date=_today_ymd(),
            state={
                "next_url": next_url_to_store,
                "last_url": last_url,
                "pages_crawled": pages_crawled,
                "records_processed": records_processed,
            },
        )

    return result, {
        "checkpoint_key": checkpoint_key,
        "effective_url": effective_url,
        "pages_crawled": pages_crawled,
        "records_processed": records_processed,
    }


def _set_job_state(job_id: str, **updates) -> None:
    with ingest_jobs_lock:
        existing = ingest_jobs.get(job_id)
        if existing is None:
            return
        existing.update(updates)


def _run_json_url_ingest_job(job_id: str, url: str, repository_id: str) -> None:
    _set_job_state(job_id, status="running", started_at=_utcnow_iso())
    try:
        result = _ingest_json_url(url, repository_id=repository_id)
        _invalidate_opds_cache(repository_id)
        _set_job_state(
            job_id,
            status="completed",
            completed_at=_utcnow_iso(),
            accepted=result.accepted,
            rejected=result.rejected,
            total_indexed=store.count(repository_id=repository_id),
        )
    except Exception as exc:
        _set_job_state(job_id, status="failed", completed_at=_utcnow_iso(), error=str(exc))


def _ingest_oai(request: OaiIngestRequest, repository_id: str = DEFAULT_REPOSITORY_ID) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    last_invalidation_count = 0
    checkpoint_key = request.checkpoint_key or _checkpoint_key(repository_id, request.base_url, request.metadata_prefix, request.set_name)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    effective_from = request.from_date or (checkpoint.last_until_date if checkpoint else None)
    effective_until = request.until_date or _today_ymd()

    records = load_oai_dc_records(
        base_url=request.base_url,
        metadata_prefix=request.metadata_prefix,
        set_name=request.set_name,
        from_date=effective_from,
        until_date=effective_until,
        max_records=request.max_records,
    )
    harvested_dates: list[str | None] = []
    for fields in records:
        normalized = normalize_oai_record(fields)
        if normalized is None:
            result.rejected += 1
            continue
        store.upsert(_set_repository_on_publication(normalized, repository_id))
        harvested_dates.append(normalized.published)
        result.accepted += 1
        last_invalidation_count = _maybe_invalidate_opds_cache_during_ingest(
            accepted_count=result.accepted,
            last_invalidation_count=last_invalidation_count,
            repository_id=repository_id,
        )

    if request.incremental:
        latest_harvested = _max_date_string(harvested_dates) or effective_until
        store.upsert_checkpoint(
            checkpoint_key=checkpoint_key,
            repository_id=repository_id,
            source_type="oai-pmh",
            base_url=request.base_url,
            metadata_prefix=request.metadata_prefix,
            set_name=request.set_name,
            last_from_date=effective_from,
            last_until_date=latest_harvested,
            state=None,
        )
    return result


def _to_opds_publication(pub, base_url: str | None = None, repository_id: str | None = None) -> dict:
    def without_none_values(value: dict) -> dict:
        return {key: item for key, item in value.items() if item is not None}

    def to_rfc3339(value: str | None) -> str | None:
        if not value:
            return None
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        if " " in candidate and "T" not in candidate:
            candidate = candidate.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.isoformat()

    modified = to_rfc3339(pub.published)
    raw = pub.raw if isinstance(pub.raw, dict) else {}
    metadata_src = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}

    schema_type = "http://schema.org/Book"
    raw_schema_type = metadata_src.get("@type") if isinstance(metadata_src.get("@type"), str) else None
    if isinstance(raw_schema_type, str) and raw_schema_type.strip():
        schema_type = raw_schema_type.strip()
    else:
        content_like_values = [
            raw.get("contentType"),
            raw.get("publicationType"),
            metadata_src.get("contentType"),
            metadata_src.get("publicationType"),
        ]
        normalized_values = {str(value).strip().casefold() for value in content_like_values if isinstance(value, str) and value.strip()}
        if "chapter" in normalized_values:
            schema_type = "http://schema.org/Chapter"

    image_links = []
    for image in _as_list(raw.get("images")):
        if not isinstance(image, dict):
            continue
        href = image.get("href")
        if not isinstance(href, str) or not href:
            continue
        image_links.append(
            {
                "href": href,
                "type": image.get("type") or "image/jpeg",
            }
        )

    publication_path = f"/publications/{pub.publication_id}"
    if repository_id and repository_id != DEFAULT_REPOSITORY_ID:
        publication_path = f"/repositories/{repository_id}/publications/{pub.publication_id}"

    links = pub.links or [
        {
            "rel": "self",
            "href": f"{base_url}{publication_path}" if base_url else publication_path,
            "type": "application/opds+json",
        }
    ]

    alt_identifiers = []
    doi = metadata_src.get("doi")
    if isinstance(doi, str) and doi.strip():
        doi_value = doi.strip()
        if doi_value.lower().startswith("doi:"):
            doi_value = doi_value[4:].strip()
        alt_identifiers.append(f"https://doi.org/{doi_value}")
    for isbn in _as_list(metadata_src.get("isbn")):
        if isinstance(isbn, str) and isbn.strip():
            alt_identifiers.append(f"urn:isbn:{isbn.strip()}")

    series_entry = None
    if pub.series_name:
        series_entry = {"name": pub.series_name}
        if pub.series_position is not None:
            series_entry["position"] = pub.series_position
        if pub.series_slug:
            if repository_id and repository_id != DEFAULT_REPOSITORY_ID:
                href = f"/repositories/{repository_id}/opds/series/{pub.series_slug}"
            else:
                href = f"/opds/series/{pub.series_slug}"
            if base_url:
                href = f"{base_url}{href}"
            series_entry["links"] = [{"href": href, "type": "application/opds+json"}]
    collection_value = None
    if pub.collection:
        collection_value = pub.collection

    accessibility = []
    for item in _as_list(metadata_src.get("accessibility")):
        if isinstance(item, dict):
            accessibility.append(item)

    author = [{"name": name} for name in pub.authors] if pub.authors else []
    subject = [{"name": value} for value in pub.subjects] if pub.subjects else []

    description = metadata_src.get("description")
    if not isinstance(description, str) or not description.strip():
        raw_description = raw.get("description")
        description = raw_description if isinstance(raw_description, str) and raw_description.strip() else None

    metadata = without_none_values(
        {
            "@type": schema_type,
            "title": pub.title,
            "identifier": pub.identifier or pub.publication_id,
            "modified": modified,
            "published": modified,
            "author": author,
            "subject": subject,
        }
    )
    if pub.language:
        metadata["language"] = pub.language

    publisher = first_valid_publisher(raw.get("publisher"), metadata_src.get("publisher"), metadata_src.get("imprint"), pub.publisher)
    if publisher and pub.publisher_slug:
        if repository_id and repository_id != DEFAULT_REPOSITORY_ID:
            publisher_href = f"/repositories/{repository_id}/opds/publishers/{pub.publisher_slug}"
        else:
            publisher_href = f"/opds/publishers/{pub.publisher_slug}"
        publisher_link = {
            "href": f"{base_url}{publisher_href}" if base_url else publisher_href,
            "type": "application/opds+json",
        }
        if isinstance(publisher, str):
            publisher = {"name": publisher, "links": [publisher_link]}
        elif isinstance(publisher, dict):
            publisher = {**publisher, "links": [publisher_link]}
        elif isinstance(publisher, list):
            primary_name = primary_publisher_name(publisher)
            linked_publishers = []
            for item in publisher:
                if item.get("name") == primary_name:
                    linked_publishers.append({**item, "links": [publisher_link]})
                else:
                    linked_publishers.append(item)
            publisher = linked_publishers
    if publisher:
        metadata["publisher"] = publisher

    if description:
        metadata["description"] = description
    belongs_to_obj = {}
    if series_entry:
        belongs_to_obj["series"] = series_entry
    if collection_value:
        collection_object: dict[str, object] = {"name": collection_value}
        collection_slug = pub.collection_slug
        if collection_slug:
            if repository_id and repository_id != DEFAULT_REPOSITORY_ID:
                collection_href = f"/repositories/{repository_id}/opds/collections/{collection_slug}"
            else:
                collection_href = f"/opds/collections/{collection_slug}"
            collection_object["links"] = [
                {
                    "href": f"{base_url}{collection_href}" if base_url else collection_href,
                    "type": "application/opds+json",
                }
            ]
        belongs_to_obj["collection"] = collection_object
    if belongs_to_obj:
        metadata["belongsTo"] = belongs_to_obj
    if alt_identifiers:
        metadata["altIdentifier"] = alt_identifiers
    if accessibility:
        metadata["accessibility"] = accessibility

    publication = {
        "metadata": metadata,
        "links": links,
    }
    if image_links:
        publication["images"] = image_links
    return publication


def _build_url(request: Request, path: str, params: dict[str, str | int]) -> str:
    base = str(request.base_url).rstrip("/")
    query = urlencode(params)
    return f"{base}{path}?{query}" if query else f"{base}{path}"


def _language_label(code: str) -> str:
    native_name = native_name_for_language(code)
    return native_name.upper() if native_name else code.upper()


def _build_feed_response(
    request: Request,
    title: str,
    path: str,
    page: int,
    page_size: int,
    total: int,
    subset,
    repository_id: str,
) -> dict:
    end = (page - 1) * page_size + len(subset)
    base_url = str(request.base_url).rstrip("/")
    publications = [_to_opds_publication(pub, base_url=base_url, repository_id=repository_id) for pub in subset]
    repository = store.get_repository(repository_id)
    repository_name = repository.name if repository else (
        DEFAULT_REPOSITORY_NAME if repository_id == DEFAULT_REPOSITORY_ID else repository_id
    )
    is_default_repository = repository_id == DEFAULT_REPOSITORY_ID
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = "/opds" if is_default_repository else f"/repositories/{repository_id}/opds"
    search_path = "/opds/search" if is_default_repository else f"/repositories/{repository_id}/opds/search"
    search_template_href = f"{base_url}{search_path}" + "{?query,title,author,publisher,series,collection,subject}"

    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "search", "href": search_template_href, "type": "application/opds+json", "templated": True},
    ]
    if path != start_path:
        links.append({"rel": "up", "href": _build_url(request, start_path, {}), "type": "application/opds+json"})
    if end < total:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )

    return {
        "metadata": {
            "@type": "http://schema.org/DataFeed",
            "title": title,
            "numberOfItems": total,
            "itemsPerPage": page_size,
            "currentPage": page,
            "repositoryId": repository_id,
            "repositoryName": repository_name,
            "isDefaultRepository": is_default_repository,
        },
        "links": links,
        "publications": publications,
    }


def _language_path_prefix(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/languages"
    return f"/repositories/{repository_id}/opds/languages"


def _attach_language_facets(request: Request, response: dict, language_counts: list[dict[str, str | int]], repository_id: str) -> dict:
    links = response.setdefault("links", [])
    is_default_repository = repository_id == DEFAULT_REPOSITORY_ID
    start_path = "/opds" if is_default_repository else f"/repositories/{repository_id}/opds"
    if not any(isinstance(link, dict) and link.get("rel") == "start" for link in links):
        links.append(
            {
                "rel": "start",
                "href": _build_url(request, start_path, {}),
                "type": "application/opds+json",
            }
        )
    existing_facets = response.get("facets", [])
    response["facets"] = [
        {
            "metadata": {"title": "Language"},
            "links": [
                {
                    "href": _build_url(request, f"{_language_path_prefix(repository_id)}/{item['code']}", {}),
                    "type": "application/opds+json",
                    "title": _language_label(str(item["code"])),
                    "properties": {"numberOfItems": int(item["count"])},
                }
                for item in language_counts
            ],
        },
        *existing_facets,
    ]
    return response


def _attach_browse_facets(request: Request, response: dict, repository_id: str) -> dict:
    if repository_id == DEFAULT_REPOSITORY_ID:
        collection_prefix = "/opds/collections"
        series_prefix = "/opds/series"
    else:
        collection_prefix = f"/repositories/{repository_id}/opds/collections"
        series_prefix = f"/repositories/{repository_id}/opds/series"

    collection_links = [
        {
            "href": _build_url(request, f"{collection_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "properties": {"numberOfItems": int(item["count"])},
        }
        for item in store.list_collection_counts(repository_id=repository_id)
    ]
    series_links = [
        {
            "href": _build_url(request, f"{series_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": f"Series: {item['name']}",
            "properties": {"numberOfItems": int(item["count"])},
        }
        for item in store.list_series_counts(repository_id=repository_id)
    ]
    browse_links = [*collection_links, *series_links]
    if not browse_links:
        return response
    existing_facets = response.get("facets", [])
    response["facets"] = [
        *existing_facets,
        {
            "metadata": {"title": "Collection"},
            "links": browse_links,
        },
    ]
    return response


def _invalidate_opds_cache(repository_id: str | None = None) -> None:
    if repository_id is None:
        opds_cache.invalidate_feed_keys()
        return
    opds_cache.invalidate_feed_keys(namespace=_cache_namespace(repository_id))


def _cached_opds_response(request: Request, builder, repository_id: str) -> dict:
    cache_key = opds_cache.key_for_request(request, namespace=_cache_namespace(repository_id))
    cached = opds_cache.get_json(cache_key)
    if cached is not None:
        return cached
    payload = builder()
    opds_cache.set_json(cache_key, payload)
    return payload


def _get_repository_or_404(repository_id: str) -> RepositoryConfig:
    repository = store.get_repository(repository_id)
    if repository is None:
        raise HTTPException(status_code=404, detail="Repository not found")
    return repository


@app.get("/health")
def health() -> dict:
    scheduler_enabled = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
    return {
        "status": "ok",
        "publications": store.count(repository_id=DEFAULT_REPOSITORY_ID),
        "database_url": os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db"),
        "scheduler_enabled": scheduler_enabled,
        "scheduler_running": harvest_scheduler.is_running() if scheduler_enabled else False,
        "opds_cache_enabled": opds_cache.is_enabled(),
        "opds_cache_invalidate_every_n_upserts": _cache_invalidate_every_n_upserts(),
        "repositories": len(store.list_repositories()),
    }


@app.on_event("startup")
def startup() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db")
    run_migrations(database_url)
    _ensure_default_repository()
    if os.getenv("SCHEDULER_ENABLED", "true").lower() == "true":
        harvest_scheduler.start()


@app.on_event("shutdown")
def shutdown() -> None:
    harvest_scheduler.shutdown()
    opds_cache.close()


@app.get("/repositories")
def list_repositories(include_inactive: bool = Query(default=True)) -> dict:
    repositories = [item.__dict__ for item in store.list_repositories(include_inactive=include_inactive)]
    return {"count": len(repositories), "repositories": repositories}


@app.put("/repositories/{repository_id}")
def upsert_repository(repository_id: str, request: RepositoryUpsertRequest) -> dict:
    repository = RepositoryConfig(
        repository_id=repository_id,
        source_type=request.source_type,
        name=request.name,
        config=request.config,
        is_active=request.is_active,
        updated_at="",
        created_at="",
    )
    store.upsert_repository(repository)
    return {"repository": store.get_repository(repository_id).__dict__}


@app.get("/repositories/{repository_id}")
def get_repository(repository_id: str) -> dict:
    repository = _get_repository_or_404(repository_id)
    return {"repository": repository.__dict__}


@app.delete("/repositories/{repository_id}")
def delete_repository(repository_id: str) -> dict:
    if repository_id == DEFAULT_REPOSITORY_ID:
        raise HTTPException(status_code=400, detail="Default repository cannot be deleted")
    repository = _get_repository_or_404(repository_id)
    removed_publications = store.count(repository_id=repository_id)
    removed_checkpoints = len(store.list_checkpoints(repository_id=repository_id))
    store.clear(repository_id=repository_id)
    store.clear_checkpoints(repository_id=repository_id)
    store.delete_repository(repository_id=repository_id)
    _invalidate_opds_cache(repository_id)
    return {
        "deleted": True,
        "repository_id": repository_id,
        "repository_name": repository.name,
        "removed_publications": removed_publications,
        "removed_checkpoints": removed_checkpoints,
    }


@app.post("/ingest/json")
def ingest_json(request: JsonIngestRequest) -> dict:
    try:
        result = _ingest_json(request.path, repository_id=DEFAULT_REPOSITORY_ID)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"JSON file not found on server: {request.path}") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON ingest failed: {exc}") from exc
    _invalidate_opds_cache(DEFAULT_REPOSITORY_ID)
    return {
        "repository_id": DEFAULT_REPOSITORY_ID,
        "source": "json",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=DEFAULT_REPOSITORY_ID),
    }


@app.post("/repositories/{repository_id}/ingest/json")
def ingest_json_for_repository(repository_id: str, request: JsonIngestRequest) -> dict:
    _get_repository_or_404(repository_id)
    try:
        result = _ingest_json(request.path, repository_id=repository_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"JSON file not found on server: {request.path}") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON ingest failed: {exc}") from exc
    _invalidate_opds_cache(repository_id)
    return {
        "repository_id": repository_id,
        "source": "json",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=repository_id),
    }


@app.post("/ingest/json-url")
def ingest_json_url(request: JsonUrlIngestRequest) -> dict:
    try:
        result = _ingest_json_url(request.url, repository_id=DEFAULT_REPOSITORY_ID)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON URL ingest failed: {exc}") from exc
    _invalidate_opds_cache(DEFAULT_REPOSITORY_ID)
    return {
        "repository_id": DEFAULT_REPOSITORY_ID,
        "source": "json-url",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=DEFAULT_REPOSITORY_ID),
    }


@app.post("/repositories/{repository_id}/ingest/json-url")
def ingest_json_url_for_repository(repository_id: str, request: JsonUrlIngestRequest) -> dict:
    _get_repository_or_404(repository_id)
    try:
        result = _ingest_json_url(request.url, repository_id=repository_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON URL ingest failed: {exc}") from exc
    _invalidate_opds_cache(repository_id)
    return {
        "repository_id": repository_id,
        "source": "json-url",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=repository_id),
    }


@app.post("/ingest/opds-json")
def ingest_opds_json(request: OpdsJsonIngestRequest) -> dict:
    checkpoint_key = request.checkpoint_key or _opds_json_checkpoint_key(DEFAULT_REPOSITORY_ID, request.url)
    prior_checkpoint = store.get_checkpoint(checkpoint_key, repository_id=DEFAULT_REPOSITORY_ID) if request.incremental else None
    try:
        result, details = _ingest_opds_json(request, repository_id=DEFAULT_REPOSITORY_ID)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"OPDS JSON ingest failed: {exc}") from exc
    _invalidate_opds_cache(DEFAULT_REPOSITORY_ID)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=DEFAULT_REPOSITORY_ID) if request.incremental else None
    return {
        "repository_id": DEFAULT_REPOSITORY_ID,
        "source": "opds-json",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "pages_crawled": details["pages_crawled"],
        "records_processed": details["records_processed"],
        "effective_url": details["effective_url"],
        "total_indexed": store.count(repository_id=DEFAULT_REPOSITORY_ID),
        "incremental": request.incremental,
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
        "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
    }


@app.post("/repositories/{repository_id}/ingest/opds-json")
def ingest_opds_json_for_repository(repository_id: str, request: OpdsJsonIngestRequest) -> dict:
    _get_repository_or_404(repository_id)
    checkpoint_key = request.checkpoint_key or _opds_json_checkpoint_key(repository_id, request.url)
    prior_checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    try:
        result, details = _ingest_opds_json(request, repository_id=repository_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"OPDS JSON ingest failed: {exc}") from exc
    _invalidate_opds_cache(repository_id)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    return {
        "repository_id": repository_id,
        "source": "opds-json",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "pages_crawled": details["pages_crawled"],
        "records_processed": details["records_processed"],
        "effective_url": details["effective_url"],
        "total_indexed": store.count(repository_id=repository_id),
        "incremental": request.incremental,
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
        "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
    }


@app.post("/ingest/json-url/jobs")
def create_json_url_ingest_job(request: IngestJobRequest) -> dict:
    job_id = str(uuid.uuid4())
    with ingest_jobs_lock:
        ingest_jobs[job_id] = {
            "job_id": job_id,
            "type": "json-url",
            "repository_id": DEFAULT_REPOSITORY_ID,
            "status": "queued",
            "url": request.url,
            "created_at": _utcnow_iso(),
            "started_at": None,
            "completed_at": None,
            "accepted": None,
            "rejected": None,
            "total_indexed": None,
            "error": None,
        }

    worker = threading.Thread(
        target=_run_json_url_ingest_job,
        args=(job_id, request.url, DEFAULT_REPOSITORY_ID),
        daemon=True,
    )
    worker.start()
    with ingest_jobs_lock:
        return dict(ingest_jobs[job_id])


@app.get("/ingest/jobs/{job_id}")
def ingest_job(job_id: str) -> dict:
    with ingest_jobs_lock:
        job = ingest_jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Ingest job not found")
        return dict(job)


@app.get("/ingest/jobs")
def list_ingest_jobs(limit: int = Query(default=20, ge=1, le=200)) -> dict:
    with ingest_jobs_lock:
        jobs = list(ingest_jobs.values())
    jobs.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return {"count": len(jobs), "jobs": jobs[:limit]}


@app.post("/ingest/oai-pmh")
def ingest_oai_pmh(request: OaiIngestRequest) -> dict:
    checkpoint_key = request.checkpoint_key or _checkpoint_key(
        DEFAULT_REPOSITORY_ID,
        request.base_url,
        request.metadata_prefix,
        request.set_name,
    )
    prior_checkpoint = store.get_checkpoint(checkpoint_key, repository_id=DEFAULT_REPOSITORY_ID) if request.incremental else None
    result = _ingest_oai(request, repository_id=DEFAULT_REPOSITORY_ID)
    _invalidate_opds_cache(DEFAULT_REPOSITORY_ID)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=DEFAULT_REPOSITORY_ID) if request.incremental else None
    return {
        "repository_id": DEFAULT_REPOSITORY_ID,
        "source": "oai-pmh",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=DEFAULT_REPOSITORY_ID),
        "incremental": request.incremental,
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
        "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
    }


@app.post("/repositories/{repository_id}/ingest/oai-pmh")
def ingest_oai_pmh_for_repository(repository_id: str, request: OaiIngestRequest) -> dict:
    _get_repository_or_404(repository_id)
    checkpoint_key = request.checkpoint_key or _checkpoint_key(
        repository_id,
        request.base_url,
        request.metadata_prefix,
        request.set_name,
    )
    prior_checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    result = _ingest_oai(request, repository_id=repository_id)
    _invalidate_opds_cache(repository_id)
    checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if request.incremental else None
    return {
        "repository_id": repository_id,
        "source": "oai-pmh",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=repository_id),
        "incremental": request.incremental,
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
        "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
    }


def _opds_feed_for_repository(
    repository_id: str,
    request: Request,
    page: int,
    page_size: int,
    path: str,
    title: str,
) -> dict:
    def build_response() -> dict:
        total, subset = store.page(page=page, page_size=page_size, repository_id=repository_id)
        response = _build_feed_response(
            request=request,
            title=title,
            path=path,
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

        languages = store.list_language_counts(repository_id=repository_id)
        year_counts = store.list_publication_year_counts(repository_id=repository_id)

        if repository_id == DEFAULT_REPOSITORY_ID:
            year_path_prefix = "/opds/years"
        else:
            year_path_prefix = f"/repositories/{repository_id}/opds/years"

        response["navigation"] = [
            {
                "href": _build_url(request, f"{year_path_prefix}/{item['year']}", {}),
                "title": f"Publication Year: {item['year']}",
                "type": "application/opds+json",
                "rel": "subsection",
            }
            for item in year_counts
        ]
        response = _attach_language_facets(request=request, response=response, language_counts=languages, repository_id=repository_id)
        return _attach_browse_facets(request=request, response=response, repository_id=repository_id)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds")
def opds_feed(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    return _opds_feed_for_repository(
        repository_id=DEFAULT_REPOSITORY_ID,
        request=request,
        page=page,
        page_size=page_size,
        path="/opds",
        title="OPDS Catalog",
    )


@app.get("/repositories/{repository_id}/opds")
def opds_feed_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    repository = _get_repository_or_404(repository_id)
    return _opds_feed_for_repository(
        repository_id=repository_id,
        request=request,
        page=page,
        page_size=page_size,
        path=f"/repositories/{repository_id}/opds",
        title=f"{repository.name} OPDS Catalog",
    )


@app.get("/opds/default")
def opds_feed_default_alias(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    return opds_feed(request=request, page=page, page_size=page_size)


def _search_feed_for_repository(
    repository_id: str,
    request: Request,
    page: int,
    page_size: int,
    query: str | None,
    title: str | None,
    author: str | None,
    publisher: str | None,
    series: str | None,
    collection: str | None,
    subject: str | None,
    path: str,
) -> dict:
    def build_response() -> dict:
        total, subset = store.search_publications(
            repository_id=repository_id,
            query=query,
            title=title,
            author=author,
            publisher=publisher,
            series=series,
            collection=collection,
            subject=subject,
            page=page,
            page_size=page_size,
        )
        title_parts = ["Search Results"]
        if query:
            title_parts.append(f'query="{query}"')
        if title:
            title_parts.append(f'title="{title}"')
        if author:
            title_parts.append(f'author="{author}"')
        if publisher:
            title_parts.append(f'publisher="{publisher}"')
        if series:
            title_parts.append(f'series="{series}"')
        if collection:
            title_parts.append(f'collection="{collection}"')
        if subject:
            title_parts.append(f'subject="{subject}"')
        feed_title = " | ".join(title_parts)
        return _build_feed_response(
            request=request,
            title=feed_title,
            path=path,
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/search")
def opds_search(
    request: Request,
    query: str | None = Query(default=None),
    title: str | None = Query(default=None),
    author: str | None = Query(default=None),
    publisher: str | None = Query(default=None),
    series: str | None = Query(default=None),
    collection: str | None = Query(default=None),
    subject: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    return _search_feed_for_repository(
        repository_id=DEFAULT_REPOSITORY_ID,
        request=request,
        page=page,
        page_size=page_size,
        query=query,
        title=title,
        author=author,
        publisher=publisher,
        series=series,
        collection=collection,
        subject=subject,
        path="/opds/search",
    )


@app.get("/repositories/{repository_id}/opds/search")
def opds_search_repository(
    repository_id: str,
    request: Request,
    query: str | None = Query(default=None),
    title: str | None = Query(default=None),
    author: str | None = Query(default=None),
    publisher: str | None = Query(default=None),
    series: str | None = Query(default=None),
    collection: str | None = Query(default=None),
    subject: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    return _search_feed_for_repository(
        repository_id=repository_id,
        request=request,
        page=page,
        page_size=page_size,
        query=query,
        title=title,
        author=author,
        publisher=publisher,
        series=series,
        collection=collection,
        subject=subject,
        path=f"/repositories/{repository_id}/opds/search",
    )


@app.get("/opds/index")
def opds_repository_index(request: Request) -> dict:
    repositories = store.list_repositories(include_inactive=False)
    base = str(request.base_url).rstrip("/")
    links = []
    for repository in repositories:
        links.append(
            {
                "href": f"{base}/opds/{repository.repository_id}",
                "title": repository.name,
                "type": "application/opds+json",
                "rel": "subsection",
                "properties": {
                    "repositoryId": repository.repository_id,
                    "sourceType": repository.source_type,
                    "isDefaultRepository": repository.repository_id == DEFAULT_REPOSITORY_ID,
                },
            }
        )
    return {
        "metadata": {
            "@type": "http://schema.org/DataFeed",
            "title": "OPDS Repository Index",
            "numberOfItems": len(links),
        },
        "links": [
            {
                "rel": "self",
                "href": f"{base}/opds/index",
                "type": "application/opds+json",
            }
        ],
        "navigation": links,
    }


@app.get("/opds/collections/{collection_slug}")
def opds_collection_feed(
    collection_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        total, subset = store.page_by_collection_slug(
            collection_slug=collection_slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _build_feed_response(
            request=request,
            title=f"Collection: {collection_slug}",
            path=f"/opds/collections/{collection_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/collections/{collection_slug}")
def opds_collection_feed_repository(
    repository_id: str,
    collection_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        total, subset = store.page_by_collection_slug(
            collection_slug=collection_slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        return _build_feed_response(
            request=request,
            title=f"Collection: {collection_slug}",
            path=f"/repositories/{repository_id}/opds/collections/{collection_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/years/{year}")
def opds_year_feed(
    year: int,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        total, subset = store.page_by_publication_year(year=year, page=page, page_size=page_size, repository_id=DEFAULT_REPOSITORY_ID)
        response = _build_feed_response(
            request=request,
            title=f"Publication Year: {year}",
            path=f"/opds/years/{year}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        language_counts = store.list_language_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID)
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=language_counts,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/years/{year}")
def opds_year_feed_repository(
    repository_id: str,
    year: int,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        total, subset = store.page_by_publication_year(year=year, page=page, page_size=page_size, repository_id=repository_id)
        response = _build_feed_response(
            request=request,
            title=f"Publication Year: {year}",
            path=f"/repositories/{repository_id}/opds/years/{year}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        language_counts = store.list_language_counts_by_publication_year(year=year, repository_id=repository_id)
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=language_counts,
            repository_id=repository_id,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=repository_id)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/languages/{language_code}")
def opds_language_feed(
    language_code: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    normalized_language = normalize_language_value(language_code)
    if normalized_language is None:
        raise HTTPException(status_code=400, detail="language_code must be a 2-letter code, 3-letter code, or language name.")

    def build_response() -> dict:
        total, subset = store.page_by_language(
            language=normalized_language,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Language: {_language_label(normalized_language)}",
            path=f"/opds/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts(repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/languages/{language_code}")
def opds_language_feed_repository(
    repository_id: str,
    language_code: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    normalized_language = normalize_language_value(language_code)
    if normalized_language is None:
        raise HTTPException(status_code=400, detail="language_code must be a 2-letter code, 3-letter code, or language name.")

    def build_response() -> dict:
        total, subset = store.page_by_language(
            language=normalized_language,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Language: {_language_label(normalized_language)}",
            path=f"/repositories/{repository_id}/opds/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts(repository_id=repository_id),
            repository_id=repository_id,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=repository_id)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/series/{series_slug}")
def opds_series_feed(
    series_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        total, subset = store.page_by_series_slug(
            series_slug=series_slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _build_feed_response(
            request=request,
            title=f"Series: {series_slug}",
            path=f"/opds/series/{series_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/series/{series_slug}")
def opds_series_feed_repository(
    repository_id: str,
    series_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        total, subset = store.page_by_series_slug(
            series_slug=series_slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        return _build_feed_response(
            request=request,
            title=f"Series: {series_slug}",
            path=f"/repositories/{repository_id}/opds/series/{series_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/publishers/{publisher_slug}")
def opds_publisher_feed(
    publisher_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        total, subset = store.page_by_publisher_slug(
            publisher_slug=publisher_slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _build_feed_response(
            request=request,
            title=f"Publisher: {publisher_slug}",
            path=f"/opds/publishers/{publisher_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/publishers/{publisher_slug}")
def opds_publisher_feed_repository(
    repository_id: str,
    publisher_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        total, subset = store.page_by_publisher_slug(
            publisher_slug=publisher_slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        return _build_feed_response(
            request=request,
            title=f"Publisher: {publisher_slug}",
            path=f"/repositories/{repository_id}/opds/publishers/{publisher_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/publications/{publication_id}")
def publication(publication_id: str, request: Request) -> dict:
    pub = store.get(publication_id, repository_id=DEFAULT_REPOSITORY_ID)
    if pub is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    return _to_opds_publication(pub, base_url=str(request.base_url).rstrip("/"), repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/publications/{publication_id}")
def publication_for_repository(repository_id: str, publication_id: str, request: Request) -> dict:
    _get_repository_or_404(repository_id)
    pub = store.get(publication_id, repository_id=repository_id)
    if pub is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    return _to_opds_publication(pub, base_url=str(request.base_url).rstrip("/"), repository_id=repository_id)


@app.get("/harvest/checkpoints")
def harvest_checkpoints(repository_id: str = Query(default=DEFAULT_REPOSITORY_ID)) -> dict:
    checkpoints = [item.__dict__ for item in store.list_checkpoints(repository_id=repository_id)]
    return {"count": len(checkpoints), "checkpoints": checkpoints}


@app.post("/harvest/run")
def run_harvest(request: ManualHarvestRequest) -> dict:
    result = run_incremental_for_all_checkpoints(store=store, max_records=request.max_records)
    _invalidate_opds_cache()
    return result


@app.get("/validate/palace")
def validate_palace(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    feed = opds_feed(request=request, page=page, page_size=page_size)
    return validate_palace_opds_feed(feed)


@app.get("/repositories/{repository_id}/validate/palace")
def validate_palace_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    feed = opds_feed_repository(repository_id=repository_id, request=request, page=page, page_size=page_size)
    return validate_palace_opds_feed(feed)


@app.get("/opds/{repository_id}")
def opds_feed_repository_alias(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    return opds_feed_repository(repository_id=repository_id, request=request, page=page, page_size=page_size)
