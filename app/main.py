from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import UTC, datetime
from urllib.parse import urlencode, urljoin, urlparse

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
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
logger = logging.getLogger(__name__)
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


class CleanupByDomainRequest(BaseModel):
    domain: str
    dry_run: bool = False


class CleanupByIdentifierPrefixRequest(BaseModel):
    prefix: str
    dry_run: bool = False


class SubjectBackfillRequest(BaseModel):
    batch_size: int = Field(default=500, ge=1, le=5000)
    start_after: str | None = None


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _iter_url_strings(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_url_strings(item)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_url_strings(item)
        return
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("http://") or text.startswith("https://"):
            yield text


def _url_matches_domain(url: str, domain: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.casefold()
    target = domain.casefold()
    return bool(host) and (host == target or host.endswith(f".{target}"))


def _publication_matches_domain(publication, domain: str) -> bool:
    candidates = []
    if isinstance(publication.identifier, str) and publication.identifier.strip():
        candidates.append(publication.identifier.strip())
    candidates.extend(link.get("href", "") for link in publication.links if isinstance(link, dict))
    if isinstance(publication.raw, dict):
        candidates.extend(_iter_url_strings(publication.raw))
    for candidate in candidates:
        if isinstance(candidate, str) and candidate and _url_matches_domain(candidate, domain):
            return True
    return False


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
            "itemsPerPage": len(publications),
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


def _classification_path_prefix(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/classifications"
    return f"/repositories/{repository_id}/opds/classifications"


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
    else:
        collection_prefix = f"/repositories/{repository_id}/opds/collections"

    collection_links = [
        {
            "href": _build_url(request, f"{collection_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "properties": {"numberOfItems": int(item["count"])},
        }
        for item in store.list_collection_counts(repository_id=repository_id)
    ]
    classification_links = [
        {
            "href": _build_url(request, f"{_classification_path_prefix(repository_id)}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "properties": {"numberOfItems": int(item["count"])},
        }
        for item in store.list_category_counts(repository_id=repository_id)
    ]
    if not collection_links and not classification_links:
        return response
    existing_facets = response.get("facets", [])
    appended_facets = []
    if collection_links:
        appended_facets.append(
            {
                "metadata": {"title": "Collections"},
                "links": collection_links,
            }
        )
    if classification_links:
        appended_facets.append(
            {
                "metadata": {"title": "Classifications"},
                "links": classification_links,
            }
        )
    response["facets"] = [*existing_facets, *appended_facets]
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


def _attach_subclassification_facets(
    *,
    request: Request,
    response: dict,
    repository_id: str,
    category_slug: str,
) -> dict:
    subject_links = [
        {
            "href": _build_url(
                request,
                f"{_classification_path_prefix(repository_id)}/{category_slug}/subjects/{item['slug']}",
                {},
            ),
            "type": "application/opds+json",
            "title": item["name"],
            "properties": {"numberOfItems": int(item["count"])},
        }
        for item in store.list_subject_counts_for_category(category_slug=category_slug, repository_id=repository_id, min_count=1)
    ]
    if not subject_links:
        return response
    response["facets"] = [
        *response.get("facets", []),
        {
            "metadata": {"title": "Sub-Classifications"},
            "links": subject_links,
        },
    ]
    return response


def _get_repository_or_404(repository_id: str) -> RepositoryConfig:
    repository = store.get_repository(repository_id)
    if repository is None:
        raise HTTPException(status_code=404, detail="Repository not found")
    return repository


def _repository_source_domain(repository: RepositoryConfig, checkpoints) -> str | None:
    if repository.repository_id == DEFAULT_REPOSITORY_ID:
        return "oapen.org"
    config = repository.config if isinstance(repository.config, dict) else {}
    candidate_url = None
    for key in ("url", "base_url", "feed_url", "endpoint"):
        value = config.get(key)
        if isinstance(value, str) and value.strip():
            candidate_url = value.strip()
            break
    if candidate_url is None and checkpoints:
        latest = checkpoints[0]
        if isinstance(latest.base_url, str) and latest.base_url.strip():
            candidate_url = latest.base_url.strip()
    if candidate_url:
        parsed = urlparse(candidate_url)
        if parsed.netloc:
            return parsed.netloc
    return None


@app.get("/admin", response_class=HTMLResponse)
def admin_ui() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OPDS Admin</title>
  <style>
    :root {
      --bg: #f3efe4;
      --card: rgba(255, 251, 242, 0.92);
      --ink: #1f2933;
      --muted: #52606d;
      --line: #d9cbb0;
      --accent: #a23e2a;
      --accent-2: #2f6f62;
      --shadow: 0 18px 40px rgba(64, 47, 23, 0.14);
      --radius: 18px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Trebuchet MS", ui-sans-serif, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(162, 62, 42, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(47, 111, 98, 0.16), transparent 28%),
        linear-gradient(180deg, #fbf6ea 0%, var(--bg) 100%);
      min-height: 100vh;
    }
    .wrap {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 64px;
    }
    .hero {
      display: grid;
      gap: 14px;
      margin-bottom: 22px;
    }
    .eyebrow {
      letter-spacing: 0.18em;
      text-transform: uppercase;
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
    }
    h1 {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(2rem, 4vw, 3.3rem);
      line-height: 1.02;
      max-width: 10ch;
    }
    .sub {
      margin: 0;
      color: var(--muted);
      max-width: 70ch;
      line-height: 1.5;
    }
    .grid {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      align-items: start;
    }
    .stack {
      display: grid;
      gap: 18px;
    }
    .manage-grid {
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(320px, 1.2fr) minmax(280px, 0.8fr);
      align-items: start;
    }
    .section {
      display: grid;
      gap: 12px;
      margin-top: 22px;
    }
    .section-head {
      display: grid;
      gap: 4px;
    }
    .section-kicker {
      letter-spacing: 0.16em;
      text-transform: uppercase;
      color: var(--accent-2);
      font-size: 11px;
      font-weight: 700;
    }
    .section-title {
      margin: 0;
      font-size: 1.25rem;
      font-family: Georgia, "Times New Roman", serif;
    }
    .section-copy {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .card {
      background: var(--card);
      border: 1px solid rgba(217, 203, 176, 0.75);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px;
      backdrop-filter: blur(8px);
    }
    .card h2 {
      margin: 0 0 14px;
      font-size: 1rem;
      letter-spacing: 0.02em;
    }
    label {
      display: block;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 6px;
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 13px;
      background: rgba(255, 255, 255, 0.9);
      color: var(--ink);
      font: inherit;
    }
    textarea {
      min-height: 112px;
      resize: vertical;
    }
    .row {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .row-3 {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .check {
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 10px 0 14px;
      color: var(--muted);
      font-size: 14px;
    }
    .check input { width: auto; }
    button {
      appearance: none;
      border: 0;
      border-radius: 999px;
      padding: 11px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      color: white;
      background: linear-gradient(135deg, var(--accent), #cf6a36);
    }
    button.secondary {
      background: linear-gradient(135deg, var(--accent-2), #3f8f80);
    }
    button.ghost {
      color: var(--ink);
      background: rgba(255, 255, 255, 0.65);
      border: 1px solid var(--line);
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .list {
      display: grid;
      gap: 10px;
      max-height: 420px;
      overflow: auto;
      padding-right: 4px;
    }
    .repo {
      border: 1px solid rgba(217, 203, 176, 0.9);
      border-radius: 14px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.6);
      cursor: pointer;
    }
    .repo strong, .repo code {
      display: block;
    }
    .repo small {
      display: block;
      margin-top: 6px;
      color: var(--muted);
    }
    .pill-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      background: rgba(31, 41, 51, 0.08);
      color: var(--ink);
    }
    .pill.default {
      background: rgba(162, 62, 42, 0.14);
      color: var(--accent);
    }
    .pill.active {
      background: rgba(47, 111, 98, 0.14);
      color: var(--accent-2);
    }
    .repo-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      margin-top: 8px;
    }
    .repo-actions a {
      color: var(--accent-2);
      font-weight: 700;
      text-decoration: none;
    }
    .repo-actions button {
      padding: 7px 11px;
      font-size: 12px;
    }
    .summary {
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .detail-grid {
      display: grid;
      gap: 10px;
    }
    .detail-row {
      display: grid;
      gap: 4px;
      padding: 10px 0;
      border-top: 1px solid rgba(217, 203, 176, 0.7);
    }
    .detail-row:first-of-type {
      border-top: 0;
      padding-top: 0;
    }
    .detail-label {
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
    }
    .detail-value {
      font-size: 14px;
      line-height: 1.5;
      color: var(--ink);
      word-break: break-word;
    }
    .detail-actions {
      margin-top: 12px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .detail-actions a {
      color: var(--accent-2);
      font-weight: 700;
      text-decoration: none;
    }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      background: #1f2933;
      color: #d9e2ec;
      padding: 14px;
      border-radius: 14px;
      min-height: 120px;
      max-height: 320px;
      overflow: auto;
      font-size: 12px;
      line-height: 1.5;
    }
    .hint {
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    @media (max-width: 720px) {
      .manage-grid { grid-template-columns: 1fr; }
      .row, .row-3 { grid-template-columns: 1fr; }
      .wrap { padding: 20px 14px 48px; }
      .card { padding: 16px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Operator Console</div>
      <h1>Multi-Repository OPDS Admin</h1>
      <p class="sub">Manage repository definitions, seed new OPDS-like JSON harvests, and inspect checkpoints without posting raw JSON by hand. This wraps the same API routes the service already exposes.</p>
    </section>

    <section class="section">
      <div class="section-head">
        <div class="section-kicker">Configure</div>
        <h2 class="section-title">Add And Seed A Repository</h2>
        <p class="section-copy">Set the repository definition and run the initial harvest from the same dedicated workspace.</p>
      </div>
      <div class="grid">
        <div class="card">
          <h2>Repository Configuration</h2>
          <p class="hint">Create or update a repository. Use <code>source_type</code> <code>opds-json</code> for remote OPDS-like feeds.</p>
          <form id="repo-form">
            <div class="row">
              <div>
                <label for="repo-id">Repository ID</label>
                <input id="repo-id" name="repository_id" placeholder="example-repo" required>
              </div>
              <div>
                <label for="repo-name">Name</label>
                <input id="repo-name" name="name" placeholder="Example Repository" required>
              </div>
            </div>
            <div class="row">
              <div>
                <label for="repo-type">Source Type</label>
                <select id="repo-type" name="source_type">
                  <option value="opds-json">opds-json</option>
                  <option value="json">json</option>
                  <option value="oai-pmh">oai-pmh</option>
                  <option value="mixed">mixed</option>
                </select>
              </div>
              <div>
                <label for="repo-config">Config JSON</label>
                <textarea id="repo-config" name="config" placeholder="{}">{}</textarea>
              </div>
            </div>
            <label class="check"><input id="repo-active" type="checkbox" checked> Active repository</label>
            <div class="actions">
              <button type="submit">Save Repository</button>
              <button type="button" class="ghost" id="refresh-repos">Refresh List</button>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Harvest OPDS-Like JSON</h2>
          <p class="hint">Seed a repository from a remote OPDS feed. This saves a checkpoint so later <code>/harvest/run</code> and the daily scheduler continue from <code>next_url</code>.</p>
          <form id="harvest-form">
            <div>
              <label for="harvest-repo">Repository</label>
              <select id="harvest-repo" name="repository_id"></select>
            </div>
            <div>
              <label for="harvest-url">Remote Feed URL</label>
              <input id="harvest-url" name="url" type="url" placeholder="https://example.org/catalog.json" required>
            </div>
            <div class="row-3">
              <div>
                <label for="harvest-max-pages">Max Pages</label>
                <input id="harvest-max-pages" name="max_pages" type="number" min="1" placeholder="5">
              </div>
              <div>
                <label for="harvest-max-records">Max Records</label>
                <input id="harvest-max-records" name="max_records" type="number" min="1" placeholder="250">
              </div>
              <div>
                <label for="harvest-timeout">Timeout (s)</label>
                <input id="harvest-timeout" name="timeout_seconds" type="number" min="1" max="600" value="120">
              </div>
            </div>
            <label class="check"><input id="harvest-follow-next" type="checkbox" checked> Follow <code>rel: next</code> links</label>
            <label class="check"><input id="harvest-incremental" type="checkbox" checked> Persist checkpoint for scheduled harvests</label>
            <div class="actions">
              <button type="submit" class="secondary">Start Harvest</button>
              <button type="button" class="ghost" id="load-checkpoints">Load Checkpoints</button>
            </div>
          </form>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div class="section-kicker">Manage</div>
        <h2 class="section-title">Manage Repositories</h2>
        <p class="section-copy">Inspect repository state, open feeds, backfill classifications, clear harvested data, or remove non-default repositories.</p>
      </div>
      <div class="manage-grid">
        <div class="card">
          <h2>Known Repositories</h2>
          <p id="repo-summary" class="summary">Loading repositories...</p>
          <p class="hint">Select a repository to inspect it on the right. Use the selected panel for management actions such as subject reindex, clear, delete, or loading config into the edit form.</p>
          <div id="repo-list" class="list"></div>
        </div>
        <div class="card">
          <h2>Selected Repository</h2>
          <p id="repo-detail-empty" class="summary">Select a repository card to inspect its current configuration, load it into the form, and run management actions from one place.</p>
          <div id="repo-detail" class="detail-grid" hidden></div>
          <div id="repo-detail-actions" class="detail-actions" hidden></div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div class="section-kicker">Output</div>
        <h2 class="section-title">Request Output</h2>
        <p class="section-copy">All API responses from configuration, harvest, and management actions are shown here.</p>
      </div>
      <div class="stack">
        <div class="card">
          <h2>Output</h2>
          <pre id="output">Ready.</pre>
        </div>
      </div>
    </section>
  </div>

  <script>
    const repoList = document.getElementById("repo-list");
    const repoSelect = document.getElementById("harvest-repo");
    const repoSummary = document.getElementById("repo-summary");
    const repoDetail = document.getElementById("repo-detail");
    const repoDetailEmpty = document.getElementById("repo-detail-empty");
    const repoDetailActions = document.getElementById("repo-detail-actions");
    const output = document.getElementById("output");
    const subjectBackfillCursors = {};
    let selectedRepository = null;

    function show(data) {
      output.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
    }

    function resolveRepositoryId(explicitRepositoryId, fallbackValue) {
      const value = explicitRepositoryId || fallbackValue || "";
      return value.trim();
    }

    function updateSubjectBackfillCursor(repositoryId, data) {
      if (!repositoryId || !data) {
        return;
      }
      if (typeof data.next_cursor === "string" && data.next_cursor) {
        subjectBackfillCursors[repositoryId] = data.next_cursor;
        return;
      }
      if (data.has_more === false) {
        subjectBackfillCursors[repositoryId] = "";
      }
    }

    async function refreshRepositoriesSilently(preferredRepositoryId) {
      await loadRepositories(preferredRepositoryId, { silent: true });
    }

    async function readJson(response) {
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) {
        return response.json();
      }
      return response.text();
    }

    function normalizeConfigText(text) {
      const trimmed = text.trim();
      if (!trimmed) {
        return {};
      }
      try {
        return JSON.parse(trimmed);
      } catch (error) {
        throw new Error("Config JSON is invalid: " + error.message);
      }
    }

    function createDetailRow(label, value) {
      const row = document.createElement("div");
      row.className = "detail-row";
      const labelNode = document.createElement("div");
      labelNode.className = "detail-label";
      labelNode.textContent = label;
      const valueNode = document.createElement("div");
      valueNode.className = "detail-value";
      valueNode.textContent = value;
      row.appendChild(labelNode);
      row.appendChild(valueNode);
      return row;
    }

    function renderRepositoryDetail(repo) {
      selectedRepository = repo || null;
      repoDetail.innerHTML = "";
      repoDetailActions.innerHTML = "";
      if (!repo) {
        repoDetail.hidden = true;
        repoDetailActions.hidden = true;
        repoDetailEmpty.hidden = false;
        return;
      }

      repoDetail.hidden = false;
      repoDetailActions.hidden = false;
      repoDetailEmpty.hidden = true;

      const detailRows = [
        createDetailRow("Repository", repo.name + " (" + repo.repository_id + ")"),
        createDetailRow("Feed URL", repo.feedHref || "n/a"),
        createDetailRow("Updated", repo.updated_at || "n/a"),
        createDetailRow("Created", repo.created_at || "n/a"),
        createDetailRow("Config JSON", JSON.stringify(repo.config || {}, null, 2)),
      ];
      for (const row of detailRows) {
        repoDetail.appendChild(row);
      }

      const editButton = document.createElement("button");
      editButton.type = "button";
      editButton.textContent = "Edit In Form";
      editButton.addEventListener("click", () => {
        loadRepositoryIntoForm(repo);
      });
      repoDetailActions.appendChild(editButton);

      const openLink = document.createElement("a");
      openLink.href = repo.feedHref;
      openLink.target = "_blank";
      openLink.rel = "noopener noreferrer";
      openLink.textContent = "Open feed";
      repoDetailActions.appendChild(openLink);

      const backfillButton = document.createElement("button");
      backfillButton.type = "button";
      backfillButton.className = "secondary";
      backfillButton.textContent = "Reindex Classifications";
      backfillButton.addEventListener("click", () => {
        backfillSubjects(repo.repository_id).catch((error) => show({ error: String(error) }));
      });
      repoDetailActions.appendChild(backfillButton);

      if (!repo.isDefaultRepository) {
        const clearButton = document.createElement("button");
        clearButton.type = "button";
        clearButton.className = "ghost";
        clearButton.textContent = "Clear Data";
        clearButton.addEventListener("click", () => {
          clearRepositoryData(repo.repository_id).catch((error) => show({ error: String(error) }));
        });
        repoDetailActions.appendChild(clearButton);

        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "ghost";
        deleteButton.textContent = "Delete";
        deleteButton.addEventListener("click", () => {
          deleteRepository(repo.repository_id).catch((error) => show({ error: String(error) }));
        });
        repoDetailActions.appendChild(deleteButton);
      }
    }

    function renderRepositories(payload, preferredRepositoryId) {
      const repositories = payload.repositories || [];
      const selectedRepositoryId = preferredRepositoryId || repoSelect.value;
      repoList.innerHTML = "";
      repoSelect.innerHTML = "";
      let detailRepository = null;
      const defaultRepo = repositories.find((repo) => repo.isDefaultRepository);
      if (defaultRepo) {
        repoSummary.textContent = "Default feed: " + defaultRepo.name + " (" + defaultRepo.repository_id + ").";
      } else {
        repoSummary.textContent = "No default repository is currently configured.";
      }
      for (const repo of repositories) {
        if (!(repo.repository_id in subjectBackfillCursors)) {
          subjectBackfillCursors[repo.repository_id] = "";
        }
        if (selectedRepository && selectedRepository.repository_id === repo.repository_id) {
          detailRepository = repo;
        }
        const option = document.createElement("option");
        option.value = repo.repository_id;
        option.textContent = repo.name + " (" + repo.repository_id + ")";
        repoSelect.appendChild(option);

        const card = document.createElement("div");
        card.className = "repo";
        const metaBits = [
          "source_type: " + repo.source_type,
          "source: " + (repo.sourceDomain || "n/a"),
          "items: " + (repo.publicationCount != null ? repo.publicationCount : 0),
          "checkpoints: " + (repo.checkpointCount != null ? repo.checkpointCount : 0)
        ];
        const pills = [];
        if (repo.isDefaultRepository) {
          pills.push('<span class="pill default">Default Feed</span>');
        }
        if (repo.is_active) {
          pills.push('<span class="pill active">Active</span>');
        }
        card.innerHTML = [
          "<strong>" + repo.name + "</strong>",
          "<code>" + repo.repository_id + "</code>",
          "<small>" + metaBits.join(" | ") + "</small>",
          '<div class="pill-row">' + pills.join("") + "</div>"
        ].join("");

        const actions = document.createElement("div");
        actions.className = "repo-actions";

        const openLink = document.createElement("a");
        openLink.href = repo.feedHref;
        openLink.target = "_blank";
        openLink.rel = "noopener noreferrer";
        openLink.textContent = "Open feed";
        openLink.addEventListener("click", (event) => event.stopPropagation());
        actions.appendChild(openLink);

        card.appendChild(actions);
        card.addEventListener("click", () => selectRepository(repo));
        repoList.appendChild(card);
      }
      if (selectedRepositoryId && repositories.some((repo) => repo.repository_id === selectedRepositoryId)) {
        repoSelect.value = selectedRepositoryId;
      }
      if (!detailRepository && selectedRepositoryId) {
        detailRepository = repositories.find((repo) => repo.repository_id === selectedRepositoryId) || null;
      }
      renderRepositoryDetail(detailRepository);
    }

    async function backfillSubjects(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoSelect.value);
      if (!repositoryId) {
        show({ error: "Select a repository first." });
        return;
      }
      const batchInput = window.prompt("Reindex batch size (1-5000):", "500");
      if (batchInput === null) {
        return;
      }
      const batchSize = Number(batchInput.trim() || "500");
      if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 5000) {
        show({ error: "Batch size must be between 1 and 5000." });
        return;
      }
      const body = { batch_size: Math.floor(batchSize) };
      const cursor = subjectBackfillCursors[repositoryId];
      if (cursor) {
        body.start_after = cursor;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/reindex/subjects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      show(data);
      if (response.ok) {
        updateSubjectBackfillCursor(repositoryId, data);
        await refreshRepositoriesSilently(repositoryId);
      }
    }

    async function clearRepositoryData(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, document.getElementById("repo-id").value);
      if (!repositoryId) {
        show({ error: "Select or enter a repository first." });
        return;
      }
      if (repositoryId === "default") {
        show({ error: "The default repository cannot be cleared." });
        return;
      }
      const confirmed = window.confirm("Clear harvested data and checkpoints for '" + repositoryId + "' while keeping its configuration?");
      if (!confirmed) {
        return;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/clear-data", {
        method: "POST",
      });
      const data = await readJson(response);
      show(data);
      if (response.ok) {
        subjectBackfillCursors[repositoryId] = "";
        await refreshRepositoriesSilently(repositoryId);
      }
    }

    function loadRepositoryIntoForm(repo) {
      document.getElementById("repo-id").value = repo.repository_id;
      document.getElementById("repo-name").value = repo.name;
      document.getElementById("repo-type").value = repo.source_type;
      document.getElementById("repo-config").value = JSON.stringify(repo.config || {}, null, 2);
      document.getElementById("repo-active").checked = Boolean(repo.is_active);
    }

    function selectRepository(repo) {
      selectedRepository = repo;
      repoSelect.value = repo.repository_id;
      renderRepositoryDetail(repo);
    }

    async function loadRepositories(preferredRepositoryId, options) {
      const settings = options || {};
      const response = await fetch("/repositories");
      const data = await readJson(response);
      if (!response.ok) {
        show(data);
        return;
      }
      renderRepositories(data, preferredRepositoryId);
      if (!settings.silent) {
        show(data);
      }
    }

    async function saveRepository(event) {
      event.preventDefault();
      const repositoryId = document.getElementById("repo-id").value.trim();
      const body = {
        source_type: document.getElementById("repo-type").value,
        name: document.getElementById("repo-name").value.trim(),
        config: normalizeConfigText(document.getElementById("repo-config").value),
        is_active: document.getElementById("repo-active").checked,
      };
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      show(data);
      if (response.ok) {
        await refreshRepositoriesSilently(repositoryId);
        repoSelect.value = repositoryId;
      }
    }

    async function deleteRepository(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, document.getElementById("repo-id").value);
      if (!repositoryId) {
        show({ error: "Select or enter a repository first." });
        return;
      }
      if (repositoryId === "default") {
        show({ error: "The default repository cannot be deleted." });
        return;
      }
      const confirmed = window.confirm("Delete repository '" + repositoryId + "' and all of its harvested data?");
      if (!confirmed) {
        return;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId), {
        method: "DELETE",
      });
      const data = await readJson(response);
      show(data);
      if (response.ok) {
        delete subjectBackfillCursors[repositoryId];
        if (selectedRepository && selectedRepository.repository_id === repositoryId) {
          renderRepositoryDetail(null);
        }
        document.getElementById("repo-form").reset();
        document.getElementById("repo-config").value = "{}";
        document.getElementById("repo-active").checked = true;
        await refreshRepositoriesSilently();
      }
    }

    async function runHarvest(event) {
      event.preventDefault();
      const repositoryId = repoSelect.value;
      const body = {
        url: document.getElementById("harvest-url").value.trim(),
        follow_next: document.getElementById("harvest-follow-next").checked,
        incremental: document.getElementById("harvest-incremental").checked,
        timeout_seconds: Number(document.getElementById("harvest-timeout").value) || 120,
      };
      const maxPages = document.getElementById("harvest-max-pages").value.trim();
      const maxRecords = document.getElementById("harvest-max-records").value.trim();
      if (maxPages) body.max_pages = Number(maxPages);
      if (maxRecords) body.max_records = Number(maxRecords);

      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/ingest/opds-json", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      show(data);
    }

    async function loadCheckpoints() {
      const repositoryId = repoSelect.value;
      const response = await fetch("/harvest/checkpoints?repository_id=" + encodeURIComponent(repositoryId));
      const data = await readJson(response);
      show(data);
    }

    document.getElementById("repo-form").addEventListener("submit", (event) => {
      saveRepository(event).catch((error) => show({ error: String(error) }));
    });
    document.getElementById("harvest-form").addEventListener("submit", (event) => {
      runHarvest(event).catch((error) => show({ error: String(error) }));
    });
    document.getElementById("refresh-repos").addEventListener("click", () => {
      loadRepositories().catch((error) => show({ error: String(error) }));
    });
    document.getElementById("load-checkpoints").addEventListener("click", () => {
      loadCheckpoints().catch((error) => show({ error: String(error) }));
    });
    loadRepositories().catch((error) => show({ error: String(error) }));
  </script>
</body>
</html>"""


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
    try:
        database_url = os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db")
        run_migrations(database_url)
        _ensure_default_repository()
        if os.getenv("SCHEDULER_ENABLED", "true").lower() == "true":
            harvest_scheduler.start()
    except Exception:
        logger.exception("Application startup failed")
        raise


@app.on_event("shutdown")
def shutdown() -> None:
    harvest_scheduler.shutdown()
    opds_cache.close()


@app.get("/repositories")
def list_repositories(request: Request, include_inactive: bool = Query(default=True)) -> dict:
    base = str(request.base_url).rstrip("/")
    repositories = []
    for item in store.list_repositories(include_inactive=include_inactive):
        checkpoints = store.list_checkpoints(repository_id=item.repository_id)
        repositories.append(
            {
                **item.__dict__,
                "isDefaultRepository": item.repository_id == DEFAULT_REPOSITORY_ID,
                "publicationCount": store.count(repository_id=item.repository_id),
                "checkpointCount": len(checkpoints),
                "sourceDomain": _repository_source_domain(item, checkpoints),
                "feedHref": (
                    f"{base}/opds"
                    if item.repository_id == DEFAULT_REPOSITORY_ID
                    else f"{base}/repositories/{item.repository_id}/opds"
                ),
            }
        )
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


@app.post("/repositories/{repository_id}/clear-data")
def clear_repository_data(repository_id: str) -> dict:
    if repository_id == DEFAULT_REPOSITORY_ID:
        raise HTTPException(status_code=400, detail="Default repository data cannot be cleared")
    repository = _get_repository_or_404(repository_id)
    removed_publications = store.count(repository_id=repository_id)
    removed_checkpoints = len(store.list_checkpoints(repository_id=repository_id))
    store.clear(repository_id=repository_id)
    store.clear_checkpoints(repository_id=repository_id)
    _invalidate_opds_cache(repository_id)
    return {
        "cleared": True,
        "repository_id": repository_id,
        "repository_name": repository.name,
        "removed_publications": removed_publications,
        "removed_checkpoints": removed_checkpoints,
        "repository_preserved": True,
    }


@app.post("/repositories/{repository_id}/cleanup/domain")
def cleanup_repository_by_domain(repository_id: str, request: CleanupByDomainRequest) -> dict:
    repository = _get_repository_or_404(repository_id)
    domain = request.domain.strip().casefold()
    if not domain:
        raise HTTPException(status_code=400, detail="domain is required")

    matched_publications = [
        publication.publication_id
        for publication in store.all(repository_id=repository_id)
        if _publication_matches_domain(publication, domain)
    ]
    removed = 0
    if not request.dry_run and matched_publications:
        removed = store.delete_publications(matched_publications, repository_id=repository_id)
        _invalidate_opds_cache(repository_id)

    return {
        "repository_id": repository_id,
        "repository_name": repository.name,
        "domain": domain,
        "dry_run": request.dry_run,
        "matched_publications": len(matched_publications),
        "removed_publications": removed,
        "remaining_publications": store.count(repository_id=repository_id),
        "matched_ids": matched_publications[:100],
    }


@app.post("/repositories/{repository_id}/cleanup/identifier-prefix")
def cleanup_repository_by_identifier_prefix(repository_id: str, request: CleanupByIdentifierPrefixRequest) -> dict:
    repository = _get_repository_or_404(repository_id)
    prefix = request.prefix.strip()
    if not prefix:
        raise HTTPException(status_code=400, detail="prefix is required")

    matched_count = store.count_publications_by_identifier_prefix(prefix, repository_id=repository_id)
    matched_publications = store.list_publication_ids_by_identifier_prefix(prefix, repository_id=repository_id, limit=100)
    removed = 0
    if not request.dry_run and matched_count:
        removed = store.delete_publications_by_identifier_prefix(prefix, repository_id=repository_id)
        _invalidate_opds_cache(repository_id)

    return {
        "repository_id": repository_id,
        "repository_name": repository.name,
        "prefix": prefix,
        "dry_run": request.dry_run,
        "matched_publications": matched_count,
        "removed_publications": removed,
        "remaining_publications": store.count(repository_id=repository_id),
        "matched_ids": matched_publications[:100],
    }


@app.post("/repositories/{repository_id}/backfill/subjects")
def backfill_repository_subjects(repository_id: str, request: SubjectBackfillRequest) -> dict:
    repository = _get_repository_or_404(repository_id)
    result = store.backfill_publication_subjects(
        repository_id=repository_id,
        batch_size=request.batch_size,
        start_after=request.start_after,
    )
    if result.processed_publications:
        _invalidate_opds_cache(repository_id)
    return {
        "repository_id": repository_id,
        "repository_name": repository.name,
        "processed_publications": result.processed_publications,
        "indexed_subject_rows": result.indexed_subject_rows,
        "next_cursor": result.next_cursor,
        "has_more": result.has_more,
        "batch_size": request.batch_size,
        "total_publications": store.count(repository_id=repository_id),
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


@app.get("/opds/classifications/{classification_slug}")
def opds_classification_feed(
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    category_entry = store.get_category(classification_slug, repository_id=DEFAULT_REPOSITORY_ID)
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_category_slug(
            category_slug=classification_slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Classification: {category_name}",
            path=f"/opds/classifications/{classification_slug}",
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
        response = _attach_subclassification_facets(
            request=request,
            response=response,
            repository_id=DEFAULT_REPOSITORY_ID,
            category_slug=classification_slug,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/classifications/{classification_slug}")
def opds_classification_feed_repository(
    repository_id: str,
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    category_entry = store.get_category(classification_slug, repository_id=repository_id)
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_category_slug(
            category_slug=classification_slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Classification: {category_name}",
            path=f"/repositories/{repository_id}/opds/classifications/{classification_slug}",
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
        response = _attach_subclassification_facets(
            request=request,
            response=response,
            repository_id=repository_id,
            category_slug=classification_slug,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=repository_id)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/classifications/{classification_slug}/subjects/{subject_slug}")
def opds_subclassification_feed(
    classification_slug: str,
    subject_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    category_entry = store.get_category(classification_slug, repository_id=DEFAULT_REPOSITORY_ID)
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    subject_entry = next(
        (
            item
            for item in store.list_subject_counts_for_category(
                category_slug=classification_slug,
                repository_id=DEFAULT_REPOSITORY_ID,
                min_count=1,
            )
            if item["slug"] == subject_slug
        ),
        None,
    )
    if subject_entry is None:
        raise HTTPException(status_code=404, detail="Sub-classification not found")
    subject_name = str(subject_entry["name"])
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_subject_slug(
            subject_slug=subject_slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Sub-Classification: {subject_name}",
            path=f"/opds/classifications/{classification_slug}/subjects/{subject_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response["metadata"]["belongsTo"] = {"classification": category_name}
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts(repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/classifications/{classification_slug}/subjects/{subject_slug}")
def opds_subclassification_feed_repository(
    repository_id: str,
    classification_slug: str,
    subject_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    category_entry = store.get_category(classification_slug, repository_id=repository_id)
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    subject_entry = next(
        (
            item
            for item in store.list_subject_counts_for_category(
                category_slug=classification_slug,
                repository_id=repository_id,
                min_count=1,
            )
            if item["slug"] == subject_slug
        ),
        None,
    )
    if subject_entry is None:
        raise HTTPException(status_code=404, detail="Sub-classification not found")
    subject_name = str(subject_entry["name"])
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_subject_slug(
            subject_slug=subject_slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Sub-Classification: {subject_name}",
            path=f"/repositories/{repository_id}/opds/classifications/{classification_slug}/subjects/{subject_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response["metadata"]["belongsTo"] = {"classification": category_name}
        response = _attach_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts(repository_id=repository_id),
            repository_id=repository_id,
        )
        return _attach_browse_facets(request=request, response=response, repository_id=repository_id)

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


@app.get("/repositories/{repository_id}/classifications/stats")
def classification_stats(
    repository_id: str,
    min_count: int = Query(default=3, ge=1),
    top_limit: int = Query(default=25, ge=1, le=200),
) -> dict:
    _get_repository_or_404(repository_id)
    stats = store.subject_statistics(repository_id=repository_id, min_count=min_count, top_limit=top_limit)
    return {
        "repository_id": repository_id,
        "minimum_facet_count": stats["minimum_facet_count"],
        "total_assignments": stats["total_assignments"],
        "distinct_subject_slugs": stats["distinct_subject_slugs"],
        "distinct_subject_labels": stats["distinct_subject_labels"],
        "displayable_facet_count": stats["displayable_facet_count"],
        "top_subjects": stats["top_subjects"],
    }


@app.get("/repositories/{repository_id}/classification-categories/stats")
def classification_category_stats(
    repository_id: str,
    min_count: int = Query(default=3, ge=1),
) -> dict:
    _get_repository_or_404(repository_id)
    categories = store.list_category_counts(repository_id=repository_id, min_count=min_count)
    return {
        "repository_id": repository_id,
        "minimum_facet_count": min_count,
        "displayable_category_count": len(categories),
        "categories": categories,
    }


@app.get("/repositories/{repository_id}/classifications/raw-stats")
def classification_raw_stats(
    repository_id: str,
    min_count: int = Query(default=1, ge=1),
    top_limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    stats = store.raw_subject_statistics(repository_id=repository_id, min_count=min_count, top_limit=top_limit)
    return {
        "repository_id": repository_id,
        "minimum_count": stats["minimum_count"],
        "total_assignments": stats["total_assignments"],
        "distinct_raw_subjects": stats["distinct_raw_subjects"],
        "distinct_canonical_subjects": stats["distinct_canonical_subjects"],
        "top_raw_subjects": stats["top_raw_subjects"],
        "top_canonical_subjects": stats["top_canonical_subjects"],
    }


@app.post("/repositories/{repository_id}/reindex/subjects")
def reindex_repository_subjects(repository_id: str, request: SubjectBackfillRequest) -> dict:
    return backfill_repository_subjects(repository_id=repository_id, request=request)


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
