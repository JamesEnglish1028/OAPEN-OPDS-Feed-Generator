from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.cache import OpdsCache
from app.db_migrations import run_migrations
from app.harvest import run_incremental_for_all_checkpoints
from app.scheduler import IncrementalHarvestScheduler
from app.sources import iter_json_records, iter_json_records_from_url, load_oai_dc_records
from app.springer import SpringerSource
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
springer_base_url = os.getenv("SPRINGER_OPENACCESS_BASE_URL", "").strip() or "https://api.springernature.com/openaccess/json"
springer_source = SpringerSource(base_url=springer_base_url)
harvest_scheduler = IncrementalHarvestScheduler(
    store=store,
    hour_utc=int(os.getenv("SCHEDULER_DAILY_UTC_HOUR", "2")),
    minute_utc=int(os.getenv("SCHEDULER_DAILY_UTC_MINUTE", "0")),
)


class JsonIngestRequest(BaseModel):
    path: str = Field(description="Absolute or workspace-relative path to JSON metadata file.")


class JsonUrlIngestRequest(BaseModel):
    url: str = Field(description="HTTP(S) URL to a JSON metadata file.")


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


class SpringerIngestRequest(BaseModel):
    max_records: int | None = None
    reset_checkpoint: bool = False
    clear_existing: bool = False
    start_offset: int | None = None


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
            "@type": "http://schema.org/Book",
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
        belongs_to_obj["collection"] = collection_value
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

    links = [{"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"}]
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
        },
        "links": links,
        "publications": publications,
    }


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


@app.post("/repositories/{repository_id}/ingest/springer")
def ingest_springer_repository(repository_id: str, request: SpringerIngestRequest) -> dict:
    repository = _get_repository_or_404(repository_id)
    if repository.source_type != "springer-openaccess":
        raise HTTPException(status_code=400, detail="Repository source_type must be springer-openaccess")
    if request.clear_existing:
        store.clear(repository_id=repository_id)
    if request.reset_checkpoint:
        store.clear_checkpoints(repository_id=repository_id)
    try:
        result = springer_source.ingest_repository(
            store=store,
            repository=repository,
            max_records=request.max_records,
            start_offset=request.start_offset if request.start_offset is not None else (1 if request.reset_checkpoint else None),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Springer ingest failed: {exc}") from exc
    _invalidate_opds_cache(repository_id)
    checkpoint = store.get_checkpoint(f"springer::{repository_id}", repository_id=repository_id)
    return {
        "repository_id": repository_id,
        "source": "springer-openaccess",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(repository_id=repository_id),
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
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
            language_path_prefix = "/opds/languages"
        else:
            year_path_prefix = f"/repositories/{repository_id}/opds/years"
            language_path_prefix = f"/repositories/{repository_id}/opds/languages"

        response["navigation"] = [
            {
                "href": _build_url(request, f"{year_path_prefix}/{item['year']}", {}),
                "title": f"Publication Year: {item['year']}",
                "type": "application/opds+json",
                "rel": "subsection",
            }
            for item in year_counts
        ]
        response["facets"] = [
            {
                "metadata": {"title": "Language"},
                "links": [
                    {
                        "href": _build_url(request, f"{language_path_prefix}/{item['code']}", {}),
                        "type": "application/opds+json",
                        "title": _language_label(str(item["code"])),
                        "properties": {"numberOfItems": int(item["count"])},
                    }
                    for item in languages
                ],
            }
        ]
        return response

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
        return _build_feed_response(
            request=request,
            title=f"Publication Year: {year}",
            path=f"/opds/years/{year}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )

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
        return _build_feed_response(
            request=request,
            title=f"Publication Year: {year}",
            path=f"/repositories/{repository_id}/opds/years/{year}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

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
        return _build_feed_response(
            request=request,
            title=f"Language: {_language_label(normalized_language)}",
            path=f"/opds/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )

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
        return _build_feed_response(
            request=request,
            title=f"Language: {_language_label(normalized_language)}",
            path=f"/repositories/{repository_id}/opds/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )

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
