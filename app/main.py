from __future__ import annotations

import os
from datetime import UTC, datetime
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.db_migrations import run_migrations
from app.harvest import run_incremental_for_all_checkpoints
from app.scheduler import IncrementalHarvestScheduler
from app.sources import iter_json_records, iter_json_records_from_url, load_oai_dc_records
from app.store import IngestResult, PublicationStore
from app.transform import normalize_json_record, normalize_oai_record
from app.validation import validate_palace_opds_feed

app = FastAPI(title="OAPEN OPDS Feed Generator", version="0.1.0")
store = PublicationStore(os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db"))
harvest_scheduler = IncrementalHarvestScheduler(
    store=store,
    hour_utc=int(os.getenv("SCHEDULER_DAILY_UTC_HOUR", "2")),
    minute_utc=int(os.getenv("SCHEDULER_DAILY_UTC_MINUTE", "0")),
)


class JsonIngestRequest(BaseModel):
    path: str = Field(description="Absolute or workspace-relative path to JSON metadata file.")


class JsonUrlIngestRequest(BaseModel):
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


def _checkpoint_key(base_url: str, metadata_prefix: str, set_name: str | None) -> str:
    return f"{base_url}|{metadata_prefix}|{set_name or 'default'}"


def _today_ymd() -> str:
    return datetime.now(UTC).date().isoformat()


def _max_date_string(values: list[str | None]) -> str | None:
    candidates = sorted([value for value in values if value])
    return candidates[-1] if candidates else None


def _ingest_json(path: str) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    for raw in iter_json_records(path):
        normalized = normalize_json_record(raw)
        if normalized is None:
            result.rejected += 1
            continue
        store.upsert(normalized)
        result.accepted += 1
    return result


def _ingest_json_url(url: str) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    for raw in iter_json_records_from_url(url):
        normalized = normalize_json_record(raw)
        if normalized is None:
            result.rejected += 1
            continue
        store.upsert(normalized)
        result.accepted += 1
    return result


def _ingest_oai(request: OaiIngestRequest) -> IngestResult:
    result = IngestResult(accepted=0, rejected=0, errors=[])
    checkpoint_key = request.checkpoint_key or _checkpoint_key(request.base_url, request.metadata_prefix, request.set_name)
    checkpoint = store.get_checkpoint(checkpoint_key) if request.incremental else None
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
        store.upsert(normalized)
        harvested_dates.append(normalized.published)
        result.accepted += 1

    if request.incremental:
        latest_harvested = _max_date_string(harvested_dates) or effective_until
        store.upsert_checkpoint(
            checkpoint_key=checkpoint_key,
            base_url=request.base_url,
            metadata_prefix=request.metadata_prefix,
            set_name=request.set_name,
            last_from_date=effective_from,
            last_until_date=latest_harvested,
        )
    return result


def _to_opds_publication(pub) -> dict:
    def to_rfc3339(value: str | None) -> str | None:
        if not value:
            return None
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        # Handle common upstream format: "YYYY-MM-DD HH:MM:SS"
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
    links = pub.links or [
        {
            "rel": "self",
            "href": f"/publications/{pub.publication_id}",
            "type": "application/opds+json",
        }
    ]
    author = [{"name": name} for name in pub.authors] if pub.authors else []
    subject = [{"name": value} for value in pub.subjects] if pub.subjects else []

    return {
        "metadata": {
            "@type": "http://schema.org/Book",
            "title": pub.title,
            "identifier": pub.identifier or pub.publication_id,
            "language": pub.language,
            "modified": modified,
            "published": modified,
            "author": author,
            "subject": subject,
            "publisher": {"name": pub.publisher} if pub.publisher else None,
        },
        "links": links,
    }


def _build_url(request: Request, path: str, params: dict[str, str | int]) -> str:
    base = str(request.base_url).rstrip("/")
    query = urlencode(params)
    return f"{base}{path}?{query}" if query else f"{base}{path}"


@app.get("/health")
def health() -> dict:
    scheduler_enabled = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
    return {
        "status": "ok",
        "publications": store.count(),
        "database_url": os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db"),
        "scheduler_enabled": scheduler_enabled,
        "scheduler_running": harvest_scheduler.is_running() if scheduler_enabled else False,
    }


@app.on_event("startup")
def startup() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db")
    run_migrations(database_url)
    if os.getenv("SCHEDULER_ENABLED", "true").lower() == "true":
        harvest_scheduler.start()


@app.on_event("shutdown")
def shutdown() -> None:
    harvest_scheduler.shutdown()


@app.post("/ingest/json")
def ingest_json(request: JsonIngestRequest) -> dict:
    try:
        result = _ingest_json(request.path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"JSON file not found on server: {request.path}") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON ingest failed: {exc}") from exc
    return {
        "source": "json",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(),
    }


@app.post("/ingest/json-url")
def ingest_json_url(request: JsonUrlIngestRequest) -> dict:
    try:
        result = _ingest_json_url(request.url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"JSON URL ingest failed: {exc}") from exc
    return {
        "source": "json-url",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(),
    }


@app.post("/ingest/oai-pmh")
def ingest_oai_pmh(request: OaiIngestRequest) -> dict:
    checkpoint_key = request.checkpoint_key or _checkpoint_key(request.base_url, request.metadata_prefix, request.set_name)
    prior_checkpoint = store.get_checkpoint(checkpoint_key) if request.incremental else None
    result = _ingest_oai(request)
    checkpoint = store.get_checkpoint(checkpoint_key) if request.incremental else None
    return {
        "source": "oai-pmh",
        "accepted": result.accepted,
        "rejected": result.rejected,
        "total_indexed": store.count(),
        "incremental": request.incremental,
        "checkpoint": checkpoint.__dict__ if checkpoint else None,
        "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
    }


@app.get("/opds")
def opds_feed(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    total, subset = store.page(page=page, page_size=page_size)
    end = (page - 1) * page_size + len(subset)
    publications = [_to_opds_publication(pub) for pub in subset]

    links = [
        {"rel": "self", "href": _build_url(request, "/opds", {"page": page, "page_size": page_size}), "type": "application/opds+json"}
    ]
    if end < total:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, "/opds", {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, "/opds", {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )

    return {
        "metadata": {
            "@type": "http://schema.org/DataFeed",
            "title": "OAPEN OPDS Catalog",
            "numberOfItems": total,
            "itemsPerPage": page_size,
            "currentPage": page,
        },
        "links": links,
        "publications": publications,
    }


@app.get("/publications/{publication_id}")
def publication(publication_id: str) -> dict:
    pub = store.get(publication_id)
    if pub is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    return _to_opds_publication(pub)


@app.get("/harvest/checkpoints")
def harvest_checkpoints() -> dict:
    checkpoints = [item.__dict__ for item in store.list_checkpoints()]
    return {"count": len(checkpoints), "checkpoints": checkpoints}


@app.post("/harvest/run")
def run_harvest(request: ManualHarvestRequest) -> dict:
    return run_incremental_for_all_checkpoints(store=store, max_records=request.max_records)


@app.get("/validate/palace")
def validate_palace(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    feed = opds_feed(request=request, page=page, page_size=page_size)
    return validate_palace_opds_feed(feed)
