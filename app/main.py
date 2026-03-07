from __future__ import annotations

import logging
import os
import re
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.cache import OpdsCache
from app.db_migrations import run_migrations
from app.harvest import run_incremental_for_all_checkpoints
from app.orcid_enrichment import enrich_publication_authors
from app.publication_groups import list_publication_groups, publication_group_by_slug
from app.scheduler import IncrementalHarvestScheduler
from app.sources import extract_json_records, iter_json_records, iter_json_records_from_url, load_json_payload_from_url, load_oai_dc_records
from app.store import IngestResult, PublicationStore, RepositoryConfig
from app.subject_authorities import resolve_lcc, resolve_lcsh, resolve_thema
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
COLLECTION_FACET_LINK_LIMIT = max(1, int(os.getenv("OPDS_COLLECTION_FACET_LINK_LIMIT", "100")))
CLASSIFICATION_FACET_LINK_LIMIT = max(1, int(os.getenv("OPDS_CLASSIFICATION_FACET_LINK_LIMIT", "100")))
SUBCLASSIFICATION_FACET_LINK_LIMIT = max(1, int(os.getenv("OPDS_SUBCLASSIFICATION_FACET_LINK_LIMIT", "100")))
ROOT_NAV_GROUP_LINK_LIMIT = max(1, int(os.getenv("OPDS_ROOT_NAV_GROUP_LINK_LIMIT", "3")))
AUTHORITY_SCHEME_CANONICAL: dict[str, str] = {
    "http://id.loc.gov": "http://id.loc.gov",
    "http://id.loc.gov/authorities/subjects": "http://id.loc.gov/authorities/subjects",
    "https://ns.editeur.org/thema/": "https://ns.editeur.org/thema/",
}

app = FastAPI(title="OAPEN OPDS Feed Generator", version="0.2.0")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
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
    collection_name: str | None = None


class OpdsDirectoryEntryRequest(BaseModel):
    title: str
    href: str


class OpdsDirectoryPreviewRequest(BaseModel):
    url: str = Field(description="HTTP(S) URL to an OPDS-like JSON root.")
    timeout_seconds: int = Field(default=120, ge=1, le=600)


class OpdsDirectoryImportRequest(BaseModel):
    root_url: str = Field(description="Root OPDS URL used for directory discovery.")
    directories: list[OpdsDirectoryEntryRequest] = Field(default_factory=list)
    mode: str = Field(default="split-repositories")
    target_repository_id: str | None = None
    follow_next: bool = True
    incremental: bool = True
    timeout_seconds: int = Field(default=120, ge=1, le=600)
    max_pages: int | None = Field(default=None, ge=1)
    max_records: int | None = Field(default=None, ge=1)


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
    offset: int | None = Field(default=None, ge=0)


class CacheInvalidateRequest(BaseModel):
    repository_id: str | None = None


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
    return enrich_publication_authors(publication)


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


def _extract_opds_navigation_urls(payload: object, current_url: str) -> list[str]:
    if not isinstance(payload, dict):
        return []
    candidates: list[str] = []

    def _append_from_links(value: object) -> None:
        if not isinstance(value, list):
            return
        for link in value:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            if isinstance(href, str) and href.strip():
                candidates.append(urljoin(current_url, href.strip()))

    _append_from_links(payload.get("navigation"))
    groups = payload.get("groups")
    if isinstance(groups, list):
        for group in groups:
            if not isinstance(group, dict):
                continue
            _append_from_links(group.get("navigation"))

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _extract_opds_navigation_entries(payload: object, current_url: str) -> list[dict[str, str]]:
    if not isinstance(payload, dict):
        return []
    entries: list[dict[str, str]] = []

    def _append_from_links(value: object, group_title: str | None = None) -> None:
        if not isinstance(value, list):
            return
        for link in value:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            if not isinstance(href, str) or not href.strip():
                continue
            absolute_href = urljoin(current_url, href.strip())
            title = link.get("title")
            if not isinstance(title, str) or not title.strip():
                parsed = urlparse(absolute_href)
                title = parsed.path.strip("/") or parsed.netloc or absolute_href
            entry = {"title": title.strip(), "href": absolute_href}
            if isinstance(group_title, str) and group_title.strip():
                entry["group"] = group_title.strip()
            entries.append(entry)

    _append_from_links(payload.get("navigation"))
    groups = payload.get("groups")
    if isinstance(groups, list):
        for group in groups:
            if not isinstance(group, dict):
                continue
            group_title = None
            metadata = group.get("metadata")
            if isinstance(metadata, dict):
                candidate = metadata.get("title")
                if isinstance(candidate, str) and candidate.strip():
                    group_title = candidate
            _append_from_links(group.get("navigation"), group_title=group_title)

    deduped: list[dict[str, str]] = []
    seen_hrefs: set[str] = set()
    for entry in entries:
        href = entry["href"]
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        deduped.append(entry)
    return deduped


def _slugify_text(value: str | None, max_length: int = 64) -> str:
    text = (value or "").strip().lower()
    if not text:
        return ""
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    if not slug:
        return ""
    return slug[:max_length]


def _next_available_repository_id(base_slug: str) -> str:
    candidate = base_slug
    suffix = 2
    while store.get_repository(candidate) is not None:
        candidate = f"{base_slug}-{suffix}"
        suffix += 1
    return candidate


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
    state = checkpoint.state if checkpoint and isinstance(checkpoint.state, dict) else {}
    pending_urls = [item for item in state.get("pending_urls", []) if isinstance(item, str) and item.strip()]
    visited_urls = set(item for item in state.get("visited_urls", []) if isinstance(item, str) and item.strip())

    while current_url:
        visited_urls.add(current_url)
        payload = load_json_payload_from_url(current_url, timeout_seconds=request.timeout_seconds)
        last_url = current_url
        pages_crawled += 1
        page_records = 0
        for raw in extract_json_records(payload):
            normalized = normalize_json_record(raw)
            records_processed += 1
            page_records += 1
            if normalized is None:
                result.rejected += 1
            else:
                if isinstance(request.collection_name, str) and request.collection_name.strip():
                    collection_name = request.collection_name.strip()
                    normalized.collection = collection_name
                    normalized.collection_slug = _slugify_text(collection_name, max_length=512) or None
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
        if request.follow_next and not next_url and page_records == 0:
            for candidate in _extract_opds_navigation_urls(payload, current_url):
                if candidate in visited_urls or candidate in pending_urls:
                    continue
                pending_urls.append(candidate)

        if request.max_records and records_processed >= request.max_records:
            if next_url:
                next_url_to_store = next_url
            elif pending_urls:
                next_url_to_store = pending_urls[0]
            else:
                next_url_to_store = request.url
            break
        if request.max_pages and pages_crawled >= request.max_pages:
            if next_url:
                next_url_to_store = next_url
            elif pending_urls:
                next_url_to_store = pending_urls[0]
            else:
                next_url_to_store = request.url
            break
        if not request.follow_next:
            next_url_to_store = request.url
            break
        if next_url:
            next_url_to_store = next_url
            current_url = next_url
            continue
        while pending_urls and pending_urls[0] in visited_urls:
            pending_urls.pop(0)
        if pending_urls:
            next_url_to_store = pending_urls[0]
            current_url = pending_urls.pop(0)
            continue
        next_url_to_store = request.url
        break

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
                "pending_urls": pending_urls[:500],
                "visited_urls": list(visited_urls)[-2000:],
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

    author: list[dict[str, str]] = []
    if isinstance(pub.authors_enriched, list) and pub.authors_enriched:
        for row in pub.authors_enriched:
            if not isinstance(row, dict):
                continue
            name = row.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            entry = {"name": name.strip()}
            uri = row.get("uri")
            if isinstance(uri, str) and uri.strip():
                entry["uri"] = uri.strip()
            author.append(entry)
    if not author and pub.authors:
        author = [{"name": name} for name in pub.authors]
    normalized_subject_names = [value.strip() for value in pub.subjects if isinstance(value, str) and value.strip()]

    def _canonical_authority_scheme(raw_scheme: str) -> str:
        normalized = raw_scheme.strip().casefold()
        if normalized in AUTHORITY_SCHEME_CANONICAL:
            return AUTHORITY_SCHEME_CANONICAL[normalized]
        if normalized == "lcc":
            return AUTHORITY_SCHEME_CANONICAL["http://id.loc.gov"]
        if normalized == "lcsh":
            return AUTHORITY_SCHEME_CANONICAL["http://id.loc.gov/authorities/subjects"]
        if normalized == "thema":
            return AUTHORITY_SCHEME_CANONICAL["https://ns.editeur.org/thema/"]
        return raw_scheme.strip()

    def _subject_sort_as(value: str) -> str:
        candidate = value.strip()
        if ":" in candidate:
            trailing = candidate.split(":")[-1].strip()
            if trailing:
                return trailing
        return candidate

    def _normalize_authority_record(row: dict) -> dict | None:
        scheme = row.get("scheme")
        term = row.get("term")
        code = row.get("code")
        if not isinstance(scheme, str) or not scheme.strip():
            return None
        if not isinstance(term, str) or not term.strip():
            return None
        scheme_uri = _canonical_authority_scheme(scheme)
        authority = {
            "name": term.strip(),
            "sortAs": _subject_sort_as(term),
            "scheme": scheme_uri,
        }
        if isinstance(code, str) and code.strip():
            authority["code"] = code.strip()
        subject_name = row.get("subject_name")
        if isinstance(subject_name, str) and subject_name.strip():
            authority["sourceSubjectName"] = subject_name.strip()
        return authority

    def _computed_subject_entries_for_subject(subject_name: str) -> list[dict]:
        computed_rows: list[dict] = []
        lcc_match = resolve_lcc(subject_name)
        if isinstance(lcc_match, dict):
            computed_rows.append(lcc_match)
        lcsh_matches = resolve_lcsh(subject_name)
        if isinstance(lcsh_matches, list):
            computed_rows.extend([item for item in lcsh_matches if isinstance(item, dict)])
        thema_matches = resolve_thema(subject_name)
        if isinstance(thema_matches, list):
            computed_rows.extend([item for item in thema_matches if isinstance(item, dict)])

        normalized_authorities: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for row in computed_rows:
            normalized = _normalize_authority_record(row)
            if normalized is None:
                continue
            dedupe_key = (
                normalized.get("scheme", ""),
                normalized.get("name", ""),
                normalized.get("code", ""),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized_authorities.append(normalized)
        return normalized_authorities

    enriched_subjects = []
    raw_subject_authorities = pub.subject_authorities if isinstance(pub.subject_authorities, list) else []
    authorities_by_subject_name: dict[str, list[dict]] = {}
    for row in raw_subject_authorities:
        if not isinstance(row, dict):
            continue
        subject_name = row.get("subject_name")
        scheme = row.get("scheme")
        term = row.get("term")
        code = row.get("code")
        if not isinstance(subject_name, str) or not subject_name:
            continue
        if not isinstance(scheme, str) or not scheme:
            continue
        if not isinstance(term, str) or not term:
            continue
        authority = _normalize_authority_record({"scheme": scheme, "term": term, "code": code})
        if authority is None:
            continue
        key = subject_name.strip().casefold()
        authorities_by_subject_name.setdefault(key, []).append(authority)

    seen_subject_entries: set[tuple[str, str, str]] = set()
    for subject_name in normalized_subject_names:
        subject_key = subject_name.casefold()
        entries = authorities_by_subject_name.get(subject_key, [])
        if not entries:
            entries = _computed_subject_entries_for_subject(subject_name)
        if not entries:
            entries = [{"name": subject_name, "sortAs": _subject_sort_as(subject_name)}]
        for entry in entries:
            dedupe_key = (
                str(entry.get("name", "")),
                str(entry.get("scheme", "")),
                str(entry.get("code", "")),
            )
            if dedupe_key in seen_subject_entries:
                continue
            seen_subject_entries.add(dedupe_key)
            entry.pop("sourceSubjectName", None)
            enriched_subjects.append(entry)

    if not enriched_subjects and raw_subject_authorities:
        for row in raw_subject_authorities:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_authority_record(row)
            if normalized is None:
                continue
            normalized.pop("sourceSubjectName", None)
            dedupe_key = (
                str(normalized.get("name", "")),
                str(normalized.get("scheme", "")),
                str(normalized.get("code", "")),
            )
            if dedupe_key in seen_subject_entries:
                continue
            seen_subject_entries.add(dedupe_key)
            enriched_subjects.append(normalized)

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
            "subjects": enriched_subjects,
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
            "@type": "https://opds.io/opds-catalog",
            "title": title,
            "numberOfItems": total,
            "itemsPerPage": len(publications),
            "currentPage": page,
        },
        "links": links,
        "publications": publications,
    }


def _build_collections_index_response(
    *,
    request: Request,
    repository_id: str,
    path: str,
    page: int,
    page_size: int,
) -> dict:
    offset = (page - 1) * page_size
    total = store.count_collection_facets(repository_id=repository_id)
    items = store.list_collection_counts_limited(
        repository_id=repository_id,
        limit=page_size,
        offset=offset,
        order_by_count_desc=True,
    )
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = "/opds" if repository_id == DEFAULT_REPOSITORY_ID else f"/repositories/{repository_id}/opds"
    collections_prefix = _collections_index_path(repository_id)
    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
    ]
    if path != start_path:
        links.append({"rel": "up", "href": _build_url(request, start_path, {}), "type": "application/opds+json"})
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page < last_page:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    navigation = [
        {
            "href": _build_url(request, f"{collections_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "rel": "subsection",
            "numberOfItems": int(item["count"]),
        }
        for item in items
    ]
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
            "title": "Collections",
            "numberOfItems": total,
            "itemsPerPage": len(navigation),
            "currentPage": page,
        },
        "links": links,
        "navigation": navigation,
    }


def _build_classifications_index_response(
    *,
    request: Request,
    repository_id: str,
    path: str,
    page: int,
    page_size: int,
) -> dict:
    offset = (page - 1) * page_size
    total = store.count_category_facets(repository_id=repository_id, min_count=3)
    items = store.list_category_counts(
        repository_id=repository_id,
        min_count=3,
        limit=page_size,
        offset=offset,
        order_by_count_desc=True,
    )
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = "/opds" if repository_id == DEFAULT_REPOSITORY_ID else f"/repositories/{repository_id}/opds"
    class_prefix = _classifications_index_path(repository_id)
    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
    ]
    if path != start_path:
        links.append({"rel": "up", "href": _build_url(request, start_path, {}), "type": "application/opds+json"})
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page < last_page:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    navigation = [
        {
            "href": _build_url(request, f"{class_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "rel": "subsection",
            "numberOfItems": int(item["count"]),
        }
        for item in items
    ]
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
            "title": "Classifications",
            "numberOfItems": total,
            "itemsPerPage": len(navigation),
            "currentPage": page,
        },
        "links": links,
        "navigation": navigation,
    }


def _build_languages_index_response(
    *,
    request: Request,
    repository_id: str,
    path: str,
    page: int,
    page_size: int,
) -> dict:
    offset = (page - 1) * page_size
    all_languages = store.list_language_counts(repository_id=repository_id)
    total = len(all_languages)
    items = all_languages[offset : offset + page_size]
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = "/opds" if repository_id == DEFAULT_REPOSITORY_ID else f"/repositories/{repository_id}/opds"
    language_prefix = _language_path_prefix(repository_id)
    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
    ]
    if path != start_path:
        links.append({"rel": "up", "href": _build_url(request, start_path, {}), "type": "application/opds+json"})
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page < last_page:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    navigation = [
        {
            "href": _build_url(request, f"{language_prefix}/{item['code']}", {}),
            "type": "application/opds+json",
            "title": _language_label(str(item["code"])),
            "rel": "subsection",
            "numberOfItems": int(item["count"]),
        }
        for item in items
    ]
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
            "title": "Languages",
            "numberOfItems": total,
            "itemsPerPage": len(navigation),
            "currentPage": page,
        },
        "links": links,
        "navigation": navigation,
    }


def _build_lcc_index_response(
    *,
    request: Request,
    repository_id: str,
    path: str,
    page: int,
    page_size: int,
) -> dict:
    offset = (page - 1) * page_size
    total = store.count_lcc_heading_facets(repository_id=repository_id, min_count=3)
    items = store.list_lcc_heading_counts(
        repository_id=repository_id,
        min_count=3,
        limit=page_size,
        offset=offset,
    )
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = "/opds" if repository_id == DEFAULT_REPOSITORY_ID else f"/repositories/{repository_id}/opds"
    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
    ]
    if path != start_path:
        links.append({"rel": "up", "href": _build_url(request, start_path, {}), "type": "application/opds+json"})
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page < last_page:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    navigation = [
        {
            "href": _build_url(request, f"{_lcc_path_prefix(repository_id)}/{item['code']}", {}),
            "type": "application/opds+json",
            "title": f"{item['term']} ({item['code']})",
            "rel": "subsection",
            "numberOfItems": int(item["count"]),
        }
        for item in items
    ]
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
            "title": "LCC Top-Level Subjects",
            "numberOfItems": total,
            "itemsPerPage": len(navigation),
            "currentPage": page,
        },
        "links": links,
        "navigation": navigation,
    }


def _build_subclassifications_index_response(
    *,
    request: Request,
    repository_id: str,
    category_slug: str,
    path: str,
    page: int,
    page_size: int,
) -> dict:
    category = store.get_category(category_slug, repository_id=repository_id)
    if category is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    offset = (page - 1) * page_size
    total = store.count_subject_facets_for_category(category_slug, repository_id=repository_id, min_count=1)
    items = store.list_subject_counts_for_category(
        category_slug=category_slug,
        repository_id=repository_id,
        min_count=1,
        limit=page_size,
        offset=offset,
        order_by_count_desc=True,
    )
    last_page = max(1, (total + page_size - 1) // page_size) if page_size > 0 else 1
    start_path = _classifications_index_path(repository_id)
    sub_path_prefix = f"{start_path}/{category_slug}"
    links = [
        {"rel": "self", "href": _build_url(request, path, {"page": page, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "start", "href": _build_url(request, start_path, {}), "type": "application/opds+json"},
        {"rel": "up", "href": _build_url(request, f"{start_path}/{category_slug}", {}), "type": "application/opds+json"},
        {"rel": "first", "href": _build_url(request, path, {"page": 1, "page_size": page_size}), "type": "application/opds+json"},
        {"rel": "last", "href": _build_url(request, path, {"page": last_page, "page_size": page_size}), "type": "application/opds+json"},
    ]
    if page > 1:
        links.append(
            {
                "rel": "previous",
                "href": _build_url(request, path, {"page": page - 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    if page < last_page:
        links.append(
            {
                "rel": "next",
                "href": _build_url(request, path, {"page": page + 1, "page_size": page_size}),
                "type": "application/opds+json",
            }
        )
    navigation = [
        {
            "href": _build_url(request, f"{sub_path_prefix}/subjects/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "rel": "subsection",
            "numberOfItems": int(item["count"]),
        }
        for item in items
    ]
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
            "title": f"Sub-Classifications: {category['name']}",
            "numberOfItems": total,
            "itemsPerPage": len(navigation),
            "currentPage": page,
            "classificationSlug": category_slug,
            "classificationName": category["name"],
        },
        "links": links,
        "navigation": navigation,
    }


def _language_path_prefix(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/languages"
    return f"/repositories/{repository_id}/opds/languages"


def _language_index_path(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/navigation/languages"
    return f"/repositories/{repository_id}/opds/navigation/languages"


def _lcc_index_path(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/navigation/lcc"
    return f"/repositories/{repository_id}/opds/navigation/lcc"


def _lcc_path_prefix(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/lcc"
    return f"/repositories/{repository_id}/opds/lcc"


def _classification_path_prefix(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/classifications"
    return f"/repositories/{repository_id}/opds/classifications"


def _collections_index_path(repository_id: str) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return "/opds/collections"
    return f"/repositories/{repository_id}/opds/collections"


def _classifications_index_path(repository_id: str) -> str:
    return _classification_path_prefix(repository_id)


def _year_path_prefix(repository_id: str, year: int) -> str:
    if repository_id == DEFAULT_REPOSITORY_ID:
        return f"/opds/years/{year}"
    return f"/repositories/{repository_id}/opds/years/{year}"


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
                    "numberOfItems": int(item["count"]),
                }
                for item in language_counts
            ],
        },
        *existing_facets,
    ]
    return response


def _attach_year_language_facets(
    request: Request,
    response: dict,
    language_counts: list[dict[str, str | int]],
    repository_id: str,
    year: int,
) -> dict:
    links = response.setdefault("links", [])
    start_path = _year_path_prefix(repository_id, year)
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
                    "href": _build_url(request, f"{start_path}/languages/{item['code']}", {}),
                    "type": "application/opds+json",
                    "title": _language_label(str(item["code"])),
                    "numberOfItems": int(item["count"]),
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

    total_collections = store.count_collection_facets(repository_id=repository_id)
    collection_links = [
        {
            "href": _build_url(request, f"{collection_prefix}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "numberOfItems": int(item["count"]),
        }
        for item in store.list_collection_counts_limited(
            repository_id=repository_id,
            limit=COLLECTION_FACET_LINK_LIMIT,
            offset=0,
            order_by_count_desc=True,
        )
    ]
    total_classifications = store.count_category_facets(repository_id=repository_id, min_count=3)
    classification_links = [
        {
            "href": _build_url(request, f"{_classification_path_prefix(repository_id)}/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "numberOfItems": int(item["count"]),
        }
        for item in store.list_category_counts(
            repository_id=repository_id,
            min_count=3,
            limit=CLASSIFICATION_FACET_LINK_LIMIT,
            offset=0,
            order_by_count_desc=True,
        )
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
    if total_collections > len(collection_links):
        response["links"] = [
            *response.get("links", []),
            {
                "rel": "collection",
                "href": _build_url(request, _collections_index_path(repository_id), {}),
                "type": "application/opds+json",
                "title": "Browse All Collections",
            },
        ]
    if total_classifications > len(classification_links):
        response["links"] = [
            *response.get("links", []),
            {
                "rel": "collection",
                "href": _build_url(request, _classifications_index_path(repository_id), {}),
                "type": "application/opds+json",
                "title": "Browse All Classifications",
            },
        ]
    return response


def _attach_year_browse_facets(request: Request, response: dict, repository_id: str, year: int) -> dict:
    year_prefix = _year_path_prefix(repository_id, year)
    collection_links = [
        {
            "href": _build_url(request, f"{year_prefix}/collections/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "numberOfItems": int(item["count"]),
        }
        for item in store.list_collection_counts_by_publication_year(
            year=year,
            repository_id=repository_id,
            limit=COLLECTION_FACET_LINK_LIMIT,
            offset=0,
            order_by_count_desc=True,
        )
    ]
    classification_links = [
        {
            "href": _build_url(request, f"{year_prefix}/classifications/{item['slug']}", {}),
            "type": "application/opds+json",
            "title": item["name"],
            "numberOfItems": int(item["count"]),
        }
        for item in store.list_category_counts_by_publication_year(
            year=year,
            repository_id=repository_id,
            min_count=3,
            limit=CLASSIFICATION_FACET_LINK_LIMIT,
            offset=0,
            order_by_count_desc=True,
        )
    ]
    if not collection_links and not classification_links:
        return response
    existing_facets = response.get("facets", [])
    appended_facets = []
    if collection_links:
        appended_facets.append({"metadata": {"title": "Collections"}, "links": collection_links})
    if classification_links:
        appended_facets.append({"metadata": {"title": "Classifications"}, "links": classification_links})
    response["facets"] = [*existing_facets, *appended_facets]
    return response


def _invalidate_opds_cache(repository_id: str | None = None) -> int:
    if repository_id is None:
        return int(opds_cache.invalidate_feed_keys())
    namespace = _cache_namespace(repository_id)
    try:
        return int(opds_cache.invalidate_feed_keys(namespace=namespace))
    except TypeError:
        # Backward-compatibility for older cache fakes/tests that only expose
        # invalidate_feed_keys() without a namespace parameter.
        return int(opds_cache.invalidate_feed_keys())


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
    year: int | None = None,
) -> dict:
    try:
        total_subjects = (
            store.count_subject_facets_for_category_by_publication_year(
                category_slug=category_slug,
                year=year,
                repository_id=repository_id,
                min_count=1,
            )
            if year is not None
            else store.count_subject_facets_for_category(category_slug, repository_id=repository_id, min_count=1)
        )
        subject_items = (
            store.list_subject_counts_for_category_by_publication_year(
                category_slug=category_slug,
                year=year,
                repository_id=repository_id,
                min_count=1,
                limit=SUBCLASSIFICATION_FACET_LINK_LIMIT,
                offset=0,
                order_by_count_desc=True,
            )
            if year is not None
            else store.list_subject_counts_for_category(
                category_slug=category_slug,
                repository_id=repository_id,
                min_count=1,
                limit=SUBCLASSIFICATION_FACET_LINK_LIMIT,
                offset=0,
                order_by_count_desc=True,
            )
        )
    except Exception:
        logger.exception(
            "Failed to attach sub-classifications",
            extra={
                "repository_id": repository_id,
                "category_slug": category_slug,
                "year": year,
            },
        )
        return response
    path_prefix = (
        f"{_year_path_prefix(repository_id, year)}/classifications/{category_slug}"
        if year is not None
        else f"{_classification_path_prefix(repository_id)}/{category_slug}"
    )
    subject_links = [
        {
            "href": _build_url(
                request,
                f"{path_prefix}/subjects/{item['slug']}",
                {},
            ),
            "type": "application/opds+json",
            "title": item["name"],
            "numberOfItems": int(item["count"]),
        }
        for item in subject_items
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
    if year is None and total_subjects > len(subject_links):
        response["links"] = [
            *response.get("links", []),
            {
                "rel": "collection",
                "href": _build_url(request, f"{_classification_path_prefix(repository_id)}/{category_slug}/subjects", {}),
                "type": "application/opds+json",
                "title": "Browse All Sub-Classifications",
            },
        ]
    return response


def _get_repository_or_404(repository_id: str) -> RepositoryConfig:
    if repository_id == DEFAULT_REPOSITORY_ID:
        _ensure_default_repository()
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


@app.get("/admin")
def admin_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "admin.html")


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
        "root_nav_group_link_limit": ROOT_NAV_GROUP_LINK_LIMIT,
        "repositories": len(store.list_repositories()),
    }


@app.on_event("startup")
def startup() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///./oapen_opds.db")
    try:
        run_migrations(database_url)
    except BaseException:
        # Temporary operational fallback: keep the service booting even if Alembic
        # gets stuck on the category migration. The modeled tables are created below.
        logger.exception("Database migration failed during startup; continuing with metadata initialization")
    try:
        store.initialize()
        _ensure_default_repository()
        if os.getenv("SCHEDULER_ENABLED", "true").lower() == "true":
            harvest_scheduler.start()
    except BaseException:
        logger.exception("Application startup failed")
        raise


@app.on_event("shutdown")
def shutdown() -> None:
    harvest_scheduler.shutdown()
    opds_cache.close()


@app.get("/repositories")
def list_repositories(request: Request, include_inactive: bool = Query(default=True)) -> dict:
    _ensure_default_repository()
    base = str(request.base_url).rstrip("/")
    repositories = []
    for item in store.list_repositories(include_inactive=include_inactive):
        checkpoints = store.list_checkpoints(repository_id=item.repository_id)
        repositories.append(
            {
                **item.__dict__,
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
        offset=request.offset,
    )
    if result.processed_publications:
        _invalidate_opds_cache(repository_id)
    return {
        "repository_id": repository_id,
        "repository_name": repository.name,
        "processed_publications": result.processed_publications,
        "indexed_subject_rows": result.indexed_subject_rows,
        "skipped_publications": result.skipped_publications,
        "error_examples": result.error_examples or [],
        "next_cursor": result.next_cursor,
        "has_more": result.has_more,
        "batch_size": request.batch_size,
        "total_publications": store.count(repository_id=repository_id),
    }


@app.post("/repositories/{repository_id}/backfill/subject-authorities")
def backfill_repository_subject_authorities(repository_id: str, request: SubjectBackfillRequest) -> dict:
    repository = _get_repository_or_404(repository_id)
    result = store.backfill_publication_subject_authorities(
        repository_id=repository_id,
        batch_size=request.batch_size,
        start_after=request.start_after,
        offset=request.offset,
    )
    if result.processed_publications:
        _invalidate_opds_cache(repository_id)
    return {
        "repository_id": repository_id,
        "repository_name": repository.name,
        "processed_publications": result.processed_publications,
        "indexed_authority_rows": result.indexed_authority_rows,
        "skipped_publications": result.skipped_publications,
        "error_examples": result.error_examples or [],
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


@app.post("/ingest/opds-json/directories")
def preview_opds_json_directories(request: OpdsDirectoryPreviewRequest) -> dict:
    try:
        payload = load_json_payload_from_url(request.url, timeout_seconds=request.timeout_seconds)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"OPDS directory preview failed: {exc}") from exc
    entries = _extract_opds_navigation_entries(payload, request.url)
    return {
        "root_url": request.url,
        "directory_count": len(entries),
        "directories": entries,
    }


@app.post("/ingest/opds-json/directories/import")
def import_opds_json_directories(request: OpdsDirectoryImportRequest) -> dict:
    mode = request.mode.strip().casefold()
    valid_modes = {"split-repositories", "single-repository-collections"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"mode must be one of: {', '.join(sorted(valid_modes))}")
    if not request.directories:
        raise HTTPException(status_code=400, detail="At least one directory must be selected")

    imports: list[dict] = []
    created_repositories: list[dict] = []

    target_repository_id = request.target_repository_id.strip() if isinstance(request.target_repository_id, str) else None
    if mode == "single-repository-collections":
        if not target_repository_id:
            raise HTTPException(status_code=400, detail="target_repository_id is required for single-repository-collections mode")
        _get_repository_or_404(target_repository_id)

    for directory in request.directories:
        directory_title = directory.title.strip()
        directory_href = directory.href.strip()
        if not directory_href:
            continue

        repository_id = target_repository_id
        if mode == "split-repositories":
            base_slug = _slugify_text(directory_title)
            if not base_slug:
                parsed = urlparse(directory_href)
                base_slug = _slugify_text(parsed.path.strip("/") or parsed.netloc or "opds-directory")
            if not base_slug:
                base_slug = f"opds-directory-{len(imports) + 1}"
            repository_id = _next_available_repository_id(base_slug)
            repository_name = directory_title or repository_id
            repository = RepositoryConfig(
                repository_id=repository_id,
                source_type="opds-json",
                name=repository_name,
                config={"url": directory_href, "directory_title": directory_title, "source_root": request.root_url},
                is_active=True,
                updated_at="",
                created_at="",
            )
            store.upsert_repository(repository)
            created_repositories.append(
                {
                    "repository_id": repository_id,
                    "name": repository_name,
                    "source_url": directory_href,
                }
            )

        ingest_request = OpdsJsonIngestRequest(
            url=directory_href,
            max_records=request.max_records,
            max_pages=request.max_pages,
            follow_next=request.follow_next,
            timeout_seconds=request.timeout_seconds,
            incremental=request.incremental,
            collection_name=directory_title if mode == "single-repository-collections" else None,
        )
        checkpoint_key = ingest_request.checkpoint_key or _opds_json_checkpoint_key(repository_id, ingest_request.url)
        prior_checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if ingest_request.incremental else None
        try:
            result, details = _ingest_opds_json(ingest_request, repository_id=repository_id)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Directory import failed for '{directory_title or directory_href}': {exc}",
            ) from exc
        _invalidate_opds_cache(repository_id)
        checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository_id) if ingest_request.incremental else None
        imports.append(
            {
                "directory_title": directory_title,
                "directory_url": directory_href,
                "repository_id": repository_id,
                "accepted": result.accepted,
                "rejected": result.rejected,
                "pages_crawled": details["pages_crawled"],
                "records_processed": details["records_processed"],
                "effective_url": details["effective_url"],
                "total_indexed": store.count(repository_id=repository_id),
                "checkpoint": checkpoint.__dict__ if checkpoint else None,
                "previous_checkpoint": prior_checkpoint.__dict__ if prior_checkpoint else None,
            }
        )

    return {
        "mode": mode,
        "root_url": request.root_url,
        "selected_directories": len(request.directories),
        "imported_directories": len(imports),
        "created_repositories": created_repositories,
        "imports": imports,
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
        lcc_headings = store.list_lcc_heading_counts(
            repository_id=repository_id,
            min_count=3,
            limit=ROOT_NAV_GROUP_LINK_LIMIT,
            offset=0,
        )
        collection_items = store.list_collection_counts_limited(
            repository_id=repository_id,
            limit=ROOT_NAV_GROUP_LINK_LIMIT,
            offset=0,
            order_by_count_desc=True,
        )

        language_group_links = [
            {
                "href": _build_url(request, f"{_language_path_prefix(repository_id)}/{item['code']}", {}),
                "title": _language_label(str(item["code"])),
                "type": "application/opds+json",
                "rel": "subsection",
                "numberOfItems": int(item["count"]),
            }
            for item in languages[:ROOT_NAV_GROUP_LINK_LIMIT]
        ]
        language_group_links.append(
            {
                "href": _build_url(request, _language_index_path(repository_id), {}),
                "title": "Browse All Languages",
                "type": "application/opds+json",
                "rel": "collection",
            }
        )

        lcc_group_links = [
            {
                "href": _build_url(request, f"{_lcc_path_prefix(repository_id)}/{item['code']}", {}),
                "title": f"{item['term']} ({item['code']})",
                "type": "application/opds+json",
                "rel": "subsection",
                "numberOfItems": int(item["count"]),
            }
            for item in lcc_headings
        ]
        lcc_group_links.append(
            {
                "href": _build_url(request, _lcc_index_path(repository_id), {}),
                "title": "Browse All LCC Headings",
                "type": "application/opds+json",
                "rel": "collection",
            }
        )

        collection_group_links = [
            {
                "href": _build_url(request, f"{_collections_index_path(repository_id)}/{item['slug']}", {}),
                "title": item["name"],
                "type": "application/opds+json",
                "rel": "subsection",
                "numberOfItems": int(item["count"]),
            }
            for item in collection_items
        ]
        collection_group_links.append(
            {
                "href": _build_url(request, _collections_index_path(repository_id), {}),
                "title": "Browse All Collections",
                "type": "application/opds+json",
                "rel": "collection",
            }
        )

        response.pop("navigation", None)
        groups = [
            {
                "metadata": {"title": "Language"},
                "navigation": language_group_links,
            },
            {
                "metadata": {"title": "LCC Top-Level Subjects"},
                "navigation": lcc_group_links,
            },
            {
                "metadata": {"title": "Collections"},
                "navigation": collection_group_links,
            },
        ]

        publication_group_counts = store.list_publication_group_counts(repository_id=repository_id)
        for definition in list_publication_groups():
            total_items = next(
                (
                    int(item["count"])
                    for item in publication_group_counts
                    if isinstance(item, dict) and item.get("slug") == definition.slug
                ),
                0,
            )
            _, preview_subset = store.page_by_publication_group_slug(
                group_slug=definition.slug,
                page=1,
                page_size=10,
                repository_id=repository_id,
            )
            if repository_id == DEFAULT_REPOSITORY_ID:
                group_path = f"/opds/groups/{definition.slug}"
            else:
                group_path = f"/repositories/{repository_id}/opds/groups/{definition.slug}"
            groups.append(
                {
                    "metadata": {
                        "title": definition.title,
                        "numberOfItems": total_items,
                    },
                    "links": [
                        {
                            "rel": "self",
                            "href": _build_url(request, group_path, {}),
                            "type": "application/opds+json",
                        }
                    ],
                    "publications": [
                        _to_opds_publication(pub, base_url=str(request.base_url).rstrip("/"), repository_id=repository_id)
                        for pub in preview_subset
                    ],
                }
            )
        response["groups"] = groups
        response.pop("facets", None)
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


@app.get("/opds/default")
def opds_feed_default_alias(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    return opds_feed(request=request, page=page, page_size=page_size)


@app.get("/opds/navigation/languages")
def opds_languages_index(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        return _build_languages_index_response(
            request=request,
            repository_id=DEFAULT_REPOSITORY_ID,
            path="/opds/navigation/languages",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/navigation/languages")
def opds_languages_index_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        return _build_languages_index_response(
            request=request,
            repository_id=repository_id,
            path=f"/repositories/{repository_id}/opds/navigation/languages",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/navigation/lcc")
def opds_lcc_index(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        return _build_lcc_index_response(
            request=request,
            repository_id=DEFAULT_REPOSITORY_ID,
            path="/opds/navigation/lcc",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/navigation/lcc")
def opds_lcc_index_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        return _build_lcc_index_response(
            request=request,
            repository_id=repository_id,
            path=f"/repositories/{repository_id}/opds/navigation/lcc",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/lcc/{top_code}")
def opds_lcc_top_level_feed(
    top_code: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    normalized_code = top_code.strip().upper()
    heading = next(
        (
            item
            for item in store.list_lcc_heading_counts(repository_id=DEFAULT_REPOSITORY_ID, min_count=1, limit=1000, offset=0)
            if str(item.get("code", "")).upper() == normalized_code
        ),
        None,
    )
    if heading is None:
        raise HTTPException(status_code=404, detail="LCC heading not found")

    def build_response() -> dict:
        total, subset = store.page_by_lcc_top_code(
            top_code=normalized_code,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"LCC Top-Level Subject: {heading['term']} ({normalized_code})",
            path=f"/opds/lcc/{normalized_code}",
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


@app.get("/repositories/{repository_id}/opds/lcc/{top_code}")
def opds_lcc_top_level_feed_repository(
    repository_id: str,
    top_code: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    normalized_code = top_code.strip().upper()
    heading = next(
        (
            item
            for item in store.list_lcc_heading_counts(repository_id=repository_id, min_count=1, limit=1000, offset=0)
            if str(item.get("code", "")).upper() == normalized_code
        ),
        None,
    )
    if heading is None:
        raise HTTPException(status_code=404, detail="LCC heading not found")

    def build_response() -> dict:
        total, subset = store.page_by_lcc_top_code(
            top_code=normalized_code,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"LCC Top-Level Subject: {heading['term']} ({normalized_code})",
            path=f"/repositories/{repository_id}/opds/lcc/{normalized_code}",
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


@app.get("/opds/groups/{group_slug}")
def opds_publication_group_feed(
    group_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    group = publication_group_by_slug(group_slug)
    if group is None:
        raise HTTPException(status_code=404, detail="Publication group not found")

    def build_response() -> dict:
        total, subset = store.page_by_publication_group_slug(
            group_slug=group.slug,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Group: {group.title}",
            path=f"/opds/groups/{group.slug}",
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


@app.get("/repositories/{repository_id}/opds/groups/{group_slug}")
def opds_publication_group_feed_repository(
    repository_id: str,
    group_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    group = publication_group_by_slug(group_slug)
    if group is None:
        raise HTTPException(status_code=404, detail="Publication group not found")

    def build_response() -> dict:
        total, subset = store.page_by_publication_group_slug(
            group_slug=group.slug,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Group: {group.title}",
            path=f"/repositories/{repository_id}/opds/groups/{group.slug}",
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
    _ensure_default_repository()
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
                    "sourceType": repository.source_type,
                },
            }
        )
    return {
        "metadata": {
            "@type": "https://opds.io/opds-catalog",
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


@app.get("/opds/collections")
def opds_collections_index(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        return _build_collections_index_response(
            request=request,
            repository_id=DEFAULT_REPOSITORY_ID,
            path="/opds/collections",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/collections")
def opds_collections_index_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        return _build_collections_index_response(
            request=request,
            repository_id=repository_id,
            path=f"/repositories/{repository_id}/opds/collections",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/classifications")
def opds_classifications_index(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        return _build_classifications_index_response(
            request=request,
            repository_id=DEFAULT_REPOSITORY_ID,
            path="/opds/classifications",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/classifications")
def opds_classifications_index_repository(
    repository_id: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        return _build_classifications_index_response(
            request=request,
            repository_id=repository_id,
            path=f"/repositories/{repository_id}/opds/classifications",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/classifications/{classification_slug}/subjects")
def opds_subclassifications_index(
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        return _build_subclassifications_index_response(
            request=request,
            repository_id=DEFAULT_REPOSITORY_ID,
            category_slug=classification_slug,
            path=f"/opds/classifications/{classification_slug}/subjects",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/classifications/{classification_slug}/subjects")
def opds_subclassifications_index_repository(
    repository_id: str,
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        return _build_subclassifications_index_response(
            request=request,
            repository_id=repository_id,
            category_slug=classification_slug,
            path=f"/repositories/{repository_id}/opds/classifications/{classification_slug}/subjects",
            page=page,
            page_size=page_size,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


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
        total, subset = store.page_by_subject_slug_for_category(
            subject_slug=subject_slug,
            category_slug=classification_slug,
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
        total, subset = store.page_by_subject_slug_for_category(
            subject_slug=subject_slug,
            category_slug=classification_slug,
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
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=language_counts,
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
        )
        return _attach_year_browse_facets(
            request=request,
            response=response,
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
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
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=language_counts,
            repository_id=repository_id,
            year=year,
        )
        return _attach_year_browse_facets(
            request=request,
            response=response,
            repository_id=repository_id,
            year=year,
        )

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/years/{year}/collections/{collection_slug}")
def opds_year_collection_feed(
    year: int,
    collection_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    def build_response() -> dict:
        total, subset = store.page_by_collection_slug_and_publication_year(
            collection_slug=collection_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Collection: {collection_slug}",
            path=f"/opds/years/{year}/collections/{collection_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID, year=year)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/years/{year}/collections/{collection_slug}")
def opds_year_collection_feed_repository(
    repository_id: str,
    year: int,
    collection_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)

    def build_response() -> dict:
        total, subset = store.page_by_collection_slug_and_publication_year(
            collection_slug=collection_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Collection: {collection_slug}",
            path=f"/repositories/{repository_id}/opds/years/{year}/collections/{collection_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=repository_id),
            repository_id=repository_id,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=repository_id, year=year)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/years/{year}/classifications/{classification_slug}")
def opds_year_classification_feed(
    year: int,
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    category_entry = next(
        (item for item in store.list_category_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID) if item["slug"] == classification_slug),
        None,
    )
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_category_slug_and_publication_year(
            category_slug=classification_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Classification: {category_name}",
            path=f"/opds/years/{year}/classifications/{classification_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
        )
        response = _attach_subclassification_facets(
            request=request,
            response=response,
            repository_id=DEFAULT_REPOSITORY_ID,
            category_slug=classification_slug,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID, year=year)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/years/{year}/classifications/{classification_slug}")
def opds_year_classification_feed_repository(
    repository_id: str,
    year: int,
    classification_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    category_entry = next(
        (item for item in store.list_category_counts_by_publication_year(year=year, repository_id=repository_id) if item["slug"] == classification_slug),
        None,
    )
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    category_name = str(category_entry["name"])

    def build_response() -> dict:
        total, subset = store.page_by_category_slug_and_publication_year(
            category_slug=classification_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Classification: {category_name}",
            path=f"/repositories/{repository_id}/opds/years/{year}/classifications/{classification_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=repository_id),
            repository_id=repository_id,
            year=year,
        )
        response = _attach_subclassification_facets(
            request=request,
            response=response,
            repository_id=repository_id,
            category_slug=classification_slug,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=repository_id, year=year)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/years/{year}/classifications/{classification_slug}/subjects/{subject_slug}")
def opds_year_subclassification_feed(
    year: int,
    classification_slug: str,
    subject_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    category_entry = next(
        (item for item in store.list_category_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID) if item["slug"] == classification_slug),
        None,
    )
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    subject_entry = next(
        (
            item
            for item in store.list_subject_counts_for_category_by_publication_year(
                category_slug=classification_slug,
                year=year,
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
        total, subset = store.page_by_subject_slug_and_category_and_publication_year(
            subject_slug=subject_slug,
            category_slug=classification_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Sub-Classification: {subject_name}",
            path=f"/opds/years/{year}/classifications/{classification_slug}/subjects/{subject_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response["metadata"]["belongsTo"] = {"classification": category_name}
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID, year=year)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/years/{year}/classifications/{classification_slug}/subjects/{subject_slug}")
def opds_year_subclassification_feed_repository(
    repository_id: str,
    year: int,
    classification_slug: str,
    subject_slug: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    category_entry = next(
        (item for item in store.list_category_counts_by_publication_year(year=year, repository_id=repository_id) if item["slug"] == classification_slug),
        None,
    )
    if category_entry is None:
        raise HTTPException(status_code=404, detail="Classification not found")
    subject_entry = next(
        (
            item
            for item in store.list_subject_counts_for_category_by_publication_year(
                category_slug=classification_slug,
                year=year,
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
        total, subset = store.page_by_subject_slug_and_category_and_publication_year(
            subject_slug=subject_slug,
            category_slug=classification_slug,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Sub-Classification: {subject_name}",
            path=f"/repositories/{repository_id}/opds/years/{year}/classifications/{classification_slug}/subjects/{subject_slug}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response["metadata"]["belongsTo"] = {"classification": category_name}
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=repository_id),
            repository_id=repository_id,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=repository_id, year=year)

    return _cached_opds_response(request, build_response, repository_id=repository_id)


@app.get("/opds/years/{year}/languages/{language_code}")
def opds_year_language_feed(
    year: int,
    language_code: str,
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
) -> dict:
    normalized_language = normalize_language_value(language_code)
    if normalized_language is None:
        raise HTTPException(status_code=400, detail="language_code must be a 2-letter code, 3-letter code, or language name.")

    def build_response() -> dict:
        total, subset = store.page_by_language_and_publication_year(
            language=normalized_language,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Language: {_language_label(normalized_language)}",
            path=f"/opds/years/{year}/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=DEFAULT_REPOSITORY_ID,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=DEFAULT_REPOSITORY_ID),
            repository_id=DEFAULT_REPOSITORY_ID,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=DEFAULT_REPOSITORY_ID, year=year)

    return _cached_opds_response(request, build_response, repository_id=DEFAULT_REPOSITORY_ID)


@app.get("/repositories/{repository_id}/opds/years/{year}/languages/{language_code}")
def opds_year_language_feed_repository(
    repository_id: str,
    year: int,
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
        total, subset = store.page_by_language_and_publication_year(
            language=normalized_language,
            year=year,
            page=page,
            page_size=page_size,
            repository_id=repository_id,
        )
        response = _build_feed_response(
            request=request,
            title=f"Publication Year {year} / Language: {_language_label(normalized_language)}",
            path=f"/repositories/{repository_id}/opds/years/{year}/languages/{normalized_language}",
            page=page,
            page_size=page_size,
            total=total,
            subset=subset,
            repository_id=repository_id,
        )
        response = _attach_year_language_facets(
            request=request,
            response=response,
            language_counts=store.list_language_counts_by_publication_year(year=year, repository_id=repository_id),
            repository_id=repository_id,
            year=year,
        )
        return _attach_year_browse_facets(request=request, response=response, repository_id=repository_id, year=year)

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


@app.post("/admin/cache/invalidate")
def admin_invalidate_cache(request: CacheInvalidateRequest) -> dict:
    repository_id = (request.repository_id or "").strip()
    if repository_id:
        _get_repository_or_404(repository_id)
        removed = _invalidate_opds_cache(repository_id)
        return {"scope": "repository", "repository_id": repository_id, "removed_keys": removed}
    removed = _invalidate_opds_cache()
    return {"scope": "global", "removed_keys": removed}


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


@app.get("/repositories/{repository_id}/classifications/authority-stats")
def classification_authority_stats(
    repository_id: str,
    scheme: str = Query(default="lcc", pattern="^(?i)(lcc|lcsh|thema)$"),
    min_count: int = Query(default=1, ge=1),
    top_limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    _get_repository_or_404(repository_id)
    try:
        stats = store.subject_authority_statistics(
            repository_id=repository_id,
            scheme=scheme,
            min_count=min_count,
            top_limit=top_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"repository_id": repository_id, **stats}


@app.get("/repositories/{repository_id}/classifications/authority-unmapped")
def classification_authority_unmapped(
    repository_id: str,
    scheme: str = Query(default="lcc", pattern="^(?i)(lcc|lcsh|thema)$"),
    min_count: int = Query(default=3, ge=1),
    limit: int = Query(default=100, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    _get_repository_or_404(repository_id)
    try:
        result = store.subject_authority_unmapped(
            repository_id=repository_id,
            scheme=scheme,
            min_count=min_count,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"repository_id": repository_id, **result}


@app.get("/repositories/{repository_id}/classifications/authority-resolve")
def classification_authority_resolve(
    repository_id: str,
    subject: str = Query(..., min_length=1),
) -> dict:
    _get_repository_or_404(repository_id)
    lcc = resolve_lcc(subject)
    lcsh = resolve_lcsh(subject)
    thema = resolve_thema(subject)
    return {
        "repository_id": repository_id,
        "subject": subject,
        "lcc": lcc,
        "lcsh": lcsh,
        "thema": thema,
        "has_mapping": bool(lcc or lcsh or thema),
    }


@app.post("/repositories/{repository_id}/reindex/subjects")
def reindex_repository_subjects(repository_id: str, request: SubjectBackfillRequest) -> dict:
    return backfill_repository_subjects(repository_id=repository_id, request=request)


@app.post("/repositories/{repository_id}/reindex/subject-authorities")
def reindex_repository_subject_authorities(repository_id: str, request: SubjectBackfillRequest) -> dict:
    return backfill_repository_subject_authorities(repository_id=repository_id, request=request)


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
