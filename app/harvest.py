from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin

from app.sources import extract_json_records, load_json_payload_from_url, load_oai_dc_records
from app.store import HarvestCheckpoint, PublicationStore
from app.transform import normalize_json_record, normalize_oai_record


@dataclass
class HarvestRunSummary:
    checkpoint_key: str
    repository_id: str
    source_type: str
    base_url: str
    accepted: int
    rejected: int
    from_date: str | None
    until_date: str
    updated_checkpoint_until: str
    error: str | None = None


def _today_ymd() -> str:
    return datetime.now(UTC).date().isoformat()


def _max_date_string(values: list[str | None]) -> str | None:
    candidates = sorted([value for value in values if value])
    return candidates[-1] if candidates else None


def _set_repository_on_publication(publication, repository_id: str):
    publication.repository_id = repository_id
    if not publication.source_publication_id:
        publication.source_publication_id = publication.publication_id
    return publication


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


def run_incremental_for_checkpoint(
    store: PublicationStore,
    checkpoint: HarvestCheckpoint,
    max_records: int | None = None,
) -> HarvestRunSummary:
    effective_from = checkpoint.last_until_date
    effective_until = _today_ymd()
    accepted = 0
    rejected = 0
    harvested_dates: list[str | None] = []

    records = load_oai_dc_records(
        base_url=checkpoint.base_url,
        metadata_prefix=checkpoint.metadata_prefix,
        set_name=checkpoint.set_name,
        from_date=effective_from,
        until_date=effective_until,
        max_records=max_records,
    )
    for fields in records:
        normalized = normalize_oai_record(fields)
        if normalized is None:
            rejected += 1
            continue
        store.upsert(_set_repository_on_publication(normalized, checkpoint.repository_id))
        harvested_dates.append(normalized.published)
        accepted += 1

    latest_harvested = _max_date_string(harvested_dates) or effective_until
    store.upsert_checkpoint(
        checkpoint_key=checkpoint.checkpoint_key,
        repository_id=checkpoint.repository_id,
        source_type=checkpoint.source_type,
        base_url=checkpoint.base_url,
        metadata_prefix=checkpoint.metadata_prefix,
        set_name=checkpoint.set_name,
        last_from_date=effective_from,
        last_until_date=latest_harvested,
        state=checkpoint.state,
    )
    return HarvestRunSummary(
        checkpoint_key=checkpoint.checkpoint_key,
        repository_id=checkpoint.repository_id,
        source_type=checkpoint.source_type,
        base_url=checkpoint.base_url,
        accepted=accepted,
        rejected=rejected,
        from_date=effective_from,
        until_date=effective_until,
        updated_checkpoint_until=latest_harvested,
    )


def run_incremental_for_opds_json_checkpoint(
    store: PublicationStore,
    checkpoint: HarvestCheckpoint,
    max_records: int | None = None,
) -> HarvestRunSummary:
    accepted = 0
    rejected = 0
    processed = 0
    today = _today_ymd()
    state = checkpoint.state if isinstance(checkpoint.state, dict) else {}
    next_url_value = state.get("next_url")
    current_url = next_url_value if isinstance(next_url_value, str) and next_url_value.strip() else checkpoint.base_url
    last_url = current_url
    next_url_to_store = checkpoint.base_url
    pages_crawled = 0
    pending_urls = [item for item in state.get("pending_urls", []) if isinstance(item, str) and item.strip()]
    visited_urls = set(item for item in state.get("visited_urls", []) if isinstance(item, str) and item.strip())

    while current_url:
        visited_urls.add(current_url)
        payload = load_json_payload_from_url(current_url)
        last_url = current_url
        pages_crawled += 1
        page_records = 0
        for raw in extract_json_records(payload):
            normalized = normalize_json_record(raw)
            processed += 1
            page_records += 1
            if normalized is None:
                rejected += 1
            else:
                store.upsert(_set_repository_on_publication(normalized, checkpoint.repository_id))
                accepted += 1
            if max_records and processed >= max_records:
                break

        next_url = _extract_opds_next_url(payload, current_url)
        if not next_url and page_records == 0:
            for candidate in _extract_opds_navigation_urls(payload, current_url):
                if candidate in visited_urls or candidate in pending_urls:
                    continue
                pending_urls.append(candidate)
        if max_records and processed >= max_records:
            if next_url:
                next_url_to_store = next_url
            elif pending_urls:
                next_url_to_store = pending_urls[0]
            else:
                next_url_to_store = checkpoint.base_url
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
        next_url_to_store = checkpoint.base_url
        break

    store.upsert_checkpoint(
        checkpoint_key=checkpoint.checkpoint_key,
        repository_id=checkpoint.repository_id,
        source_type=checkpoint.source_type,
        base_url=checkpoint.base_url,
        metadata_prefix=checkpoint.metadata_prefix,
        set_name=checkpoint.set_name,
        last_from_date=checkpoint.last_from_date,
        last_until_date=today,
        state={
            "next_url": next_url_to_store,
            "last_url": last_url,
            "pages_crawled": pages_crawled,
            "records_processed": processed,
            "pending_urls": pending_urls[:500],
            "visited_urls": list(visited_urls)[-2000:],
        },
    )

    return HarvestRunSummary(
        checkpoint_key=checkpoint.checkpoint_key,
        repository_id=checkpoint.repository_id,
        source_type=checkpoint.source_type,
        base_url=checkpoint.base_url,
        accepted=accepted,
        rejected=rejected,
        from_date=None,
        until_date=today,
        updated_checkpoint_until=today,
    )


def run_incremental_for_all_checkpoints(
    store: PublicationStore,
    max_records: int | None = None,
) -> dict:
    checkpoints = store.list_checkpoints()
    results: list[HarvestRunSummary] = []

    for checkpoint in checkpoints:
        try:
            if checkpoint.source_type == "oai-pmh":
                summary = run_incremental_for_checkpoint(store=store, checkpoint=checkpoint, max_records=max_records)
            elif checkpoint.source_type == "opds-json":
                summary = run_incremental_for_opds_json_checkpoint(store=store, checkpoint=checkpoint, max_records=max_records)
            else:
                raise ValueError(f"Unsupported checkpoint source_type: {checkpoint.source_type}")
            results.append(summary)
        except Exception as exc:  # pragma: no cover - scheduler safety net
            results.append(
                HarvestRunSummary(
                    checkpoint_key=checkpoint.checkpoint_key,
                    repository_id=checkpoint.repository_id,
                    source_type=checkpoint.source_type,
                    base_url=checkpoint.base_url,
                    accepted=0,
                    rejected=0,
                    from_date=checkpoint.last_until_date,
                    until_date=_today_ymd(),
                    updated_checkpoint_until=checkpoint.last_until_date or _today_ymd(),
                    error=str(exc),
                )
            )

    return {
        "run_at": datetime.now(UTC).isoformat(),
        "checkpoints_total": len(checkpoints),
        "results": [item.__dict__ for item in results],
        "max_records": max_records,
    }
