from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from app.sources import load_oai_dc_records
from app.store import HarvestCheckpoint, PublicationStore
from app.transform import normalize_oai_record


@dataclass
class HarvestRunSummary:
    checkpoint_key: str
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
        store.upsert(normalized)
        harvested_dates.append(normalized.published)
        accepted += 1

    latest_harvested = _max_date_string(harvested_dates) or effective_until
    store.upsert_checkpoint(
        checkpoint_key=checkpoint.checkpoint_key,
        base_url=checkpoint.base_url,
        metadata_prefix=checkpoint.metadata_prefix,
        set_name=checkpoint.set_name,
        last_from_date=effective_from,
        last_until_date=latest_harvested,
    )
    return HarvestRunSummary(
        checkpoint_key=checkpoint.checkpoint_key,
        base_url=checkpoint.base_url,
        accepted=accepted,
        rejected=rejected,
        from_date=effective_from,
        until_date=effective_until,
        updated_checkpoint_until=latest_harvested,
    )


def run_incremental_for_all_checkpoints(
    store: PublicationStore,
    max_records: int | None = None,
) -> dict:
    checkpoints = store.list_checkpoints()
    results: list[HarvestRunSummary] = []

    for checkpoint in checkpoints:
        try:
            summary = run_incremental_for_checkpoint(store=store, checkpoint=checkpoint, max_records=max_records)
            results.append(summary)
        except Exception as exc:  # pragma: no cover - scheduler safety net
            results.append(
                HarvestRunSummary(
                    checkpoint_key=checkpoint.checkpoint_key,
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
    }
