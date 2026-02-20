from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.harvest import run_incremental_for_all_checkpoints
from app.store import PublicationStore


class IncrementalHarvestScheduler:
    def __init__(self, store: PublicationStore, hour_utc: int = 2, minute_utc: int = 0) -> None:
        self._store = store
        self._hour_utc = hour_utc
        self._minute_utc = minute_utc
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._job_id = "daily_incremental_harvest"

    def start(self) -> None:
        if self._scheduler.running:
            return
        trigger = CronTrigger(hour=self._hour_utc, minute=self._minute_utc, timezone="UTC")
        self._scheduler.add_job(
            self._run_job,
            trigger=trigger,
            id=self._job_id,
            replace_existing=True,
        )
        self._scheduler.start()

    def shutdown(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def _run_job(self) -> None:
        run_incremental_for_all_checkpoints(self._store)

    def is_running(self) -> bool:
        return bool(self._scheduler.running)
