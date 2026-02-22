from __future__ import annotations

import random
import re
import time
from datetime import UTC, datetime
from typing import Any

import requests

from app.models import NormalizedPublication
from app.store import IngestResult, PublicationStore, RepositoryConfig
from app.transform import normalize_language_value


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _first_str(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _slugify(value: str | None) -> str | None:
    if not value:
        return None
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or None


def _extract_year(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if 1900 <= value <= 2199 else None
    text = str(value)
    match = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text)
    if not match:
        return None
    year = int(match.group(1))
    return year if 1900 <= year <= 2199 else None


def _normalize_springer_record(record: dict[str, Any], repository_id: str) -> NormalizedPublication | None:
    title = _first_str(record.get("title"))
    if title is None:
        return None

    source_publication_id = _first_str(record.get("identifier"), record.get("doi"), record.get("id"), record.get("publicationName"), title)
    if source_publication_id is None:
        return None

    creator_values: list[str] = []
    for creator in _as_list(record.get("creators")):
        if isinstance(creator, dict):
            name = _first_str(creator.get("creator"), creator.get("name"))
            if name:
                creator_values.append(name)
        elif isinstance(creator, str) and creator.strip():
            creator_values.append(creator.strip())

    language = normalize_language_value(record.get("language"))
    published = _first_str(record.get("publicationDate"), record.get("coverDate"), record.get("onlineDate"), record.get("date"))
    publisher = _first_str(record.get("publisher"), record.get("publicationName"))

    links: list[dict[str, str]] = []
    for url_item in _as_list(record.get("url")):
        if not isinstance(url_item, dict):
            continue
        href = _first_str(url_item.get("value"), url_item.get("url"), url_item.get("href"))
        if href is None:
            continue
        media_type = _first_str(url_item.get("type"), url_item.get("format"), url_item.get("contentType"))
        media_type = media_type or ("application/pdf" if href.lower().endswith(".pdf") else "application/octet-stream")
        links.append({
            "href": href,
            "rel": "http://opds-spec.org/acquisition/open-access",
            "type": media_type,
        })

    subjects = [value.strip() for value in _as_list(record.get("keyword")) if isinstance(value, str) and value.strip()]

    return NormalizedPublication(
        publication_id=source_publication_id,
        repository_id=repository_id,
        source_publication_id=source_publication_id,
        title=title,
        authors=creator_values,
        language=language,
        publisher=publisher,
        published=published,
        identifier=_first_str(record.get("doi"), source_publication_id),
        subjects=subjects,
        links=links,
        source="springer-openaccess",
        collection=None,
        collection_slug=None,
        series_name=None,
        series_slug=None,
        series_position=None,
        publisher_slug=_slugify(publisher),
        publication_year=_extract_year(published),
        raw=record,
    )


class SpringerSource:
    def __init__(
        self,
        base_url: str = "https://api.springernature.com/openaccess/json",
        timeout_seconds: int = 45,
        max_retries: int = 4,
        backoff_seconds: float = 1.0,
    ) -> None:
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._session = requests.Session()

    def _request_page(self, params: dict[str, Any]) -> dict[str, Any]:
        attempts = 0
        while True:
            attempts += 1
            try:
                response = self._session.get(self._base_url, params=params, timeout=self._timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Springer API returned a non-object response")
                return payload
            except Exception:
                if attempts > self._max_retries:
                    raise
                sleep_seconds = self._backoff_seconds * (2 ** (attempts - 1)) + random.uniform(0, 0.25)
                time.sleep(sleep_seconds)

    def ingest_repository(
        self,
        store: PublicationStore,
        repository: RepositoryConfig,
        max_records: int | None = None,
    ) -> IngestResult:
        config = repository.config if isinstance(repository.config, dict) else {}
        api_key = _first_str(config.get("api_key"), config.get("apiKey"))
        if api_key is None:
            raise ValueError("Repository config must include api_key")

        query = _first_str(config.get("query")) or "type:Book"
        page_size = int(config.get("page_size", 50))
        page_size = max(1, min(page_size, 100))

        checkpoint_key = f"springer::{repository.repository_id}"
        checkpoint = store.get_checkpoint(checkpoint_key, repository_id=repository.repository_id)
        start_offset = 1
        if checkpoint and checkpoint.state:
            last_offset = checkpoint.state.get("last_offset")
            if isinstance(last_offset, int) and last_offset > 0:
                start_offset = last_offset

        result = IngestResult(accepted=0, rejected=0, errors=[])
        offset = start_offset

        while True:
            params = {
                "api_key": api_key,
                "q": query,
                "p": page_size,
                "s": offset,
            }
            payload = self._request_page(params)
            records = payload.get("records")
            if not isinstance(records, list) or not records:
                break

            for record in records:
                if not isinstance(record, dict):
                    result.rejected += 1
                    continue
                normalized = _normalize_springer_record(record, repository_id=repository.repository_id)
                if normalized is None:
                    result.rejected += 1
                    continue
                store.upsert(normalized)
                result.accepted += 1
                if max_records is not None and result.accepted >= max_records:
                    break

            store.upsert_checkpoint(
                checkpoint_key=checkpoint_key,
                repository_id=repository.repository_id,
                source_type="springer-openaccess",
                base_url=self._base_url,
                metadata_prefix="springer-openaccess",
                set_name=None,
                last_from_date=None,
                last_until_date=datetime.now(UTC).date().isoformat(),
                state={
                    "last_offset": offset + page_size,
                    "page_size": page_size,
                    "query": query,
                },
            )

            if max_records is not None and result.accepted >= max_records:
                break

            if len(records) < page_size:
                break
            offset += page_size

        return result
