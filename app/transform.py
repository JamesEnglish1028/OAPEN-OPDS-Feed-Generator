from __future__ import annotations

import hashlib
import re
from typing import Any

from app.models import NormalizedPublication


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


def _normalize_links(raw: dict[str, Any]) -> list[dict[str, Any]]:
    links = raw.get("links") or raw.get("formats") or raw.get("files") or []
    normalized: list[dict[str, Any]] = []
    for link in _as_list(links):
        if not isinstance(link, dict):
            continue
        href = _first_str(link.get("href"), link.get("url"), link.get("link"))
        if not href:
            continue
        rel = _first_str(link.get("rel")) or "http://opds-spec.org/acquisition"
        media_type = _first_str(link.get("type"), link.get("mediaType"), link.get("mimetype"))
        if media_type is None:
            media_type = "application/epub+zip" if href.endswith(".epub") else "application/octet-stream"
        normalized.append({"href": href, "rel": rel, "type": media_type})
    return normalized


def _slugify(value: str | None) -> str | None:
    if not value:
        return None
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or None


def _extract_publication_year(value: Any) -> int | None:
    if not value:
        return None
    if isinstance(value, int):
        return value if 1900 <= value <= 2199 else None
    text = str(value)
    match = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text)
    if not match:
        return None
    year = int(match.group(1))
    return year if 1900 <= year <= 2199 else None


def normalize_json_record(raw: dict[str, Any]) -> NormalizedPublication | None:
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    identifier = _first_str(
        raw.get("id"),
        raw.get("uuid"),
        raw.get("identifier"),
        raw.get("doi"),
        raw.get("isbn"),
        metadata.get("identifier"),
        metadata.get("doi"),
        metadata.get("isbn"),
    )
    title = _first_str(raw.get("title"), raw.get("name"), metadata.get("title"))
    if not title:
        return None
    if not identifier:
        signature = hashlib.sha1(title.encode("utf-8")).hexdigest()[:12]
        identifier = f"oapen:auto:{signature}"

    author_field = raw.get("authors") or raw.get("author") or raw.get("creators") or metadata.get("author")
    editor_field = metadata.get("editor")
    authors = [str(v).strip() for v in _as_list(author_field) if str(v).strip()]
    if not authors:
        authors = [str(v).strip() for v in _as_list(editor_field) if str(v).strip()]
    if not authors:
        creator_nodes = raw.get("creator")
        if isinstance(creator_nodes, list):
            authors = [str(v.get("name", "")).strip() for v in creator_nodes if isinstance(v, dict) and str(v.get("name", "")).strip()]

    subjects = [str(v).strip() for v in _as_list(raw.get("subjects") or raw.get("keywords") or metadata.get("subject")) if str(v).strip()]
    language = _first_str(raw.get("language"), raw.get("lang"), metadata.get("language"))
    if language is None:
        language = _first_str(*[str(v) for v in _as_list(metadata.get("language"))])
    published_candidate = _first_str(
        raw.get("published"),
        raw.get("publication_date"),
        raw.get("date"),
        metadata.get("published"),
        metadata.get("modified"),
    )
    if published_candidate is None and isinstance(metadata.get("published"), int):
        published_candidate = str(metadata.get("published"))
    published = published_candidate
    publication_year = (
        _extract_publication_year(metadata.get("published"))
        or _extract_publication_year(published)
        or _extract_publication_year(metadata.get("modified"))
    )
    publisher_value = _first_str(raw.get("publisher"), metadata.get("publisher"), metadata.get("imprint"))
    funders = [str(v).strip() for v in _as_list(metadata.get("funder")) if isinstance(v, str) and str(v).strip()]
    collection = funders[0] if funders else None
    # Keep root navigation funder-based only; do not mirror publisher into collection.
    if collection and publisher_value and collection.casefold() == publisher_value.casefold():
        collection = None

    belongs_to = metadata.get("belongsTo")
    series_name: str | None = None
    series_position: int | None = None
    if isinstance(belongs_to, dict):
        series_name = _first_str(belongs_to.get("series"), belongs_to.get("name"))
        raw_series_position = belongs_to.get("seriesNumber")
        if isinstance(raw_series_position, int):
            series_position = raw_series_position
        elif isinstance(raw_series_position, str) and raw_series_position.strip().isdigit():
            series_position = int(raw_series_position.strip())

    return NormalizedPublication(
        publication_id=identifier,
        title=title,
        authors=authors,
        language=language,
        publisher=publisher_value,
        published=published,
        identifier=identifier,
        subjects=subjects,
        links=_normalize_links(raw),
        source="json",
        collection=collection,
        collection_slug=_slugify(collection),
        series_name=series_name,
        series_slug=_slugify(series_name),
        series_position=series_position,
        publisher_slug=_slugify(_first_str(raw.get("publisher"), metadata.get("publisher"), metadata.get("imprint"))),
        publication_year=publication_year,
        raw=raw,
    )


def normalize_oai_record(fields: dict[str, list[str]]) -> NormalizedPublication | None:
    title = _first_str(*fields.get("title", []))
    identifier = _first_str(*fields.get("identifier", []))
    if not title:
        return None
    if not identifier:
        signature = hashlib.sha1(title.encode("utf-8")).hexdigest()[:12]
        identifier = f"oapen:oai:{signature}"

    links: list[dict[str, Any]] = []
    for value in fields.get("identifier", []):
        if value.startswith("http://") or value.startswith("https://"):
            media_type = "application/epub+zip" if value.endswith(".epub") else "application/octet-stream"
            links.append(
                {
                    "href": value,
                    "rel": "http://opds-spec.org/acquisition",
                    "type": media_type,
                }
            )

    return NormalizedPublication(
        publication_id=identifier,
        title=title,
        authors=[v for v in fields.get("creator", []) if v],
        language=_first_str(*fields.get("language", [])),
        publisher=_first_str(*fields.get("publisher", [])),
        published=_first_str(*fields.get("date", []), *fields.get("datestamp", [])),
        identifier=identifier,
        subjects=[v for v in fields.get("subject", []) if v],
        links=links,
        source="oai-pmh",
        collection=None,
        collection_slug=None,
        series_name=None,
        series_slug=None,
        series_position=None,
        publisher_slug=_slugify(_first_str(*fields.get("publisher", []))),
        publication_year=_extract_publication_year(_first_str(*fields.get("date", []), *fields.get("datestamp", []))),
        raw={k: v for k, v in fields.items()},
    )
