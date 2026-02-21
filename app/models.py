from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NormalizedPublication:
    publication_id: str
    title: str
    authors: list[str] = field(default_factory=list)
    language: str | None = None
    publisher: str | None = None
    published: str | None = None
    identifier: str | None = None
    subjects: list[str] = field(default_factory=list)
    links: list[dict[str, Any]] = field(default_factory=list)
    source: str = "unknown"
    collection: str | None = None
    collection_slug: str | None = None
    series_name: str | None = None
    series_slug: str | None = None
    series_position: int | None = None
    publisher_slug: str | None = None
    publication_year: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)
