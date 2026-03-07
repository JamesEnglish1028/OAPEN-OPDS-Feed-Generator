from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Index, String, Text, and_, create_engine, func as sqla_func, or_, select, text as sqla_text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.sql import func

from app.models import NormalizedPublication
from app.publication_groups import group_slugs_for_subject_names, list_publication_groups, publication_group_by_slug
from app.subject_aliases import canonicalize_subject_term
from app.subject_authorities import resolve_lcc, resolve_lcsh, resolve_thema
from app.subject_categories import classify_subject_category


def _clamp_index_text(value: str | None, max_length: int = 512) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return text[:max_length]


def _slugify_value(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().lower()
    if not text:
        return None
    slug_chars = [char if char.isalnum() else "-" for char in text]
    slug = "".join(slug_chars).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    if not slug:
        return None
    return slug[:512]


@dataclass
class IngestResult:
    accepted: int = 0
    rejected: int = 0
    errors: list[str] | None = None


@dataclass
class RepositoryConfig:
    repository_id: str
    source_type: str
    name: str
    config: dict[str, Any]
    is_active: bool
    updated_at: str
    created_at: str


@dataclass
class HarvestCheckpoint:
    checkpoint_key: str
    repository_id: str
    source_type: str
    base_url: str
    metadata_prefix: str
    set_name: str | None
    last_from_date: str | None
    last_until_date: str | None
    state: dict[str, Any] | None
    updated_at: str


@dataclass
class SubjectBackfillResult:
    processed_publications: int
    indexed_subject_rows: int
    next_cursor: str | None
    has_more: bool
    skipped_publications: int = 0
    error_examples: list[dict[str, str]] | None = None


@dataclass
class SubjectAuthorityBackfillResult:
    processed_publications: int
    indexed_authority_rows: int
    next_cursor: str | None
    has_more: bool
    skipped_publications: int = 0
    error_examples: list[dict[str, str]] | None = None


class Base(DeclarativeBase):
    pass


class RepositoryRow(Base):
    __tablename__ = "repositories"

    repository_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    config_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class PublicationRow(Base):
    __tablename__ = "publications"

    publication_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    repository_id: Mapped[str] = mapped_column(String(128), nullable=False, default="default")
    source_publication_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    title: Mapped[str] = mapped_column(String(2000), nullable=False)
    authors_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    authors_enriched_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    language: Mapped[str | None] = mapped_column(String(64), nullable=True)
    publisher: Mapped[str | None] = mapped_column(String(512), nullable=True)
    published: Mapped[str | None] = mapped_column(String(64), nullable=True)
    identifier: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    subjects_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    subject_authorities_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    links_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="unknown")
    collection: Mapped[str | None] = mapped_column(String(512), nullable=True)
    collection_slug: Mapped[str | None] = mapped_column(String(512), nullable=True)
    series_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    series_slug: Mapped[str | None] = mapped_column(String(512), nullable=True)
    series_position: Mapped[int | None] = mapped_column(nullable=True)
    publisher_slug: Mapped[str | None] = mapped_column(String(512), nullable=True)
    publication_year: Mapped[int | None] = mapped_column(nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class PublicationSubjectRow(Base):
    __tablename__ = "publication_subjects"

    publication_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    repository_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    subject_slug: Mapped[str] = mapped_column(String(512), primary_key=True)
    subject_name: Mapped[str] = mapped_column(String(512), nullable=False)


class PublicationSubjectCategoryRow(Base):
    __tablename__ = "publication_subject_categories"
    __table_args__ = (
        Index("ix_publication_subject_categories_repository_slug", "repository_id", "category_slug"),
    )

    publication_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    repository_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    category_slug: Mapped[str] = mapped_column(String(512), primary_key=True)
    category_name: Mapped[str] = mapped_column(String(512), nullable=False)


class PublicationGroupMembershipRow(Base):
    __tablename__ = "publication_group_memberships"
    __table_args__ = (
        Index("ix_publication_group_memberships_repository_group", "repository_id", "group_slug"),
    )

    publication_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    repository_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    group_slug: Mapped[str] = mapped_column(String(128), primary_key=True)


class HarvestCheckpointRow(Base):
    __tablename__ = "harvest_checkpoints"

    checkpoint_key: Mapped[str] = mapped_column(String(1024), primary_key=True)
    repository_id: Mapped[str] = mapped_column(String(128), nullable=False, default="default")
    source_type: Mapped[str] = mapped_column(String(64), nullable=False, default="oai-pmh")
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    metadata_prefix: Mapped[str] = mapped_column(String(128), nullable=False)
    set_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_from_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_until_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class PublicationStore:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._engine = create_engine(database_url, future=True)
        self._is_postgres = database_url.startswith("postgresql")

    @staticmethod
    def _storage_publication_id(repository_id: str, source_publication_id: str) -> str:
        if repository_id == "default":
            return source_publication_id
        return f"{repository_id}::{source_publication_id}"

    @staticmethod
    def _collection_min_count(repository_id: str) -> int:
        return 5 if repository_id == "default" else 2

    def initialize(self) -> None:
        Base.metadata.create_all(self._engine)
        self._ensure_publications_compat_columns()

    def _ensure_publications_compat_columns(self) -> None:
        required_columns = {
            "repository_id": {
                "sqlite": "TEXT NOT NULL DEFAULT 'default'",
                "postgres": "VARCHAR(128) NOT NULL DEFAULT 'default'",
            },
            "source_publication_id": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "authors_enriched_json": {
                "sqlite": "TEXT NOT NULL DEFAULT '[]'",
                "postgres": "TEXT NOT NULL DEFAULT '[]'",
            },
            "subject_authorities_json": {
                "sqlite": "TEXT NOT NULL DEFAULT '[]'",
                "postgres": "TEXT NOT NULL DEFAULT '[]'",
            },
            "collection": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "collection_slug": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "series_name": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "series_slug": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "series_position": {
                "sqlite": "INTEGER",
                "postgres": "INTEGER",
            },
            "publisher_slug": {
                "sqlite": "TEXT",
                "postgres": "VARCHAR(512)",
            },
            "publication_year": {
                "sqlite": "INTEGER",
                "postgres": "INTEGER",
            },
        }
        with self._session() as session:
            if self._is_postgres:
                for column_name, ddl_variants in required_columns.items():
                    column_ddl = ddl_variants["postgres"]
                    session.execute(
                        sqla_text(
                            f"ALTER TABLE publications ADD COLUMN IF NOT EXISTS {column_name} {column_ddl}"
                        )
                    )
                session.commit()
                return

            existing_columns = {
                str(row[1])
                for row in session.execute(sqla_text("PRAGMA table_info(publications)")).all()
                if len(row) > 1
            }
            for column_name, ddl_variants in required_columns.items():
                if column_name in existing_columns:
                    continue
                column_ddl = ddl_variants["sqlite"]
                session.execute(sqla_text(f"ALTER TABLE publications ADD COLUMN {column_name} {column_ddl}"))
            session.commit()

    def _session(self) -> Session:
        return Session(self._engine)

    def _subject_facet_rows(
        self,
        *,
        repository_id: str,
        min_count: int = 1,
        limit: int | None = None,
        order_by_count_desc: bool = False,
    ) -> list[tuple[str, str, int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            totals_statement = (
                select(
                    PublicationSubjectRow.subject_slug,
                    sqla_func.count(PublicationSubjectRow.publication_id).label("total_count"),
                )
                .where(PublicationSubjectRow.repository_id == repository_id)
                .group_by(PublicationSubjectRow.subject_slug)
                .having(sqla_func.count(PublicationSubjectRow.publication_id) >= effective_min_count)
            )
            total_rows = session.execute(totals_statement).all()
            if not total_rows:
                return []

            total_by_slug = {
                slug: int(count)
                for slug, count in total_rows
                if isinstance(slug, str) and slug
            }
            if not total_by_slug:
                return []

            variant_statement = (
                select(
                    PublicationSubjectRow.subject_slug,
                    PublicationSubjectRow.subject_name,
                    sqla_func.count(PublicationSubjectRow.publication_id).label("variant_count"),
                )
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectRow.subject_slug.in_(list(total_by_slug.keys())),
                )
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
            )
            variant_rows = session.execute(variant_statement).all()

            best_label_by_slug: dict[str, tuple[str, int]] = {}
            for slug, name, variant_count in variant_rows:
                if not isinstance(slug, str) or not slug or not isinstance(name, str) or not name:
                    continue
                candidate = (name, int(variant_count))
                existing = best_label_by_slug.get(slug)
                if existing is None:
                    best_label_by_slug[slug] = candidate
                    continue
                existing_name, existing_count = existing
                if candidate[1] > existing_count:
                    best_label_by_slug[slug] = candidate
                    continue
                if candidate[1] == existing_count:
                    if len(candidate[0]) < len(existing_name):
                        best_label_by_slug[slug] = candidate
                        continue
                    if len(candidate[0]) == len(existing_name) and candidate[0].casefold() < existing_name.casefold():
                        best_label_by_slug[slug] = candidate

            rows = [
                (slug, best_label_by_slug.get(slug, (slug.replace("-", " "), 0))[0], count)
                for slug, count in total_by_slug.items()
            ]
            if order_by_count_desc:
                rows.sort(key=lambda item: (-item[2], item[1].casefold()))
            else:
                rows.sort(key=lambda item: item[1].casefold())
            if limit is not None:
                rows = rows[: max(limit, 1)]
            return rows

    @staticmethod
    def _normalized_subject_rows(subjects: list[Any]) -> list[dict[str, str]]:
        normalized_subjects = []
        seen_subject_slugs: set[str] = set()
        for subject in subjects:
            if not isinstance(subject, str):
                continue
            subject_name = _clamp_index_text(canonicalize_subject_term(subject))
            subject_slug = _slugify_value(subject_name)
            if not subject_name or not subject_slug:
                continue
            if subject_slug in seen_subject_slugs:
                continue
            seen_subject_slugs.add(subject_slug)
            normalized_subjects.append({"subject_slug": subject_slug, "subject_name": subject_name})
        return normalized_subjects

    @staticmethod
    def _normalized_subject_authority_rows(normalized_subjects: list[dict[str, str]]) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for item in normalized_subjects:
            subject_slug = item.get("subject_slug")
            subject_name = item.get("subject_name")
            if not isinstance(subject_slug, str) or not subject_slug:
                continue
            if not isinstance(subject_name, str) or not subject_name:
                continue
            mappings: list[dict[str, str]] = []
            lcc_mapping = resolve_lcc(subject_name)
            if isinstance(lcc_mapping, dict):
                mappings.append(lcc_mapping)
            lcsh_mappings = resolve_lcsh(subject_name)
            if isinstance(lcsh_mappings, list):
                mappings.extend([m for m in lcsh_mappings if isinstance(m, dict)])
            thema_mappings = resolve_thema(subject_name)
            if isinstance(thema_mappings, list):
                mappings.extend([m for m in thema_mappings if isinstance(m, dict)])
            for mapping in mappings:
                scheme = _clamp_index_text(mapping.get("scheme"), max_length=256)
                term = _clamp_index_text(mapping.get("term"), max_length=512)
                code = _clamp_index_text(mapping.get("code"), max_length=128) or ""
                if not scheme or not term:
                    continue
                dedupe_key = (subject_slug, scheme, term, code)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                rows.append(
                    {
                        "subject_slug": subject_slug,
                        "subject_name": subject_name,
                        "scheme": scheme,
                        "term": term,
                        "code": code,
                    }
                )
        return rows

    @staticmethod
    def _replace_publication_subjects(
        session: Session,
        *,
        repository_id: str,
        publication_id: str,
        normalized_subjects: list[dict[str, str]],
    ) -> None:
        session.execute(
            PublicationSubjectRow.__table__.delete().where(
                PublicationSubjectRow.repository_id == repository_id,
                PublicationSubjectRow.publication_id == publication_id,
            )
        )
        if normalized_subjects:
            session.add_all(
                [
                    PublicationSubjectRow(
                        publication_id=publication_id,
                        repository_id=repository_id,
                        subject_slug=item["subject_slug"],
                        subject_name=item["subject_name"],
                    )
                    for item in normalized_subjects
                ]
            )

    @staticmethod
    def _normalized_category_rows(normalized_subjects: list[dict[str, str]]) -> list[dict[str, str]]:
        normalized_categories = []
        seen_categories: set[tuple[str, str]] = set()
        for item in normalized_subjects:
            category_name = _clamp_index_text(classify_subject_category(item["subject_name"]))
            category_slug = _slugify_value(category_name)
            if not category_name or not category_slug:
                continue
            key = (category_slug, category_name)
            if key in seen_categories:
                continue
            seen_categories.add(key)
            normalized_categories.append({"category_slug": category_slug, "category_name": category_name})
        return normalized_categories

    @staticmethod
    def _normalized_group_rows(normalized_subjects: list[dict[str, str]]) -> list[dict[str, str]]:
        subject_names = [item["subject_name"] for item in normalized_subjects if isinstance(item.get("subject_name"), str)]
        group_slugs = sorted(group_slugs_for_subject_names(subject_names))
        return [{"group_slug": slug} for slug in group_slugs]

    @staticmethod
    def _replace_publication_subject_categories(
        session: Session,
        *,
        repository_id: str,
        publication_id: str,
        normalized_categories: list[dict[str, str]],
    ) -> None:
        session.execute(
            PublicationSubjectCategoryRow.__table__.delete().where(
                PublicationSubjectCategoryRow.repository_id == repository_id,
                PublicationSubjectCategoryRow.publication_id == publication_id,
            )
        )
        if normalized_categories:
            session.add_all(
                [
                    PublicationSubjectCategoryRow(
                        publication_id=publication_id,
                        repository_id=repository_id,
                        category_slug=item["category_slug"],
                        category_name=item["category_name"],
                    )
                    for item in normalized_categories
                ]
            )

    @staticmethod
    def _replace_publication_group_memberships(
        session: Session,
        *,
        repository_id: str,
        publication_id: str,
        normalized_groups: list[dict[str, str]],
    ) -> None:
        session.execute(
            PublicationGroupMembershipRow.__table__.delete().where(
                PublicationGroupMembershipRow.repository_id == repository_id,
                PublicationGroupMembershipRow.publication_id == publication_id,
            )
        )
        if normalized_groups:
            session.add_all(
                [
                    PublicationGroupMembershipRow(
                        publication_id=publication_id,
                        repository_id=repository_id,
                        group_slug=item["group_slug"],
                    )
                    for item in normalized_groups
                ]
            )

    def upsert_repository(self, repository: RepositoryConfig) -> None:
        payload = {
            "repository_id": repository.repository_id,
            "source_type": repository.source_type,
            "name": repository.name,
            "config_json": json.dumps(repository.config, ensure_ascii=True),
            "is_active": repository.is_active,
            "updated_at": datetime.now(UTC),
        }
        with self._session() as session:
            if self._is_postgres:
                statement = pg_insert(RepositoryRow).values(**payload)
                statement = statement.on_conflict_do_update(
                    index_elements=[RepositoryRow.repository_id],
                    set_=payload,
                )
                session.execute(statement)
            else:
                existing = session.get(RepositoryRow, repository.repository_id)
                if existing is None:
                    session.add(RepositoryRow(**payload))
                else:
                    for key, value in payload.items():
                        setattr(existing, key, value)
            session.commit()

    def get_repository(self, repository_id: str) -> RepositoryConfig | None:
        with self._session() as session:
            row = session.get(RepositoryRow, repository_id)
            if row is None:
                return None
            return self._to_repository(row)

    def list_repositories(self, include_inactive: bool = True) -> list[RepositoryConfig]:
        with self._session() as session:
            statement = select(RepositoryRow).order_by(RepositoryRow.repository_id.asc())
            if not include_inactive:
                statement = statement.where(RepositoryRow.is_active.is_(True))
            rows = session.scalars(statement).all()
            return [self._to_repository(row) for row in rows]

    def delete_repository(self, repository_id: str) -> bool:
        with self._session() as session:
            row = session.get(RepositoryRow, repository_id)
            if row is None:
                return False
            session.delete(row)
            session.commit()
            return True

    def upsert(self, pub: NormalizedPublication) -> None:
        repository_id = pub.repository_id or "default"
        source_publication_id = pub.source_publication_id or pub.publication_id
        storage_publication_id = self._storage_publication_id(repository_id, source_publication_id)
        normalized_subjects = self._normalized_subject_rows(pub.subjects)
        normalized_categories = self._normalized_category_rows(normalized_subjects)
        normalized_groups = self._normalized_group_rows(normalized_subjects)
        normalized_authorities = self._normalized_subject_authority_rows(normalized_subjects)
        payload = {
            "publication_id": storage_publication_id,
            "repository_id": repository_id,
            "source_publication_id": source_publication_id,
            "title": pub.title,
            "authors_json": json.dumps(pub.authors, ensure_ascii=True),
            "authors_enriched_json": json.dumps(pub.authors_enriched, ensure_ascii=True),
            "language": pub.language,
            "publisher": pub.publisher,
            "published": pub.published,
            "identifier": pub.identifier,
            "subjects_json": json.dumps(pub.subjects, ensure_ascii=True),
            "subject_authorities_json": json.dumps(normalized_authorities, ensure_ascii=True),
            "links_json": json.dumps(pub.links, ensure_ascii=True),
            "source": pub.source,
            "collection": pub.collection,
            "collection_slug": pub.collection_slug,
            "series_name": pub.series_name,
            "series_slug": pub.series_slug,
            "series_position": pub.series_position,
            "publisher_slug": pub.publisher_slug,
            "publication_year": pub.publication_year,
            "raw_json": json.dumps(pub.raw, ensure_ascii=True),
            "updated_at": datetime.now(UTC),
        }
        with self._session() as session:
            if self._is_postgres:
                statement = pg_insert(PublicationRow).values(**payload)
                statement = statement.on_conflict_do_update(
                    index_elements=[PublicationRow.publication_id],
                    set_=payload,
                )
                session.execute(statement)
            else:
                existing = session.get(PublicationRow, storage_publication_id)
                if existing is None:
                    session.add(PublicationRow(**payload))
                else:
                    for key, value in payload.items():
                        setattr(existing, key, value)
            self._replace_publication_subjects(
                session,
                repository_id=repository_id,
                publication_id=storage_publication_id,
                normalized_subjects=normalized_subjects,
            )
            self._replace_publication_subject_categories(
                session,
                repository_id=repository_id,
                publication_id=storage_publication_id,
                normalized_categories=normalized_categories,
            )
            self._replace_publication_group_memberships(
                session,
                repository_id=repository_id,
                publication_id=storage_publication_id,
                normalized_groups=normalized_groups,
            )
            session.commit()

    def backfill_publication_subjects(
        self,
        repository_id: str = "default",
        *,
        batch_size: int = 500,
        start_after: str | None = None,
        offset: int | None = None,
    ) -> SubjectBackfillResult:
        limit = max(1, min(batch_size, 5000))
        with self._session() as session:
            statement = (
                select(PublicationRow.publication_id, PublicationRow.subjects_json)
                .where(PublicationRow.repository_id == repository_id)
                .order_by(PublicationRow.publication_id.asc())
            )
            if offset is not None:
                statement = statement.offset(max(offset, 0))
            elif start_after:
                statement = statement.where(PublicationRow.publication_id > start_after)
            rows = session.execute(statement.limit(limit + 1)).all()
            has_more = len(rows) > limit
            work_rows = rows[:limit]
            processed_publications = 0
            indexed_subject_rows = 0
            skipped_publications = 0
            error_examples: list[dict[str, str]] = []
            next_cursor = None
            for publication_id, subjects_json in work_rows:
                next_cursor = publication_id
                try:
                    with session.begin_nested():
                        try:
                            raw_subjects = json.loads(subjects_json or "[]")
                        except json.JSONDecodeError:
                            raw_subjects = []
                        subjects = raw_subjects if isinstance(raw_subjects, list) else []
                        normalized_subjects = self._normalized_subject_rows(subjects)
                        normalized_categories = self._normalized_category_rows(normalized_subjects)
                        normalized_groups = self._normalized_group_rows(normalized_subjects)
                        normalized_authorities = self._normalized_subject_authority_rows(normalized_subjects)
                        self._replace_publication_subjects(
                            session,
                            repository_id=repository_id,
                            publication_id=publication_id,
                            normalized_subjects=normalized_subjects,
                        )
                        self._replace_publication_subject_categories(
                            session,
                            repository_id=repository_id,
                            publication_id=publication_id,
                            normalized_categories=normalized_categories,
                        )
                        self._replace_publication_group_memberships(
                            session,
                            repository_id=repository_id,
                            publication_id=publication_id,
                            normalized_groups=normalized_groups,
                        )
                        session.execute(
                            PublicationRow.__table__.update()
                            .where(
                                PublicationRow.repository_id == repository_id,
                                PublicationRow.publication_id == publication_id,
                            )
                            .values(subject_authorities_json=json.dumps(normalized_authorities, ensure_ascii=True))
                        )
                        session.flush()
                    processed_publications += 1
                    indexed_subject_rows += len(normalized_subjects)
                except Exception as exc:
                    skipped_publications += 1
                    if len(error_examples) < 10:
                        error_examples.append(
                            {
                                "publication_id": publication_id,
                                "error": str(exc),
                            }
                        )
            session.commit()
            return SubjectBackfillResult(
                processed_publications=processed_publications,
                indexed_subject_rows=indexed_subject_rows,
                next_cursor=next_cursor,
                has_more=has_more,
                skipped_publications=skipped_publications,
                error_examples=error_examples,
            )

    def backfill_publication_subject_authorities(
        self,
        *,
        repository_id: str = "default",
        batch_size: int = 500,
        start_after: str | None = None,
        offset: int | None = None,
    ) -> SubjectAuthorityBackfillResult:
        effective_batch_size = max(1, min(batch_size, 5000))
        processed_publications = 0
        indexed_authority_rows = 0
        skipped_publications = 0
        error_examples: list[dict[str, str]] = []

        with self._session() as session:
            selection = (
                select(
                    PublicationRow.publication_id,
                    PublicationRow.subjects_json,
                )
                .where(PublicationRow.repository_id == repository_id)
                .order_by(PublicationRow.publication_id.asc())
            )
            if start_after:
                selection = selection.where(PublicationRow.publication_id > start_after)
            if offset is not None and offset > 0:
                selection = selection.offset(offset)
            work_rows = session.execute(selection.limit(effective_batch_size + 1)).all()
            has_more = len(work_rows) > effective_batch_size
            work_rows = work_rows[:effective_batch_size]

            next_cursor = work_rows[-1][0] if work_rows else None
            for publication_id, subjects_json in work_rows:
                try:
                    raw_subjects = json.loads(subjects_json or "[]")
                    if not isinstance(raw_subjects, list):
                        raw_subjects = []
                    normalized_subjects = self._normalized_subject_rows(raw_subjects)
                    normalized_authorities = self._normalized_subject_authority_rows(normalized_subjects)
                    session.execute(
                        PublicationRow.__table__.update()
                        .where(
                            PublicationRow.repository_id == repository_id,
                            PublicationRow.publication_id == publication_id,
                        )
                        .values(subject_authorities_json=json.dumps(normalized_authorities, ensure_ascii=True))
                    )
                    indexed_authority_rows += len(normalized_authorities)
                    processed_publications += 1
                except Exception as exc:
                    skipped_publications += 1
                    if len(error_examples) < 5:
                        error_examples.append({"publication_id": publication_id, "error": str(exc)})

            session.commit()
            if not work_rows:
                next_cursor = None
                has_more = False
            return SubjectAuthorityBackfillResult(
                processed_publications=processed_publications,
                indexed_authority_rows=indexed_authority_rows,
                next_cursor=next_cursor,
                has_more=has_more,
                skipped_publications=skipped_publications,
                error_examples=error_examples,
            )

    def get(self, publication_id: str, repository_id: str = "default") -> NormalizedPublication | None:
        storage_publication_id = self._storage_publication_id(repository_id, publication_id)
        with self._session() as session:
            row = session.get(PublicationRow, storage_publication_id)
            if row is None:
                statement = select(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.source_publication_id == publication_id,
                )
                row = session.scalars(statement).first()
            if row is None:
                return None
            return self._to_publication(row)

    def all(self, repository_id: str = "default") -> list[NormalizedPublication]:
        with self._session() as session:
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
            )
            rows = session.scalars(statement).all()
            return [self._to_publication(row) for row in rows]

    def page(self, page: int, page_size: int, repository_id: str = "default") -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.repository_id == repository_id)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_collection_slug(
        self,
        collection_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.collection_slug == collection_slug,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.collection_slug == collection_slug)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_language(
        self,
        language: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.language == language,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.language == language)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_series_slug(
        self,
        series_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.series_slug == series_slug,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.series_slug == series_slug)
                .order_by(PublicationRow.series_position.asc().nullslast(), PublicationRow.source_publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_publisher_slug(
        self,
        publisher_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publisher_slug == publisher_slug,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.publisher_slug == publisher_slug)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def search_publications(
        self,
        *,
        repository_id: str = "default",
        query: str | None = None,
        title: str | None = None,
        author: str | None = None,
        publisher: str | None = None,
        series: str | None = None,
        collection: str | None = None,
        subject: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        filters = [PublicationRow.repository_id == repository_id]

        def like_filter(column, value: str):
            return column.ilike(f"%{value.strip()}%")

        keyword = (query or "").strip()
        if keyword:
            filters.append(
                or_(
                    like_filter(PublicationRow.title, keyword),
                    like_filter(PublicationRow.authors_json, keyword),
                    like_filter(PublicationRow.publisher, keyword),
                    like_filter(PublicationRow.series_name, keyword),
                    like_filter(PublicationRow.collection, keyword),
                    like_filter(PublicationRow.subjects_json, keyword),
                )
            )
        if title and title.strip():
            filters.append(like_filter(PublicationRow.title, title))
        if author and author.strip():
            filters.append(like_filter(PublicationRow.authors_json, author))
        if publisher and publisher.strip():
            filters.append(like_filter(PublicationRow.publisher, publisher))
        if series and series.strip():
            filters.append(like_filter(PublicationRow.series_name, series))
        if collection and collection.strip():
            filters.append(like_filter(PublicationRow.collection, collection))
        if subject and subject.strip():
            filters.append(like_filter(PublicationRow.subjects_json, subject))

        where_clause = and_(*filters)
        with self._session() as session:
            total = session.scalar(select(sqla_func.count()).select_from(PublicationRow).where(where_clause)) or 0
            statement = (
                select(PublicationRow)
                .where(where_clause)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def list_collection_counts(self, repository_id: str = "default") -> list[dict[str, str | int]]:
        return self.list_collection_counts_limited(repository_id=repository_id, limit=None, offset=0, order_by_count_desc=False)

    def list_collection_counts_limited(
        self,
        repository_id: str = "default",
        *,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        with self._session() as session:
            count_expr = sqla_func.count(PublicationRow.publication_id)
            min_count = self._collection_min_count(repository_id)
            statement = (
                select(PublicationRow.collection_slug, PublicationRow.collection, count_expr)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.collection_slug.is_not(None))
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
                .having(count_expr >= min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationRow.collection.asc())
            else:
                statement = statement.order_by(PublicationRow.collection.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [{"slug": slug, "name": name, "count": count} for slug, name, count in rows if slug and name]

    def count_collection_facets(self, repository_id: str = "default") -> int:
        with self._session() as session:
            count_expr = sqla_func.count(PublicationRow.publication_id)
            min_count = self._collection_min_count(repository_id)
            subquery = (
                select(PublicationRow.collection_slug, PublicationRow.collection)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.collection_slug.is_not(None))
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
                .having(count_expr >= min_count)
                .subquery()
            )
            return int(session.scalar(select(sqla_func.count()).select_from(subquery)) or 0)

    def list_collection_counts_by_publication_year(
        self,
        year: int,
        repository_id: str = "default",
        *,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        with self._session() as session:
            count_expr = sqla_func.count(PublicationRow.publication_id)
            min_count = self._collection_min_count(repository_id)
            statement = (
                select(PublicationRow.collection_slug, PublicationRow.collection, count_expr)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.collection_slug.is_not(None),
                )
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
                .having(count_expr >= min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationRow.collection.asc())
            else:
                statement = statement.order_by(PublicationRow.collection.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [{"slug": slug, "name": name, "count": int(count)} for slug, name, count in rows if slug and name]

    def list_series_counts(self, repository_id: str = "default") -> list[dict[str, str | int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.series_slug, PublicationRow.series_name, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.repository_id == repository_id, PublicationRow.series_slug.is_not(None))
                .group_by(PublicationRow.series_slug, PublicationRow.series_name)
                .order_by(PublicationRow.series_name.asc())
            )
            rows = session.execute(statement).all()
            return [{"slug": slug, "name": name, "count": count} for slug, name, count in rows if slug and name]

    def list_subject_counts(self, repository_id: str = "default") -> list[dict[str, str | int]]:
        rows = self._subject_facet_rows(repository_id=repository_id, min_count=3, order_by_count_desc=False)
        return [{"slug": slug, "name": name, "count": count} for slug, name, count in rows]

    def list_subject_counts_for_category(
        self,
        category_slug: str,
        repository_id: str = "default",
        *,
        min_count: int = 1,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id))
            statement = (
                select(
                    PublicationSubjectRow.subject_slug,
                    PublicationSubjectRow.subject_name,
                    count_expr,
                )
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .having(count_expr >= effective_min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationSubjectRow.subject_name.asc())
            else:
                statement = statement.order_by(PublicationSubjectRow.subject_name.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [
                {"slug": slug, "name": name, "count": int(count)}
                for slug, name, count in rows
                if isinstance(slug, str) and slug and isinstance(name, str) and name
            ]

    def list_subject_counts_for_category_by_publication_year(
        self,
        category_slug: str,
        year: int,
        repository_id: str = "default",
        *,
        min_count: int = 1,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id))
            statement = (
                select(
                    PublicationSubjectRow.subject_slug,
                    PublicationSubjectRow.subject_name,
                    count_expr,
                )
                .join(
                    PublicationRow,
                    and_(
                        PublicationRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .having(count_expr >= effective_min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationSubjectRow.subject_name.asc())
            else:
                statement = statement.order_by(PublicationSubjectRow.subject_name.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [
                {"slug": slug, "name": name, "count": int(count)}
                for slug, name, count in rows
                if isinstance(slug, str) and slug and isinstance(name, str) and name
            ]

    def list_category_counts(
        self,
        repository_id: str = "default",
        *,
        min_count: int = 3,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(PublicationSubjectCategoryRow.publication_id)
            statement = (
                select(PublicationSubjectCategoryRow.category_slug, PublicationSubjectCategoryRow.category_name, count_expr)
                .where(PublicationSubjectCategoryRow.repository_id == repository_id)
                .group_by(PublicationSubjectCategoryRow.category_slug, PublicationSubjectCategoryRow.category_name)
                .having(count_expr >= effective_min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationSubjectCategoryRow.category_name.asc())
            else:
                statement = statement.order_by(PublicationSubjectCategoryRow.category_name.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [
                {"slug": slug, "name": name, "count": int(count)}
                for slug, name, count in rows
                if isinstance(slug, str) and slug and isinstance(name, str) and name
            ]

    def list_category_counts_by_publication_year(
        self,
        year: int,
        repository_id: str = "default",
        *,
        min_count: int = 3,
        limit: int | None = None,
        offset: int = 0,
        order_by_count_desc: bool = False,
    ) -> list[dict[str, str | int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectCategoryRow.publication_id))
            statement = (
                select(
                    PublicationSubjectCategoryRow.category_slug,
                    PublicationSubjectCategoryRow.category_name,
                    count_expr,
                )
                .join(
                    PublicationRow,
                    and_(
                        PublicationRow.publication_id == PublicationSubjectCategoryRow.publication_id,
                        PublicationRow.repository_id == PublicationSubjectCategoryRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectCategoryRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                )
                .group_by(PublicationSubjectCategoryRow.category_slug, PublicationSubjectCategoryRow.category_name)
                .having(count_expr >= effective_min_count)
            )
            if order_by_count_desc:
                statement = statement.order_by(count_expr.desc(), PublicationSubjectCategoryRow.category_name.asc())
            else:
                statement = statement.order_by(PublicationSubjectCategoryRow.category_name.asc())
            statement = statement.offset(max(offset, 0))
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [
                {"slug": slug, "name": name, "count": int(count)}
                for slug, name, count in rows
                if isinstance(slug, str) and slug and isinstance(name, str) and name
            ]

    def count_category_facets(self, repository_id: str = "default", *, min_count: int = 3) -> int:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(PublicationSubjectCategoryRow.publication_id)
            subquery = (
                select(PublicationSubjectCategoryRow.category_slug, PublicationSubjectCategoryRow.category_name)
                .where(PublicationSubjectCategoryRow.repository_id == repository_id)
                .group_by(PublicationSubjectCategoryRow.category_slug, PublicationSubjectCategoryRow.category_name)
                .having(count_expr >= effective_min_count)
                .subquery()
            )
            return int(session.scalar(select(sqla_func.count()).select_from(subquery)) or 0)

    def count_subject_facets_for_category(
        self,
        category_slug: str,
        repository_id: str = "default",
        *,
        min_count: int = 1,
    ) -> int:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id))
            subquery = (
                select(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .having(count_expr >= effective_min_count)
                .subquery()
            )
            return int(session.scalar(select(sqla_func.count()).select_from(subquery)) or 0)

    def count_subject_facets_for_category_by_publication_year(
        self,
        category_slug: str,
        year: int,
        repository_id: str = "default",
        *,
        min_count: int = 1,
    ) -> int:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id))
            subquery = (
                select(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .join(
                    PublicationRow,
                    and_(
                        PublicationRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
                .having(count_expr >= effective_min_count)
                .subquery()
            )
            return int(session.scalar(select(sqla_func.count()).select_from(subquery)) or 0)

    def get_category(self, category_slug: str, repository_id: str = "default") -> dict[str, str | int] | None:
        for item in self.list_category_counts(repository_id=repository_id):
            if item["slug"] == category_slug:
                return item
        return None

    def subject_statistics(
        self,
        repository_id: str = "default",
        *,
        min_count: int = 3,
        top_limit: int = 25,
    ) -> dict[str, Any]:
        with self._session() as session:
            total_assignments = int(
                session.scalar(
                    select(sqla_func.count())
                    .select_from(PublicationSubjectRow)
                    .where(PublicationSubjectRow.repository_id == repository_id)
                )
                or 0
            )
            distinct_subject_slugs = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.subject_slug))).where(
                        PublicationSubjectRow.repository_id == repository_id
                    )
                )
                or 0
            )
            distinct_subject_labels = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.subject_name))).where(
                        PublicationSubjectRow.repository_id == repository_id
                    )
                )
                or 0
            )
            displayable_facet_count = len(self._subject_facet_rows(repository_id=repository_id, min_count=min_count))
            top_subjects = [
                {"slug": slug, "name": name, "count": count}
                for slug, name, count in self._subject_facet_rows(
                    repository_id=repository_id,
                    min_count=min_count,
                    limit=top_limit,
                    order_by_count_desc=True,
                )
            ]
            return {
                "total_assignments": total_assignments,
                "distinct_subject_slugs": distinct_subject_slugs,
                "distinct_subject_labels": distinct_subject_labels,
                "displayable_facet_count": displayable_facet_count,
                "minimum_facet_count": max(min_count, 1),
                "top_subjects": top_subjects,
            }

    def _subject_bucket_rows(
        self,
        repository_id: str,
        *,
        min_count: int,
    ) -> list[tuple[str, str, int]]:
        effective_min_count = max(min_count, 1)
        with self._session() as session:
            count_expr = sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id))
            name_expr = sqla_func.max(PublicationSubjectRow.subject_name)
            statement = (
                select(
                    PublicationSubjectRow.subject_slug,
                    name_expr.label("subject_name"),
                    count_expr.label("subject_count"),
                )
                .where(PublicationSubjectRow.repository_id == repository_id)
                .group_by(PublicationSubjectRow.subject_slug)
                .having(count_expr >= effective_min_count)
                .order_by(count_expr.desc(), PublicationSubjectRow.subject_slug.asc())
            )
            rows = session.execute(statement).all()
        normalized_rows: list[tuple[str, str, int]] = []
        for slug, name, count in rows:
            if not isinstance(slug, str) or not slug:
                continue
            if isinstance(name, str) and name:
                normalized_name = canonicalize_subject_term(name).strip(" .,!?:;")
            else:
                normalized_name = slug
            if not normalized_name:
                normalized_name = slug
            normalized_rows.append((slug, normalized_name, int(count)))
        return normalized_rows

    def raw_subject_statistics(
        self,
        repository_id: str = "default",
        *,
        min_count: int = 1,
        top_limit: int = 50,
    ) -> dict[str, Any]:
        effective_min_count = max(min_count, 1)
        raw_counter: Counter[str] = Counter()
        canonical_counter: Counter[str] = Counter()
        with self._session() as session:
            statement = select(PublicationRow.subjects_json).where(PublicationRow.repository_id == repository_id)
            for subjects_json in session.scalars(statement):
                try:
                    values = json.loads(subjects_json or "[]")
                except json.JSONDecodeError:
                    values = []
                if not isinstance(values, list):
                    continue
                for value in values:
                    if not isinstance(value, str):
                        continue
                    raw_term = value.strip()
                    if not raw_term:
                        continue
                    raw_counter[raw_term] += 1
                    canonical_term = canonicalize_subject_term(raw_term)
                    if canonical_term:
                        canonical_counter[canonical_term] += 1

        top_raw_subjects = [
            {"name": name, "count": count}
            for name, count in raw_counter.most_common(top_limit)
            if count >= effective_min_count
        ]
        top_canonical_subjects = [
            {"name": name, "count": count}
            for name, count in canonical_counter.most_common(top_limit)
            if count >= effective_min_count
        ]
        return {
            "total_assignments": int(sum(raw_counter.values())),
            "distinct_raw_subjects": len(raw_counter),
            "distinct_canonical_subjects": len(canonical_counter),
            "minimum_count": effective_min_count,
            "top_raw_subjects": top_raw_subjects,
            "top_canonical_subjects": top_canonical_subjects,
        }

    def subject_authority_statistics(
        self,
        repository_id: str = "default",
        *,
        scheme: str = "lcc",
        min_count: int = 1,
        top_limit: int = 50,
    ) -> dict[str, Any]:
        effective_min_count = max(min_count, 1)
        scheme_key = scheme.strip().casefold()
        if scheme_key not in {"lcc", "lcsh", "thema"}:
            raise ValueError("scheme must be one of: lcc, lcsh, thema")

        rows = self._subject_bucket_rows(repository_id, min_count=effective_min_count)

        total_subject_buckets = 0
        mapped_subject_buckets = 0
        total_assignment_count = 0
        mapped_assignment_count = 0
        top_mapped: list[dict[str, Any]] = []
        top_unmapped: list[dict[str, Any]] = []

        for slug, name, count in rows:
            if not isinstance(slug, str) or not slug or not isinstance(name, str) or not name:
                continue
            bucket_count = int(count)
            total_subject_buckets += 1
            total_assignment_count += bucket_count

            if scheme_key == "lcc":
                mappings = resolve_lcc(name)
            elif scheme_key == "lcsh":
                mappings = resolve_lcsh(name)
            else:
                mappings = resolve_thema(name)
            has_mapping = bool(mappings)
            if has_mapping:
                mapped_subject_buckets += 1
                mapped_assignment_count += bucket_count

            entry = {"slug": slug, "name": name, "count": bucket_count}
            if has_mapping:
                entry["mappings"] = mappings if isinstance(mappings, list) else [mappings]
                if len(top_mapped) < top_limit:
                    top_mapped.append(entry)
            else:
                if len(top_unmapped) < top_limit:
                    top_unmapped.append(entry)

        coverage_by_subjects = (
            (mapped_subject_buckets / total_subject_buckets) if total_subject_buckets else 0.0
        )
        coverage_by_assignments = (
            (mapped_assignment_count / total_assignment_count) if total_assignment_count else 0.0
        )

        return {
            "scheme": scheme_key.upper(),
            "minimum_count": effective_min_count,
            "total_subject_buckets": total_subject_buckets,
            "mapped_subject_buckets": mapped_subject_buckets,
            "unmapped_subject_buckets": max(total_subject_buckets - mapped_subject_buckets, 0),
            "total_assignment_count": total_assignment_count,
            "mapped_assignment_count": mapped_assignment_count,
            "coverage_by_subject_buckets": round(coverage_by_subjects, 6),
            "coverage_by_assignments": round(coverage_by_assignments, 6),
            "top_mapped": top_mapped,
            "top_unmapped": top_unmapped,
        }

    def subject_authority_unmapped(
        self,
        repository_id: str = "default",
        *,
        scheme: str = "lcc",
        min_count: int = 1,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        effective_min_count = max(min_count, 1)
        effective_limit = max(min(limit, 2000), 1)
        effective_offset = max(offset, 0)
        scheme_key = scheme.strip().casefold()
        if scheme_key not in {"lcc", "lcsh", "thema"}:
            raise ValueError("scheme must be one of: lcc, lcsh, thema")

        rows = self._subject_bucket_rows(repository_id, min_count=effective_min_count)
        unmapped: list[dict[str, Any]] = []
        skipped = 0
        total_unmapped = 0
        for slug, name, count in rows:
            if not isinstance(slug, str) or not slug or not isinstance(name, str) or not name:
                continue
            if scheme_key == "lcc":
                mappings = resolve_lcc(name)
            elif scheme_key == "lcsh":
                mappings = resolve_lcsh(name)
            else:
                mappings = resolve_thema(name)
            if mappings:
                continue
            total_unmapped += 1
            if skipped < effective_offset:
                skipped += 1
                continue
            if len(unmapped) >= effective_limit:
                continue
            unmapped.append({"slug": slug, "name": name, "count": int(count)})

        return {
            "scheme": scheme_key.upper(),
            "minimum_count": effective_min_count,
            "limit": effective_limit,
            "offset": effective_offset,
            "total_unmapped": total_unmapped,
            "items": unmapped,
        }

    def list_lcc_heading_counts(
        self,
        *,
        repository_id: str = "default",
        min_count: int = 3,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = self._subject_bucket_rows(repository_id, min_count=max(min_count, 1))
        counts: dict[str, dict[str, Any]] = {}
        for _, name, count in rows:
            lcc = resolve_lcc(name)
            if not lcc:
                continue
            code = lcc.get("code")
            term = lcc.get("term")
            if not isinstance(code, str) or not code:
                continue
            if not isinstance(term, str) or not term:
                continue
            top_code = code[0].upper()
            bucket = counts.setdefault(top_code, {"code": top_code, "term": term, "count": 0})
            bucket["count"] = int(bucket["count"]) + int(count)
        items = sorted(counts.values(), key=lambda item: (-int(item["count"]), str(item["term"])))
        normalized_offset = max(offset, 0)
        normalized_limit = max(limit, 1)
        return items[normalized_offset : normalized_offset + normalized_limit]

    def count_lcc_heading_facets(
        self,
        *,
        repository_id: str = "default",
        min_count: int = 3,
    ) -> int:
        rows = self._subject_bucket_rows(repository_id, min_count=max(min_count, 1))
        headings: set[str] = set()
        for _, name, _ in rows:
            lcc = resolve_lcc(name)
            if not lcc:
                continue
            code = lcc.get("code")
            if isinstance(code, str) and code:
                headings.add(code[0].upper())
        return len(headings)

    def _subject_slugs_for_lcc_top_code(self, repository_id: str, top_code: str) -> set[str]:
        normalized_code = (top_code or "").strip().upper()
        if len(normalized_code) != 1 or not normalized_code.isalpha():
            return set()
        with self._session() as session:
            rows = session.execute(
                select(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name).where(
                    PublicationSubjectRow.repository_id == repository_id
                )
            ).all()
        subject_slugs: set[str] = set()
        for subject_slug, subject_name in rows:
            if not isinstance(subject_slug, str) or not subject_slug:
                continue
            if not isinstance(subject_name, str) or not subject_name:
                continue
            lcc = resolve_lcc(subject_name)
            if not isinstance(lcc, dict):
                continue
            code = lcc.get("code")
            if isinstance(code, str) and code and code[0].upper() == normalized_code:
                subject_slugs.add(subject_slug)
        return subject_slugs

    def page_by_lcc_top_code(
        self,
        *,
        top_code: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        subject_slugs = self._subject_slugs_for_lcc_top_code(repository_id, top_code)
        if not subject_slugs:
            return 0, []
        with self._session() as session:
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id)))
                    .select_from(PublicationSubjectRow)
                    .where(
                        PublicationSubjectRow.repository_id == repository_id,
                        PublicationSubjectRow.subject_slug.in_(list(subject_slugs)),
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .join(
                    PublicationSubjectRow,
                    and_(
                        PublicationSubjectRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationSubjectRow.subject_slug.in_(list(subject_slugs)),
                )
                .distinct()
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_subject_slug(
        self,
        subject_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count())
                .select_from(PublicationSubjectRow)
                .where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectRow.subject_slug == subject_slug,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .join(
                    PublicationSubjectRow,
                    and_(
                        PublicationSubjectRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationSubjectRow.subject_slug == subject_slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_subject_slug_for_category(
        self,
        *,
        subject_slug: str,
        category_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id)))
                    .select_from(PublicationSubjectRow)
                    .join(
                        PublicationSubjectCategoryRow,
                        and_(
                            PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                            PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                        ),
                    )
                    .where(
                        PublicationSubjectRow.repository_id == repository_id,
                        PublicationSubjectRow.subject_slug == subject_slug,
                        PublicationSubjectCategoryRow.category_slug == category_slug,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .join(
                    PublicationSubjectRow,
                    and_(
                        PublicationSubjectRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationSubjectRow.subject_slug == subject_slug,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_subject_slug_and_publication_year(
        self,
        subject_slug: str,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id)))
                    .select_from(PublicationSubjectRow)
                    .join(
                        PublicationRow,
                        and_(
                            PublicationRow.publication_id == PublicationSubjectRow.publication_id,
                            PublicationRow.repository_id == PublicationSubjectRow.repository_id,
                        ),
                    )
                    .where(
                        PublicationSubjectRow.repository_id == repository_id,
                        PublicationSubjectRow.subject_slug == subject_slug,
                        PublicationRow.publication_year == year,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .join(
                    PublicationSubjectRow,
                    and_(
                        PublicationSubjectRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationSubjectRow.subject_slug == subject_slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_subject_slug_and_category_and_publication_year(
        self,
        *,
        subject_slug: str,
        category_slug: str,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectRow.publication_id)))
                    .select_from(PublicationSubjectRow)
                    .join(
                        PublicationRow,
                        and_(
                            PublicationRow.publication_id == PublicationSubjectRow.publication_id,
                            PublicationRow.repository_id == PublicationSubjectRow.repository_id,
                        ),
                    )
                    .join(
                        PublicationSubjectCategoryRow,
                        and_(
                            PublicationSubjectCategoryRow.publication_id == PublicationSubjectRow.publication_id,
                            PublicationSubjectCategoryRow.repository_id == PublicationSubjectRow.repository_id,
                        ),
                    )
                    .where(
                        PublicationSubjectRow.repository_id == repository_id,
                        PublicationSubjectRow.subject_slug == subject_slug,
                        PublicationSubjectCategoryRow.category_slug == category_slug,
                        PublicationRow.publication_year == year,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .join(
                    PublicationSubjectRow,
                    and_(
                        PublicationSubjectRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .join(
                    PublicationSubjectCategoryRow,
                    and_(
                        PublicationSubjectCategoryRow.publication_id == PublicationRow.publication_id,
                        PublicationSubjectCategoryRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationSubjectRow.subject_slug == subject_slug,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_category_slug(
        self,
        category_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            matching_publication_ids = (
                select(PublicationSubjectCategoryRow.publication_id)
                .where(
                    PublicationSubjectCategoryRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                )
            )
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectCategoryRow.publication_id)))
                    .where(
                        PublicationSubjectCategoryRow.repository_id == repository_id,
                        PublicationSubjectCategoryRow.category_slug == category_slug,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_id.in_(matching_publication_ids),
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_category_slug_and_publication_year(
        self,
        category_slug: str,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            matching_publication_ids = (
                select(PublicationSubjectCategoryRow.publication_id)
                .join(
                    PublicationRow,
                    and_(
                        PublicationRow.publication_id == PublicationSubjectCategoryRow.publication_id,
                        PublicationRow.repository_id == PublicationSubjectCategoryRow.repository_id,
                    ),
                )
                .where(
                    PublicationSubjectCategoryRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.category_slug == category_slug,
                    PublicationRow.publication_year == year,
                )
            )
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationSubjectCategoryRow.publication_id)))
                    .select_from(PublicationSubjectCategoryRow)
                    .join(
                        PublicationRow,
                        and_(
                            PublicationRow.publication_id == PublicationSubjectCategoryRow.publication_id,
                            PublicationRow.repository_id == PublicationSubjectCategoryRow.repository_id,
                        ),
                    )
                    .where(
                        PublicationSubjectCategoryRow.repository_id == repository_id,
                        PublicationSubjectCategoryRow.category_slug == category_slug,
                        PublicationRow.publication_year == year,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_id.in_(matching_publication_ids),
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def list_language_counts(self, repository_id: str = "default") -> list[dict[str, str | int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.language, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.repository_id == repository_id, PublicationRow.language.is_not(None))
                .group_by(PublicationRow.language)
                .order_by(PublicationRow.language.asc())
            )
            rows = session.execute(statement).all()
            return [{"code": code, "count": count} for code, count in rows if code]

    def list_language_counts_by_publication_year(self, year: int, repository_id: str = "default") -> list[dict[str, str | int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.language, sqla_func.count(PublicationRow.publication_id))
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.language.is_not(None),
                )
                .group_by(PublicationRow.language)
                .order_by(PublicationRow.language.asc())
            )
            rows = session.execute(statement).all()
            return [{"code": code, "count": count} for code, count in rows if code]

    def list_publication_year_counts(self, repository_id: str = "default") -> list[dict[str, int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.publication_year, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.repository_id == repository_id, PublicationRow.publication_year.is_not(None))
                .group_by(PublicationRow.publication_year)
                .order_by(PublicationRow.publication_year.desc())
            )
            rows = session.execute(statement).all()
            return [{"year": int(year), "count": int(count)} for year, count in rows if year is not None]

    def page_by_publication_year(
        self,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.publication_year == year)
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_collection_slug_and_publication_year(
        self,
        collection_slug: str,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.collection_slug == collection_slug,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.collection_slug == collection_slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_language_and_publication_year(
        self,
        language: str,
        year: int,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.language == language,
                )
            ) or 0
            statement = (
                select(PublicationRow)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.publication_year == year,
                    PublicationRow.language == language,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def list_publication_group_counts(self, repository_id: str = "default") -> list[dict[str, str | int]]:
        with self._session() as session:
            rows = session.execute(
                select(PublicationGroupMembershipRow.group_slug, sqla_func.count(PublicationGroupMembershipRow.publication_id))
                .where(PublicationGroupMembershipRow.repository_id == repository_id)
                .group_by(PublicationGroupMembershipRow.group_slug)
            ).all()
        count_by_slug = {str(group_slug): int(count) for group_slug, count in rows if isinstance(group_slug, str)}
        out: list[dict[str, str | int]] = []
        for definition in list_publication_groups():
            out.append(
                {
                    "slug": definition.slug,
                    "title": definition.title,
                    "count": count_by_slug.get(definition.slug, 0),
                }
            )
        return out

    def page_by_publication_group_slug(
        self,
        *,
        group_slug: str,
        page: int,
        page_size: int,
        repository_id: str = "default",
    ) -> tuple[int, list[NormalizedPublication]]:
        definition = publication_group_by_slug(group_slug)
        if definition is None:
            return 0, []
        offset = (page - 1) * page_size
        with self._session() as session:
            total = int(
                session.scalar(
                    select(sqla_func.count(sqla_func.distinct(PublicationGroupMembershipRow.publication_id)))
                    .select_from(PublicationGroupMembershipRow)
                    .where(
                        PublicationGroupMembershipRow.repository_id == repository_id,
                        PublicationGroupMembershipRow.group_slug == definition.slug,
                    )
                )
                or 0
            )
            statement = (
                select(PublicationRow)
                .join(
                    PublicationGroupMembershipRow,
                    and_(
                        PublicationGroupMembershipRow.publication_id == PublicationRow.publication_id,
                        PublicationGroupMembershipRow.repository_id == PublicationRow.repository_id,
                    ),
                )
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationGroupMembershipRow.group_slug == definition.slug,
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def count(self, repository_id: str = "default") -> int:
        with self._session() as session:
            return int(
                session.scalar(
                    select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.repository_id == repository_id)
                )
                or 0
            )

    def clear(self, repository_id: str | None = None) -> None:
        with self._session() as session:
            group_statement = PublicationGroupMembershipRow.__table__.delete()
            category_statement = PublicationSubjectCategoryRow.__table__.delete()
            subject_statement = PublicationSubjectRow.__table__.delete()
            statement = PublicationRow.__table__.delete()
            if repository_id is not None:
                group_statement = group_statement.where(PublicationGroupMembershipRow.repository_id == repository_id)
                category_statement = category_statement.where(PublicationSubjectCategoryRow.repository_id == repository_id)
                subject_statement = subject_statement.where(PublicationSubjectRow.repository_id == repository_id)
                statement = statement.where(PublicationRow.repository_id == repository_id)
            session.execute(group_statement)
            session.execute(category_statement)
            session.execute(subject_statement)
            session.execute(statement)
            session.commit()

    def delete_publications(self, publication_ids: list[str], repository_id: str = "default") -> int:
        if not publication_ids:
            return 0
        storage_ids = [self._storage_publication_id(repository_id, publication_id) for publication_id in publication_ids]
        with self._session() as session:
            session.execute(
                PublicationGroupMembershipRow.__table__.delete().where(
                    PublicationGroupMembershipRow.repository_id == repository_id,
                    PublicationGroupMembershipRow.publication_id.in_(storage_ids),
                )
            )
            session.execute(
                PublicationSubjectCategoryRow.__table__.delete().where(
                    PublicationSubjectCategoryRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.publication_id.in_(storage_ids),
                )
            )
            session.execute(
                PublicationSubjectRow.__table__.delete().where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectRow.publication_id.in_(storage_ids),
                )
            )
            statement = PublicationRow.__table__.delete().where(
                PublicationRow.repository_id == repository_id,
                PublicationRow.publication_id.in_(storage_ids),
            )
            result = session.execute(statement)
            session.commit()
            return int(result.rowcount or 0)

    def list_publication_ids_by_identifier_prefix(
        self,
        prefix: str,
        repository_id: str = "default",
        limit: int = 100,
    ) -> list[str]:
        normalized_prefix = prefix.casefold()
        with self._session() as session:
            statement = (
                select(PublicationRow.source_publication_id)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.identifier.is_not(None),
                    sqla_func.lower(PublicationRow.identifier).like(f"{normalized_prefix}%"),
                )
                .order_by(PublicationRow.source_publication_id.asc(), PublicationRow.publication_id.asc())
                .limit(limit)
            )
            rows = session.scalars(statement).all()
            return [row for row in rows if isinstance(row, str)]

    def count_publications_by_identifier_prefix(self, prefix: str, repository_id: str = "default") -> int:
        normalized_prefix = prefix.casefold()
        with self._session() as session:
            return int(
                session.scalar(
                    select(sqla_func.count())
                    .select_from(PublicationRow)
                    .where(
                        PublicationRow.repository_id == repository_id,
                        PublicationRow.identifier.is_not(None),
                        sqla_func.lower(PublicationRow.identifier).like(f"{normalized_prefix}%"),
                    )
                )
                or 0
            )

    def delete_publications_by_identifier_prefix(self, prefix: str, repository_id: str = "default") -> int:
        normalized_prefix = prefix.casefold()
        with self._session() as session:
            matching_publication_ids = (
                select(PublicationRow.publication_id)
                .where(
                    PublicationRow.repository_id == repository_id,
                    PublicationRow.identifier.is_not(None),
                    sqla_func.lower(PublicationRow.identifier).like(f"{normalized_prefix}%"),
                )
            )
            session.execute(
                PublicationGroupMembershipRow.__table__.delete().where(
                    PublicationGroupMembershipRow.repository_id == repository_id,
                    PublicationGroupMembershipRow.publication_id.in_(matching_publication_ids),
                )
            )
            session.execute(
                PublicationSubjectCategoryRow.__table__.delete().where(
                    PublicationSubjectCategoryRow.repository_id == repository_id,
                    PublicationSubjectCategoryRow.publication_id.in_(matching_publication_ids),
                )
            )
            session.execute(
                PublicationSubjectRow.__table__.delete().where(
                    PublicationSubjectRow.repository_id == repository_id,
                    PublicationSubjectRow.publication_id.in_(matching_publication_ids),
                )
            )
            statement = PublicationRow.__table__.delete().where(
                PublicationRow.repository_id == repository_id,
                PublicationRow.identifier.is_not(None),
                sqla_func.lower(PublicationRow.identifier).like(f"{normalized_prefix}%"),
            )
            result = session.execute(statement)
            session.commit()
            return int(result.rowcount or 0)

    def clear_checkpoints(self, repository_id: str | None = None) -> None:
        with self._session() as session:
            statement = HarvestCheckpointRow.__table__.delete()
            if repository_id is not None:
                statement = statement.where(HarvestCheckpointRow.repository_id == repository_id)
            session.execute(statement)
            session.commit()

    def get_checkpoint(self, checkpoint_key: str, repository_id: str | None = None) -> HarvestCheckpoint | None:
        with self._session() as session:
            if repository_id is None:
                row = session.get(HarvestCheckpointRow, checkpoint_key)
            else:
                statement = select(HarvestCheckpointRow).where(
                    HarvestCheckpointRow.checkpoint_key == checkpoint_key,
                    HarvestCheckpointRow.repository_id == repository_id,
                )
                row = session.scalars(statement).first()
            if row is None:
                return None
            return self._to_checkpoint(row)

    def upsert_checkpoint(
        self,
        checkpoint_key: str,
        base_url: str,
        metadata_prefix: str,
        set_name: str | None,
        last_from_date: str | None,
        last_until_date: str | None,
        repository_id: str = "default",
        source_type: str = "oai-pmh",
        state: dict[str, Any] | None = None,
    ) -> None:
        with self._session() as session:
            existing = session.get(HarvestCheckpointRow, checkpoint_key)
            state_json = json.dumps(state, ensure_ascii=True) if state is not None else None
            if existing is None:
                existing = HarvestCheckpointRow(
                    checkpoint_key=checkpoint_key,
                    repository_id=repository_id,
                    source_type=source_type,
                    base_url=base_url,
                    metadata_prefix=metadata_prefix,
                    set_name=set_name,
                    last_from_date=last_from_date,
                    last_until_date=last_until_date,
                    state_json=state_json,
                    updated_at=datetime.now(UTC),
                )
                session.add(existing)
            else:
                existing.repository_id = repository_id
                existing.source_type = source_type
                existing.base_url = base_url
                existing.metadata_prefix = metadata_prefix
                existing.set_name = set_name
                existing.last_from_date = last_from_date
                existing.last_until_date = last_until_date
                existing.state_json = state_json
                existing.updated_at = datetime.now(UTC)
            session.commit()

    def list_checkpoints(
        self,
        repository_id: str | None = None,
        source_type: str | None = None,
    ) -> list[HarvestCheckpoint]:
        with self._session() as session:
            statement = select(HarvestCheckpointRow)
            if repository_id is not None:
                statement = statement.where(HarvestCheckpointRow.repository_id == repository_id)
            if source_type is not None:
                statement = statement.where(HarvestCheckpointRow.source_type == source_type)
            rows = session.scalars(statement.order_by(HarvestCheckpointRow.updated_at.desc())).all()
            return [self._to_checkpoint(row) for row in rows]

    @staticmethod
    def _to_repository(row: RepositoryRow) -> RepositoryConfig:
        return RepositoryConfig(
            repository_id=row.repository_id,
            source_type=row.source_type,
            name=row.name,
            config=json.loads(row.config_json or "{}"),
            is_active=bool(row.is_active),
            updated_at=row.updated_at.isoformat(),
            created_at=row.created_at.isoformat(),
        )

    @staticmethod
    def _to_checkpoint(row: HarvestCheckpointRow) -> HarvestCheckpoint:
        return HarvestCheckpoint(
            checkpoint_key=row.checkpoint_key,
            repository_id=row.repository_id,
            source_type=row.source_type,
            base_url=row.base_url,
            metadata_prefix=row.metadata_prefix,
            set_name=row.set_name,
            last_from_date=row.last_from_date,
            last_until_date=row.last_until_date,
            state=json.loads(row.state_json) if row.state_json else None,
            updated_at=row.updated_at.isoformat(),
        )

    @staticmethod
    def _to_publication(row: PublicationRow) -> NormalizedPublication:
        source_publication_id = row.source_publication_id or row.publication_id
        return NormalizedPublication(
            publication_id=source_publication_id,
            repository_id=row.repository_id,
            source_publication_id=source_publication_id,
            title=row.title,
            authors=json.loads(row.authors_json or "[]"),
            authors_enriched=json.loads(row.authors_enriched_json or "[]"),
            language=row.language,
            publisher=row.publisher,
            published=row.published,
            identifier=row.identifier,
            subjects=json.loads(row.subjects_json or "[]"),
            subject_authorities=json.loads(row.subject_authorities_json or "[]"),
            links=json.loads(row.links_json or "[]"),
            source=row.source,
            collection=row.collection,
            collection_slug=row.collection_slug,
            series_name=row.series_name,
            series_slug=row.series_slug,
            series_position=row.series_position,
            publisher_slug=row.publisher_slug,
            publication_year=row.publication_year,
            raw=json.loads(row.raw_json or "{}"),
        )
