from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, String, Text, and_, create_engine, func as sqla_func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.sql import func

from app.models import NormalizedPublication


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
    return slug or None


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
    language: Mapped[str | None] = mapped_column(String(64), nullable=True)
    publisher: Mapped[str | None] = mapped_column(String(512), nullable=True)
    published: Mapped[str | None] = mapped_column(String(64), nullable=True)
    identifier: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    subjects_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
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

    def initialize(self) -> None:
        Base.metadata.create_all(self._engine)

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
            variant_counts = (
                select(
                    PublicationSubjectRow.subject_slug.label("subject_slug"),
                    PublicationSubjectRow.subject_name.label("subject_name"),
                    sqla_func.count(PublicationSubjectRow.publication_id).label("variant_count"),
                )
                .where(PublicationSubjectRow.repository_id == repository_id)
                .group_by(PublicationSubjectRow.subject_slug, PublicationSubjectRow.subject_name)
            ).subquery()

            ranked = (
                select(
                    variant_counts.c.subject_slug,
                    variant_counts.c.subject_name,
                    sqla_func.sum(variant_counts.c.variant_count)
                    .over(partition_by=variant_counts.c.subject_slug)
                    .label("total_count"),
                    sa.func.row_number()
                    .over(
                        partition_by=variant_counts.c.subject_slug,
                        order_by=(
                            variant_counts.c.variant_count.desc(),
                            sqla_func.length(variant_counts.c.subject_name).asc(),
                            sqla_func.lower(variant_counts.c.subject_name).asc(),
                        ),
                    )
                    .label("variant_rank"),
                )
            ).subquery()

            statement = (
                select(ranked.c.subject_slug, ranked.c.subject_name, ranked.c.total_count)
                .where(
                    ranked.c.variant_rank == 1,
                    ranked.c.total_count >= effective_min_count,
                )
            )
            if order_by_count_desc:
                statement = statement.order_by(ranked.c.total_count.desc(), ranked.c.subject_name.asc())
            else:
                statement = statement.order_by(ranked.c.subject_name.asc())
            if limit is not None:
                statement = statement.limit(max(limit, 1))
            rows = session.execute(statement).all()
            return [
                (slug, name, int(count))
                for slug, name, count in rows
                if isinstance(slug, str) and slug and isinstance(name, str) and name
            ]

    @staticmethod
    def _normalized_subject_rows(subjects: list[Any]) -> list[dict[str, str]]:
        normalized_subjects = []
        seen_subjects: set[tuple[str, str]] = set()
        for subject in subjects:
            if not isinstance(subject, str):
                continue
            subject_name = subject.strip()
            subject_slug = _slugify_value(subject_name)
            if not subject_name or not subject_slug:
                continue
            key = (subject_slug, subject_name)
            if key in seen_subjects:
                continue
            seen_subjects.add(key)
            normalized_subjects.append({"subject_slug": subject_slug, "subject_name": subject_name})
        return normalized_subjects

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
        payload = {
            "publication_id": storage_publication_id,
            "repository_id": repository_id,
            "source_publication_id": source_publication_id,
            "title": pub.title,
            "authors_json": json.dumps(pub.authors, ensure_ascii=True),
            "language": pub.language,
            "publisher": pub.publisher,
            "published": pub.published,
            "identifier": pub.identifier,
            "subjects_json": json.dumps(pub.subjects, ensure_ascii=True),
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
            session.commit()

    def backfill_publication_subjects(
        self,
        repository_id: str = "default",
        *,
        batch_size: int = 500,
        start_after: str | None = None,
    ) -> SubjectBackfillResult:
        limit = max(1, min(batch_size, 5000))
        with self._session() as session:
            statement = (
                select(PublicationRow.publication_id, PublicationRow.subjects_json)
                .where(PublicationRow.repository_id == repository_id)
                .order_by(PublicationRow.publication_id.asc())
            )
            if start_after:
                statement = statement.where(PublicationRow.publication_id > start_after)
            rows = session.execute(statement.limit(limit + 1)).all()
            has_more = len(rows) > limit
            work_rows = rows[:limit]
            processed_publications = 0
            indexed_subject_rows = 0
            next_cursor = None
            for publication_id, subjects_json in work_rows:
                try:
                    raw_subjects = json.loads(subjects_json or "[]")
                except json.JSONDecodeError:
                    raw_subjects = []
                subjects = raw_subjects if isinstance(raw_subjects, list) else []
                normalized_subjects = self._normalized_subject_rows(subjects)
                self._replace_publication_subjects(
                    session,
                    repository_id=repository_id,
                    publication_id=publication_id,
                    normalized_subjects=normalized_subjects,
                )
                processed_publications += 1
                indexed_subject_rows += len(normalized_subjects)
                next_cursor = publication_id
            session.commit()
            return SubjectBackfillResult(
                processed_publications=processed_publications,
                indexed_subject_rows=indexed_subject_rows,
                next_cursor=next_cursor,
                has_more=has_more,
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
        with self._session() as session:
            count_expr = sqla_func.count(PublicationRow.publication_id)
            statement = (
                select(PublicationRow.collection_slug, PublicationRow.collection, count_expr)
                .where(PublicationRow.repository_id == repository_id, PublicationRow.collection_slug.is_not(None))
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
                .having(count_expr >= 2)
                .order_by(PublicationRow.collection.asc())
            )
            rows = session.execute(statement).all()
            return [{"slug": slug, "name": name, "count": count} for slug, name, count in rows if slug and name]

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
            subject_statement = PublicationSubjectRow.__table__.delete()
            statement = PublicationRow.__table__.delete()
            if repository_id is not None:
                subject_statement = subject_statement.where(PublicationSubjectRow.repository_id == repository_id)
                statement = statement.where(PublicationRow.repository_id == repository_id)
            session.execute(subject_statement)
            session.execute(statement)
            session.commit()

    def delete_publications(self, publication_ids: list[str], repository_id: str = "default") -> int:
        if not publication_ids:
            return 0
        storage_ids = [self._storage_publication_id(repository_id, publication_id) for publication_id in publication_ids]
        with self._session() as session:
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
            language=row.language,
            publisher=row.publisher,
            published=row.published,
            identifier=row.identifier,
            subjects=json.loads(row.subjects_json or "[]"),
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
