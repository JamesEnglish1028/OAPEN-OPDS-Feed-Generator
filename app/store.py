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

    def upsert(self, pub: NormalizedPublication) -> None:
        repository_id = pub.repository_id or "default"
        source_publication_id = pub.source_publication_id or pub.publication_id
        storage_publication_id = self._storage_publication_id(repository_id, source_publication_id)
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
            session.commit()

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
            statement = (
                select(PublicationRow.collection_slug, PublicationRow.collection, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.repository_id == repository_id, PublicationRow.collection_slug.is_not(None))
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
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
            statement = PublicationRow.__table__.delete()
            if repository_id is not None:
                statement = statement.where(PublicationRow.repository_id == repository_id)
            session.execute(statement)
            session.commit()

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
