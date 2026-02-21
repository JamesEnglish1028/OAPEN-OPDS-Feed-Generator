from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import DateTime, String, Text, create_engine, func as sqla_func, select
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
class HarvestCheckpoint:
    checkpoint_key: str
    base_url: str
    metadata_prefix: str
    set_name: str | None
    last_from_date: str | None
    last_until_date: str | None
    updated_at: str


class Base(DeclarativeBase):
    pass


class PublicationRow(Base):
    __tablename__ = "publications"

    publication_id: Mapped[str] = mapped_column(String(512), primary_key=True)
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
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    metadata_prefix: Mapped[str] = mapped_column(String(128), nullable=False)
    set_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_from_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_until_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class PublicationStore:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._engine = create_engine(database_url, future=True)
        self._is_postgres = database_url.startswith("postgresql")

    def initialize(self) -> None:
        Base.metadata.create_all(self._engine)

    def _session(self) -> Session:
        return Session(self._engine)

    def upsert(self, pub: NormalizedPublication) -> None:
        payload = {
            "publication_id": pub.publication_id,
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
                existing = session.get(PublicationRow, pub.publication_id)
                if existing is None:
                    session.add(PublicationRow(**payload))
                else:
                    for key, value in payload.items():
                        setattr(existing, key, value)
            session.commit()

    def get(self, publication_id: str) -> NormalizedPublication | None:
        with self._session() as session:
            row = session.get(PublicationRow, publication_id)
            return self._to_publication(row) if row is not None else None

    def all(self) -> list[NormalizedPublication]:
        with self._session() as session:
            statement = select(PublicationRow).order_by(PublicationRow.publication_id.asc())
            rows = session.scalars(statement).all()
            return [self._to_publication(row) for row in rows]

    def page(self, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(select(sqla_func.count()).select_from(PublicationRow)) or 0
            statement = select(PublicationRow).order_by(PublicationRow.publication_id.asc()).offset(offset).limit(page_size)
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_collection_slug(self, collection_slug: str, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.collection_slug == collection_slug)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.collection_slug == collection_slug)
                .order_by(PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_language(self, language: str, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.language == language)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.language == language)
                .order_by(PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_series_slug(self, series_slug: str, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.series_slug == series_slug)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.series_slug == series_slug)
                .order_by(PublicationRow.series_position.asc().nullslast(), PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def page_by_publisher_slug(self, publisher_slug: str, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.publisher_slug == publisher_slug)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.publisher_slug == publisher_slug)
                .order_by(PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def list_collection_counts(self) -> list[dict[str, str | int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.collection_slug, PublicationRow.collection, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.collection_slug.is_not(None))
                .group_by(PublicationRow.collection_slug, PublicationRow.collection)
                .order_by(PublicationRow.collection.asc())
            )
            rows = session.execute(statement).all()
            return [{"slug": slug, "name": name, "count": count} for slug, name, count in rows if slug and name]

    def list_language_counts(self) -> list[dict[str, str | int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.language, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.language.is_not(None))
                .group_by(PublicationRow.language)
                .order_by(PublicationRow.language.asc())
            )
            rows = session.execute(statement).all()
            return [{"code": code, "count": count} for code, count in rows if code]

    def list_publication_year_counts(self) -> list[dict[str, int]]:
        with self._session() as session:
            statement = (
                select(PublicationRow.publication_year, sqla_func.count(PublicationRow.publication_id))
                .where(PublicationRow.publication_year.is_not(None))
                .group_by(PublicationRow.publication_year)
                .order_by(PublicationRow.publication_year.desc())
            )
            rows = session.execute(statement).all()
            return [{"year": int(year), "count": int(count)} for year, count in rows if year is not None]

    def page_by_publication_year(self, year: int, page: int, page_size: int) -> tuple[int, list[NormalizedPublication]]:
        offset = (page - 1) * page_size
        with self._session() as session:
            total = session.scalar(
                select(sqla_func.count()).select_from(PublicationRow).where(PublicationRow.publication_year == year)
            ) or 0
            statement = (
                select(PublicationRow)
                .where(PublicationRow.publication_year == year)
                .order_by(PublicationRow.publication_id.asc())
                .offset(offset)
                .limit(page_size)
            )
            rows = session.scalars(statement).all()
            return total, [self._to_publication(row) for row in rows]

    def count(self) -> int:
        with self._session() as session:
            return session.query(PublicationRow).count()

    def clear(self) -> None:
        with self._session() as session:
            session.query(PublicationRow).delete()
            session.commit()

    def clear_checkpoints(self) -> None:
        with self._session() as session:
            session.query(HarvestCheckpointRow).delete()
            session.commit()

    def get_checkpoint(self, checkpoint_key: str) -> HarvestCheckpoint | None:
        with self._session() as session:
            row = session.get(HarvestCheckpointRow, checkpoint_key)
            if row is None:
                return None
            return HarvestCheckpoint(
                checkpoint_key=row.checkpoint_key,
                base_url=row.base_url,
                metadata_prefix=row.metadata_prefix,
                set_name=row.set_name,
                last_from_date=row.last_from_date,
                last_until_date=row.last_until_date,
                updated_at=row.updated_at.isoformat(),
            )

    def upsert_checkpoint(
        self,
        checkpoint_key: str,
        base_url: str,
        metadata_prefix: str,
        set_name: str | None,
        last_from_date: str | None,
        last_until_date: str | None,
    ) -> None:
        with self._session() as session:
            existing = session.get(HarvestCheckpointRow, checkpoint_key)
            if existing is None:
                existing = HarvestCheckpointRow(
                    checkpoint_key=checkpoint_key,
                    base_url=base_url,
                    metadata_prefix=metadata_prefix,
                    set_name=set_name,
                    last_from_date=last_from_date,
                    last_until_date=last_until_date,
                    updated_at=datetime.now(UTC),
                )
                session.add(existing)
            else:
                existing.base_url = base_url
                existing.metadata_prefix = metadata_prefix
                existing.set_name = set_name
                existing.last_from_date = last_from_date
                existing.last_until_date = last_until_date
                existing.updated_at = datetime.now(UTC)
            session.commit()

    def list_checkpoints(self) -> list[HarvestCheckpoint]:
        with self._session() as session:
            rows = session.scalars(select(HarvestCheckpointRow).order_by(HarvestCheckpointRow.updated_at.desc())).all()
            return [
                HarvestCheckpoint(
                    checkpoint_key=row.checkpoint_key,
                    base_url=row.base_url,
                    metadata_prefix=row.metadata_prefix,
                    set_name=row.set_name,
                    last_from_date=row.last_from_date,
                    last_until_date=row.last_until_date,
                    updated_at=row.updated_at.isoformat(),
                )
                for row in rows
            ]

    @staticmethod
    def _to_publication(row: PublicationRow) -> NormalizedPublication:
        return NormalizedPublication(
            publication_id=row.publication_id,
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
