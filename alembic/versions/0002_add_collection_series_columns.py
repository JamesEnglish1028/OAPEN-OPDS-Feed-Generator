"""add collection and series columns

Revision ID: 0002_collection_series
Revises: 0001_initial
Create Date: 2026-02-20 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_collection_series"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("publications", sa.Column("collection", sa.String(length=512), nullable=True))
    op.add_column("publications", sa.Column("collection_slug", sa.String(length=512), nullable=True))
    op.add_column("publications", sa.Column("series_name", sa.String(length=512), nullable=True))
    op.add_column("publications", sa.Column("series_slug", sa.String(length=512), nullable=True))
    op.add_column("publications", sa.Column("series_position", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("publications", "series_position")
    op.drop_column("publications", "series_slug")
    op.drop_column("publications", "series_name")
    op.drop_column("publications", "collection_slug")
    op.drop_column("publications", "collection")
