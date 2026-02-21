"""add publisher slug column

Revision ID: 0003_publisher_slug
Revises: 0002_collection_series
Create Date: 2026-02-20 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_publisher_slug"
down_revision = "0002_collection_series"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("publications", sa.Column("publisher_slug", sa.String(length=512), nullable=True))


def downgrade() -> None:
    op.drop_column("publications", "publisher_slug")
