"""add publication year column

Revision ID: 0004_publication_year
Revises: 0003_publisher_slug
Create Date: 2026-02-20 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_publication_year"
down_revision = "0003_publisher_slug"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("publications", sa.Column("publication_year", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("publications", "publication_year")
