"""add publication subject categories

Revision ID: 0007_publication_subject_categories
Revises: 0006_publication_subjects
Create Date: 2026-03-04 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_publication_subject_categories"
down_revision = "0006_publication_subjects"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # This revision is intentionally schema-light.
    # The application creates the category table and index with SQLAlchemy
    # metadata `checkfirst=True` during startup, which is safer on Render
    # than running extra DDL inside the startup migration path.
    return None


def downgrade() -> None:
    op.drop_index("ix_publication_subject_categories_repository_slug", table_name="publication_subject_categories")
    op.drop_table("publication_subject_categories")
