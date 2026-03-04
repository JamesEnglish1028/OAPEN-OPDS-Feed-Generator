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
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("publication_subject_categories"):
        op.create_table(
            "publication_subject_categories",
            sa.Column("publication_id", sa.String(length=512), nullable=False),
            sa.Column("repository_id", sa.String(length=128), nullable=False),
            sa.Column("category_slug", sa.String(length=512), nullable=False),
            sa.Column("category_name", sa.String(length=512), nullable=False),
            sa.PrimaryKeyConstraint("publication_id", "repository_id", "category_slug"),
        )
        inspector = sa.inspect(bind)

    existing_indexes = {item["name"] for item in inspector.get_indexes("publication_subject_categories")}
    if "ix_publication_subject_categories_repository_slug" not in existing_indexes:
        op.create_index(
            "ix_publication_subject_categories_repository_slug",
            "publication_subject_categories",
            ["repository_id", "category_slug"],
        )


def downgrade() -> None:
    op.drop_index("ix_publication_subject_categories_repository_slug", table_name="publication_subject_categories")
    op.drop_table("publication_subject_categories")
