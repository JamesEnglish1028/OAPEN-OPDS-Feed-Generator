"""add publication group memberships table

Revision ID: 0010_publication_group_memberships
Revises: 0009_publication_authors_enriched_json
Create Date: 2026-03-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0010_publication_group_memberships"
down_revision = "0009_publication_authors_enriched_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "publication_group_memberships",
        sa.Column("publication_id", sa.String(length=512), nullable=False),
        sa.Column("repository_id", sa.String(length=128), nullable=False),
        sa.Column("group_slug", sa.String(length=128), nullable=False),
        sa.PrimaryKeyConstraint("publication_id", "repository_id", "group_slug"),
    )
    op.create_index(
        "ix_publication_group_memberships_repository_group",
        "publication_group_memberships",
        ["repository_id", "group_slug"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_publication_group_memberships_repository_group", table_name="publication_group_memberships")
    op.drop_table("publication_group_memberships")
