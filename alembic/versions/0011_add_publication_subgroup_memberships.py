"""add publication subgroup memberships table

Revision ID: 0011_publication_subgroup_memberships
Revises: 0010_publication_group_memberships
Create Date: 2026-03-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0011_publication_subgroup_memberships"
down_revision = "0010_publication_group_memberships"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "publication_subgroup_memberships",
        sa.Column("publication_id", sa.String(length=512), nullable=False),
        sa.Column("repository_id", sa.String(length=128), nullable=False),
        sa.Column("group_slug", sa.String(length=128), nullable=False),
        sa.Column("subgroup_slug", sa.String(length=256), nullable=False),
        sa.PrimaryKeyConstraint("publication_id", "repository_id", "group_slug", "subgroup_slug"),
    )
    op.create_index(
        "ix_publication_subgroup_memberships_repository_group_subgroup",
        "publication_subgroup_memberships",
        ["repository_id", "group_slug", "subgroup_slug"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_publication_subgroup_memberships_repository_group_subgroup",
        table_name="publication_subgroup_memberships",
    )
    op.drop_table("publication_subgroup_memberships")
