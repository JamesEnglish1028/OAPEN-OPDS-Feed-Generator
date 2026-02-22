"""add repository support

Revision ID: 0005_repository_support
Revises: 0004_publication_year
Create Date: 2026-02-22 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_repository_support"
down_revision = "0004_publication_year"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "repositories",
        sa.Column("repository_id", sa.String(length=128), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("config_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("repository_id"),
    )

    op.add_column("publications", sa.Column("repository_id", sa.String(length=128), nullable=True))
    op.add_column("publications", sa.Column("source_publication_id", sa.String(length=512), nullable=True))

    op.execute("UPDATE publications SET repository_id='default' WHERE repository_id IS NULL")
    op.execute("UPDATE publications SET source_publication_id=publication_id WHERE source_publication_id IS NULL")

    op.alter_column("publications", "repository_id", nullable=False)

    op.create_index("ix_publications_repository_id", "publications", ["repository_id"])
    op.create_index("ix_publications_repository_source_id", "publications", ["repository_id", "source_publication_id"])

    op.add_column("harvest_checkpoints", sa.Column("repository_id", sa.String(length=128), nullable=True))
    op.add_column("harvest_checkpoints", sa.Column("source_type", sa.String(length=64), nullable=True))
    op.add_column("harvest_checkpoints", sa.Column("state_json", sa.Text(), nullable=True))

    op.execute("UPDATE harvest_checkpoints SET repository_id='default' WHERE repository_id IS NULL")
    op.execute("UPDATE harvest_checkpoints SET source_type='oai-pmh' WHERE source_type IS NULL")

    op.alter_column("harvest_checkpoints", "repository_id", nullable=False)
    op.alter_column("harvest_checkpoints", "source_type", nullable=False)

    op.create_index("ix_harvest_checkpoints_repository_id", "harvest_checkpoints", ["repository_id"])
    op.create_index("ix_harvest_checkpoints_repository_source", "harvest_checkpoints", ["repository_id", "source_type"])


def downgrade() -> None:
    op.drop_index("ix_harvest_checkpoints_repository_source", table_name="harvest_checkpoints")
    op.drop_index("ix_harvest_checkpoints_repository_id", table_name="harvest_checkpoints")
    op.drop_column("harvest_checkpoints", "state_json")
    op.drop_column("harvest_checkpoints", "source_type")
    op.drop_column("harvest_checkpoints", "repository_id")

    op.drop_index("ix_publications_repository_source_id", table_name="publications")
    op.drop_index("ix_publications_repository_id", table_name="publications")
    op.drop_column("publications", "source_publication_id")
    op.drop_column("publications", "repository_id")

    op.drop_table("repositories")
