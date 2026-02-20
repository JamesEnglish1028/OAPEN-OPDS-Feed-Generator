"""initial schema

Revision ID: 0001_initial
Revises: 
Create Date: 2026-02-20 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "publications",
        sa.Column("publication_id", sa.String(length=512), nullable=False),
        sa.Column("title", sa.String(length=2000), nullable=False),
        sa.Column("authors_json", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=64), nullable=True),
        sa.Column("publisher", sa.String(length=512), nullable=True),
        sa.Column("published", sa.String(length=64), nullable=True),
        sa.Column("identifier", sa.String(length=1024), nullable=True),
        sa.Column("subjects_json", sa.Text(), nullable=False),
        sa.Column("links_json", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("publication_id"),
    )
    op.create_table(
        "harvest_checkpoints",
        sa.Column("checkpoint_key", sa.String(length=1024), nullable=False),
        sa.Column("base_url", sa.String(length=2048), nullable=False),
        sa.Column("metadata_prefix", sa.String(length=128), nullable=False),
        sa.Column("set_name", sa.String(length=256), nullable=True),
        sa.Column("last_from_date", sa.String(length=64), nullable=True),
        sa.Column("last_until_date", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("checkpoint_key"),
    )


def downgrade() -> None:
    op.drop_table("harvest_checkpoints")
    op.drop_table("publications")
