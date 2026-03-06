"""add publication authors enriched json column

Revision ID: 0009_publication_authors_enriched_json
Revises: 0008_publication_subject_authorities_json
Create Date: 2026-03-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0009_publication_authors_enriched_json"
down_revision = "0008_publication_subject_authorities_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "publications",
        sa.Column(
            "authors_enriched_json",
            sa.Text(),
            nullable=False,
            server_default="[]",
        ),
    )
    op.alter_column("publications", "authors_enriched_json", server_default=None)


def downgrade() -> None:
    op.drop_column("publications", "authors_enriched_json")
