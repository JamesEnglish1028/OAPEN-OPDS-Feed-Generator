"""add publication subject authorities json column

Revision ID: 0008_publication_subject_authorities_json
Revises: 0007_publication_subject_categories
Create Date: 2026-03-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0008_publication_subject_authorities_json"
down_revision = "0007_publication_subject_categories"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "publications",
        sa.Column(
            "subject_authorities_json",
            sa.Text(),
            nullable=False,
            server_default="[]",
        ),
    )
    op.alter_column("publications", "subject_authorities_json", server_default=None)


def downgrade() -> None:
    op.drop_column("publications", "subject_authorities_json")

