"""add publication subjects index

Revision ID: 0006_publication_subjects
Revises: 0005_repository_support
Create Date: 2026-03-03 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0006_publication_subjects"
down_revision = "0005_repository_support"
branch_labels = None
depends_on = None
def upgrade() -> None:
    op.create_table(
        "publication_subjects",
        sa.Column("publication_id", sa.String(length=512), nullable=False),
        sa.Column("repository_id", sa.String(length=128), nullable=False),
        sa.Column("subject_slug", sa.String(length=512), nullable=False),
        sa.Column("subject_name", sa.String(length=512), nullable=False),
        sa.PrimaryKeyConstraint("publication_id", "repository_id", "subject_slug"),
    )
    op.create_index("ix_publication_subjects_repository_slug", "publication_subjects", ["repository_id", "subject_slug"])


def downgrade() -> None:
    op.drop_index("ix_publication_subjects_repository_slug", table_name="publication_subjects")
    op.drop_table("publication_subjects")
