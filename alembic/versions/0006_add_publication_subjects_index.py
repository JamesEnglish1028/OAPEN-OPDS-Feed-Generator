"""add publication subjects index

Revision ID: 0006_publication_subjects
Revises: 0005_repository_support
Create Date: 2026-03-03 00:00:00.000000
"""
from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa


revision = "0006_publication_subjects"
down_revision = "0005_repository_support"
branch_labels = None
depends_on = None


def _slugify(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().lower()
    if not text:
        return None
    slug_chars = [char if char.isalnum() else "-" for char in text]
    slug = "".join(slug_chars).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or None


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

    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT publication_id, repository_id, subjects_json FROM publications")).fetchall()
    subject_rows = []
    seen = set()
    for publication_id, repository_id, subjects_json in rows:
        try:
            subjects = json.loads(subjects_json or "[]")
        except json.JSONDecodeError:
            subjects = []
        if not isinstance(subjects, list):
            continue
        for subject in subjects:
            if not isinstance(subject, str):
                continue
            subject_name = subject.strip()
            subject_slug = _slugify(subject_name)
            if not subject_name or not subject_slug:
                continue
            key = (publication_id, repository_id, subject_slug)
            if key in seen:
                continue
            seen.add(key)
            subject_rows.append(
                {
                    "publication_id": publication_id,
                    "repository_id": repository_id,
                    "subject_slug": subject_slug,
                    "subject_name": subject_name,
                }
            )

    if subject_rows:
        op.bulk_insert(
            sa.table(
                "publication_subjects",
                sa.column("publication_id", sa.String()),
                sa.column("repository_id", sa.String()),
                sa.column("subject_slug", sa.String()),
                sa.column("subject_name", sa.String()),
            ),
            subject_rows,
        )


def downgrade() -> None:
    op.drop_index("ix_publication_subjects_repository_slug", table_name="publication_subjects")
    op.drop_table("publication_subjects")
