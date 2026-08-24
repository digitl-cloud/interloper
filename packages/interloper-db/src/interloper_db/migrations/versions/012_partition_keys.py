"""Runs and backfills are scoped by partition keys, not dates.

``runs.partition_date`` (DATE) becomes ``partition_key`` (VARCHAR), and
``backfills.start_date`` / ``end_date`` become ``start_key`` / ``end_key``.
A key's shape carries its granularity (``2026-08-21``, ``2026-08``, ``2026``,
``2026-08-21T13``), which is what lets non-daily assets be scheduled without
a separate granularity column; keys of one granularity sort chronologically
as strings, so the newest-first dispatch ordering is preserved.

Existing DATE values render as ISO, which is already the daily key.
Idempotent: a no-op once the columns are renamed. The downgrade casts keys
back to DATE and therefore only works while every stored key is daily — true
by construction for data written before this migration.

Revision ID: 012
Revises: 011
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "012"
down_revision: str | None = "011"
branch_labels: str | None = None
depends_on: str | None = None

_RENAMES = (
    ("runs", "partition_date", "partition_key"),
    ("backfills", "start_date", "start_key"),
    ("backfills", "end_date", "end_key"),
)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table, old, new in _RENAMES:
        if not inspector.has_table(table):
            continue
        columns = {column["name"] for column in inspector.get_columns(table)}
        if new in columns or old not in columns:
            continue
        op.alter_column(table, old, new_column_name=new)
        op.alter_column(
            table,
            new,
            type_=sa.String(),
            postgresql_using=f"to_char({new}, 'YYYY-MM-DD')",
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table, old, new in _RENAMES:
        if not inspector.has_table(table):
            continue
        columns = {column["name"] for column in inspector.get_columns(table)}
        if old in columns or new not in columns:
            continue
        op.alter_column(table, new, type_=sa.Date(), postgresql_using=f"{new}::date")
        op.alter_column(table, new, new_column_name=old)
