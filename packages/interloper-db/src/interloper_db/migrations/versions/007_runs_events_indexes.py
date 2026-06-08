"""Ensure the declared composite indexes exist on ``runs`` and ``events``.

These indexes are declared on the SQLModel models (``__table_args__``) but were
only ever materialised by ``SQLModel.metadata.create_all()`` when the tables
were first created. ``create_all`` never ALTERs an existing table, so any
database whose ``runs``/``events`` tables predate these declarations is missing
them. The practical fallout:

- ``GET /runs`` filters on ``org_id`` and sorts by ``created_at DESC`` and also
  runs a ``count(*)`` — without ``ix_runs_org_id_created_at`` every request is a
  full sequential scan + sort of the whole table, which times out (504) as the
  table grows.
- The run-events endpoint pages on ``(run_id, timestamp)`` and relies on
  ``ix_events_run_id_timestamp``.

Create them here so the fix reaches existing production tables. ``CONCURRENTLY``
keeps reads and writes flowing during the build, and ``IF NOT EXISTS`` makes the
migration a no-op on databases that already have them (e.g. freshly provisioned
ones where ``create_all`` did create them).

Revision ID: 007
Revises: 006
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "007"
down_revision: str | None = "006"
branch_labels: str | None = None
depends_on: str | None = None

# (index name, table, column list) — kept in sync with the models' __table_args__.
_INDEXES: list[tuple[str, str, str]] = [
    ("ix_runs_org_id_created_at", "runs", "(org_id, created_at)"),
    ("ix_runs_backfill_id_status", "runs", "(backfill_id, status)"),
    ("ix_events_run_id_timestamp", "events", "(run_id, timestamp)"),
    ("ix_events_asset_lookup", "events", "(run_id, asset_key, event_type, timestamp)"),
]


def upgrade() -> None:
    """Create the declared indexes if they're missing, without locking the tables."""
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so step
    # outside Alembic's surrounding transaction for these statements.
    with op.get_context().autocommit_block():
        for name, table, cols in _INDEXES:
            op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {table} {cols}")


def downgrade() -> None:
    """Drop the indexes created by this migration."""
    with op.get_context().autocommit_block():
        for name, _table, _cols in _INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
