"""Index error events for windowed error scans.

``EventStore.error_groups`` reads an organisation's error events over a time
window. Error events are a small fraction of ``events``, so a partial index on
``(org_id, timestamp)`` keeps those scans off the whole table. The index is
built ``CONCURRENTLY`` so a busy ``events`` table keeps taking writes, and
``IF NOT EXISTS`` because ``create_all`` already builds it on a fresh database.

Revision ID: 005
Revises: 004
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_events_errors ON events (org_id, timestamp) "
            "WHERE error IS NOT NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_events_errors")
