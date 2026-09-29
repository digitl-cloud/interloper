"""Give the hook evaluator a delivery cursor.

The evaluator swept terminal runs by a wall-clock watermark, so a run whose
clock trailed the scheduler's, or whose completing transaction outlived the
overlap window, was never seen, and everything that finished while the
scheduler restarted was dropped. ``hooks_evaluated_at`` on runs and backfills
replaces the watermark: a terminal row is swept until it is stamped. The
partial indexes keep the unevaluated set, which is tiny, cheap to find.

Existing terminal rows are stamped so the first sweep does not replay
history. The DDL is idempotent, because ``create_all`` creates the tables
from the models and then runs the chain.

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | None = None
depends_on: str | None = None

_TABLES = ("runs", "backfills")


def upgrade() -> None:
    for table in _TABLES:
        op.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS hooks_evaluated_at TIMESTAMPTZ")
        op.execute(
            f"UPDATE {table} SET hooks_evaluated_at = completed_at "
            "WHERE status IN ('success', 'failed', 'canceled') AND hooks_evaluated_at IS NULL"
        )
    with op.get_context().autocommit_block():
        for table in _TABLES:
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_{table}_hooks_pending ON {table} (completed_at) "
                "WHERE hooks_evaluated_at IS NULL"
            )


def downgrade() -> None:
    for table in _TABLES:
        op.execute(f"DROP INDEX IF EXISTS ix_{table}_hooks_pending")
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS hooks_evaluated_at")
