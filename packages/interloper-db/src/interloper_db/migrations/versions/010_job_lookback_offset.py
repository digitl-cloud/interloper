"""Job trigger windows are counted in partitions, not days.

``CronJob.backfill_days`` becomes ``lookback`` (how many partitions each run
covers) plus ``offset`` (how many partitions back from the current one the
window ends). Existing jobs get ``offset = 1``, which is the "ends yesterday"
behaviour the day-based code hardcoded, so their windows do not move.

The rename cannot be left to the reader: a job's config is passed to
``CronJob(**config)`` on every run, and ``Component`` rejects unknown kwargs,
so a stale ``backfill_days`` key would fail hydration. Idempotent: only rows
still carrying the legacy key are touched.

Revision ID: 010
Revises: 009
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "010"
down_revision: str | None = "009"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("components"):
        return

    op.execute(
        sa.text(
            "UPDATE components SET config = "
            "(config - 'backfill_days') "
            "|| jsonb_build_object('lookback', config -> 'backfill_days', 'offset', 1) "
            "WHERE kind = 'job' AND config ? 'backfill_days'"
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("components"):
        return

    op.execute(
        sa.text(
            "UPDATE components SET config = "
            "(config - 'lookback' - 'offset') "
            "|| jsonb_build_object('backfill_days', config -> 'lookback') "
            "WHERE kind = 'job' AND config ? 'lookback'"
        )
    )
