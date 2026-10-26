"""Record why a run did not succeed on the run itself.

A run's failure reason lived only in a ``run_failed`` event, which the store
authored for endings no runner reported (the reaper, a failed dispatch) and
readers dug back out of the run's events. ``runs.error`` now holds it, written
by the one step every ending takes, so events are history and no reader parses
them for a run's state. Existing failed runs take the error of their newest
``run_failed`` event that carries one.

Revision ID: 014
Revises: 013
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "014"
down_revision: str | None = "013"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS error varchar")
    op.execute(
        """UPDATE runs SET error = (
            SELECT e.error FROM events e
            WHERE e.run_id = runs.id AND e.event_type = 'run_failed' AND e.error IS NOT NULL
            ORDER BY e.timestamp DESC
            LIMIT 1
        )
        WHERE status = 'failed'"""
    )


def downgrade() -> None:
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS error")
