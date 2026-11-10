"""Stamp the hooks of runs and backfills that will never fire them.

A canceled run or backfill, and a failed run a later attempt retried, have
no verdict for hooks to react to. The hook sweep used to recognise them on
every pass; the store now stamps ``hooks_evaluated_at`` at the transition
itself, so the sweep reads only verdicts. The rows such a transition reached
before this release still carry no stamp, and are stamped here once. The
downgrade leaves the stamps: they hold under either rule.

Revision ID: 010
Revises: 009
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "010"
down_revision: str | None = "009"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """UPDATE runs SET hooks_evaluated_at = COALESCE(completed_at, now())
        WHERE hooks_evaluated_at IS NULL
          AND (status = 'canceled'
               OR (status = 'failed' AND EXISTS (SELECT 1 FROM runs successor WHERE successor.retry_of = runs.id)))"""
    )
    op.execute(
        """UPDATE backfills SET hooks_evaluated_at = COALESCE(completed_at, now())
        WHERE hooks_evaluated_at IS NULL AND status = 'canceled'"""
    )


def downgrade() -> None:
    pass
