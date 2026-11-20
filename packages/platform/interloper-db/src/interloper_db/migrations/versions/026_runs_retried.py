"""Record on a run that its next attempt was queued, and index the runs reads that grew with history.

A listing shows one row per stack, its latest attempt. Nothing on a run said
whether it was that attempt, so every reader probed each run for a later
attempt of its stack. A page of fifty probes fifty runs; the total of that
page, and every read that filters the latest attempts before it windows
them, probed the organisation's whole history: 16,000 index blocks to count
8,000 runs.

``retried`` is set on a run in the transaction that queues its next attempt
and never cleared. A stack is a linear chain, each attempt number held by one
run, and no run is ever deleted, so the latest attempt of every stack is the
one run of it not retried. The upgrade sets it on every run that has a later
attempt, with the realtime trigger off: a rewrite of history is not news for
the open browsers.

Three indexes serve the reads that scanned an organisation's runs whatever
they kept. ``ix_runs_org_id_completed_at`` finds the runs that completed in a
window (the overview's last day, its most recent ones) and the timeline's
window. ``ix_runs_component_id_created_at`` makes a target's latest run one
probe, where every run of the organisation was ranked; it replaces
``ix_runs_component_id``, whose lookups by target it still serves.
``ix_runs_last_success`` makes a target's last success one probe, where every
successful run of the organisation was grouped.

On a fresh database ``create_all`` made the column and the indexes from the
model, so every statement is idempotent.

Revision ID: 026
Revises: 025
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "026"
down_revision: str | None = "025"
branch_labels: str | None = None
depends_on: str | None = None

_RETRIED = """UPDATE runs SET retried = true
WHERE NOT retried AND EXISTS (
    SELECT 1 FROM runs AS later WHERE later.root_run_id = runs.root_run_id AND later.attempt > runs.attempt
)"""


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS retried boolean NOT NULL DEFAULT false")
    op.execute("ALTER TABLE runs DISABLE TRIGGER trg_runs_notify_update")
    op.execute(_RETRIED)
    op.execute("ALTER TABLE runs ENABLE TRIGGER trg_runs_notify_update")
    with op.get_context().autocommit_block():
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_runs_org_id_completed_at ON runs (org_id, completed_at)")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_runs_component_id_created_at ON runs (component_id, created_at)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_runs_last_success ON runs (component_id, completed_at) "
            "WHERE status = 'success'"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_runs_component_id")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_runs_component_id ON runs (component_id)")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_runs_last_success")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_runs_component_id_created_at")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_runs_org_id_completed_at")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS retried")
