"""Give runs a heartbeat, and keep heartbeats off the realtime channel.

An executing run renews ``runs.heartbeat_at`` every few seconds, and the
reaper fails a run whose heartbeat went silent. Runs dispatched before this
release have no heartbeat yet, so they get one now and the startup timeout
applies to them; runs already running keep ``NULL`` and only the run timeout
reaches them, since their pods predate the heartbeat and never renew it.

``trg_runs_notify`` fired on every update of ``runs``, which a heartbeat would
turn into a broadcast per running run every few seconds. It is split in two:
inserts notify as before, and updates notify unless they only moved the
heartbeat. ``OLD`` cannot appear in an ``INSERT`` trigger's ``WHEN``, hence
the split. ``IF NOT EXISTS`` because ``create_all`` already adds the column on
a fresh database.

Revision ID: 011
Revises: 010
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "011"
down_revision: str | None = "010"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP WITH TIME ZONE")
    op.execute("UPDATE runs SET heartbeat_at = now() WHERE status = 'dispatched' AND heartbeat_at IS NULL")
    op.execute("DROP TRIGGER IF EXISTS trg_runs_notify ON runs")
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_runs_notify_insert AFTER INSERT ON runs "
        "FOR EACH ROW EXECUTE FUNCTION notify_target_change()"
    )
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_runs_notify_update AFTER UPDATE ON runs FOR EACH ROW "
        "WHEN (OLD.heartbeat_at IS NOT DISTINCT FROM NEW.heartbeat_at OR OLD.status IS DISTINCT FROM NEW.status) "
        "EXECUTE FUNCTION notify_target_change()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_runs_notify_update ON runs")
    op.execute("DROP TRIGGER IF EXISTS trg_runs_notify_insert ON runs")
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_runs_notify AFTER INSERT OR UPDATE ON runs "
        "FOR EACH ROW EXECUTE FUNCTION notify_target_change()"
    )
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS heartbeat_at")
