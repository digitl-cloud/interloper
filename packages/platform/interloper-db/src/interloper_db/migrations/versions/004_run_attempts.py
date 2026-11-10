"""Add the run stack and its schedule.

A run is one attempt. ``root_run_id`` groups the attempts of one unit of work
so a stack is a single indexed predicate rather than a recursive walk, and is
the run's own id for a first attempt. ``scheduled_for`` holds a retry's
backoff: the queue claims a run only once it has passed.

The DDL is idempotent, because ``create_all`` creates the tables from the
models and *then* runs the chain: on a fresh database the columns already
exist by the time this runs, and on an existing one they do not.

Existing rows are folded into stacks by walking the ``retry_of`` chains manual
retries already created. A run whose predecessor was deleted becomes its own
root, which is correct: its lineage is gone.

Revision ID: 004
Revises: 003
"""

from __future__ import annotations

from alembic import op

_EXECUTIONS_VIEW = """CREATE OR REPLACE VIEW executions AS
WITH ranked AS (
    SELECT
        e.run_id,
        e.org_id,
        e.component_id,
        e.component_key,
        e.event_type,
        e.timestamp,
        row_number() OVER (
            PARTITION BY e.run_id, e.component_id
            ORDER BY
                COALESCE((e.data->>'attempt')::int, 1) DESC,
                CASE e.event_type
                    WHEN 'operation_failed' THEN 1
                    WHEN 'operation_canceled' THEN 2
                    WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4
                    WHEN 'operation_skipped' THEN 5
                    WHEN 'operation_retried' THEN 6
                    WHEN 'operation_queued' THEN 7
                END,
                e.timestamp DESC
        ) AS rn,
        max(COALESCE((e.data->>'attempt')::int, 1)) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS attempts,
        min(CASE WHEN e.event_type = 'operation_queued' THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS queued_at,
        min(CASE WHEN e.event_type = 'operation_started' THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS started_at,
        max(CASE WHEN e.event_type IN ('operation_completed', 'operation_failed', 'operation_canceled')
            THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS completed_at
    FROM events e
    WHERE e.component_id IS NOT NULL
      AND e.event_type IN (
          'operation_queued', 'operation_skipped', 'operation_started',
          'operation_completed', 'operation_failed', 'operation_canceled',
          'operation_retried'
      )
)
SELECT
    r.run_id,
    r.org_id,
    r.component_id,
    r.component_key,
    CASE r.event_type
        WHEN 'operation_failed' THEN 'failed'
        WHEN 'operation_canceled' THEN 'canceled'
        WHEN 'operation_completed' THEN 'success'
        WHEN 'operation_started' THEN 'running'
        WHEN 'operation_skipped' THEN 'skipped'
        WHEN 'operation_retried' THEN 'running'
        WHEN 'operation_queued' THEN 'queued'
    END AS status,
    r.started_at,
    r.completed_at,
    r.queued_at AS created_at,
    r.attempts
FROM ranked r
WHERE r.rn = 1
"""

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS root_run_id uuid REFERENCES runs(id) ON DELETE SET NULL")
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS scheduled_for timestamptz")
    op.execute(
        """
        WITH RECURSIVE chain AS (
            SELECT id, id AS root FROM runs WHERE retry_of IS NULL
            UNION ALL
            SELECT r.id, c.root FROM runs r JOIN chain c ON r.retry_of = c.id
        )
        UPDATE runs SET root_run_id = chain.root FROM chain WHERE runs.id = chain.id
        """
    )
    op.execute("UPDATE runs SET root_run_id = id WHERE root_run_id IS NULL")
    op.execute("ALTER TABLE runs ALTER COLUMN root_run_id SET NOT NULL")
    op.execute("CREATE INDEX IF NOT EXISTS ix_runs_root_run_id ON runs (root_run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_runs_claim ON runs (status, scheduled_for, created_at)")
    op.execute(_EXECUTIONS_VIEW)


def downgrade() -> None:
    # Leaves the attempt-aware view in place: it is a superset of the old one
    # for any data the old chain could have produced, and re-creating the
    # previous definition here would duplicate migration 002 verbatim.
    op.execute("DROP INDEX IF EXISTS ix_runs_claim")
    op.execute("DROP INDEX IF EXISTS ix_runs_root_run_id")
    op.execute("ALTER TABLE runs DROP COLUMN scheduled_for")
    op.execute("ALTER TABLE runs DROP COLUMN root_run_id")
