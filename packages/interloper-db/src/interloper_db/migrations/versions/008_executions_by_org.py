"""Scope the ``executions`` view by organisation.

Postgres pushes a filter on a view column below the view's window functions
only when that column is in every window's ``PARTITION BY``. The windows
partitioned by ``(run_id, component_id)`` alone, so an organisation's read
ranked every organisation's operation events and dropped the others
afterwards. They now partition by ``(org_id, run_id, component_id)``: a run
belongs to one organisation, so the rows are the same, and an ``org_id``
filter on the view reaches the ``events`` scan.

``ix_events_executions`` serves that scan: a partial index over exactly the
rows the view reads, keyed in the windows' partition order so the ranking
sorts within each ``(run, operation)`` instead of over the whole set. It is
built ``CONCURRENTLY`` so a busy ``events`` table keeps taking writes, and
``IF NOT EXISTS`` because ``create_all`` already builds it on a fresh
database. A build that failed (a concurrent build waiting out older
transactions can hit the lock timeout) leaves an invalid index behind, which
``IF NOT EXISTS`` would keep forever, so a retry drops it first. Downgrading restores the definition of migration 004.

Revision ID: 008
Revises: 007
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | None = None
depends_on: str | None = None

_OPERATION_EVENTS = """component_id IS NOT NULL
      AND event_type IN (
          'operation_queued', 'operation_skipped', 'operation_started',
          'operation_completed', 'operation_failed', 'operation_canceled',
          'operation_retried'
      )"""

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
            PARTITION BY {partition}
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
            PARTITION BY {partition}
        ) AS attempts,
        min(CASE WHEN e.event_type = 'operation_queued' THEN e.timestamp END) OVER (
            PARTITION BY {partition}
        ) AS queued_at,
        min(CASE WHEN e.event_type = 'operation_started' THEN e.timestamp END) OVER (
            PARTITION BY {partition}
        ) AS started_at,
        max(CASE WHEN e.event_type IN ('operation_completed', 'operation_failed', 'operation_canceled')
            THEN e.timestamp END) OVER (
            PARTITION BY {partition}
        ) AS completed_at
    FROM events e
    WHERE {operation_events}
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


def upgrade() -> None:
    op.execute(
        _EXECUTIONS_VIEW.format(partition="e.org_id, e.run_id, e.component_id", operation_events=_OPERATION_EVENTS)
    )
    invalid = (
        op.get_bind()
        .exec_driver_sql(
            "SELECT 1 FROM pg_index WHERE indexrelid = to_regclass('ix_events_executions') AND NOT indisvalid"
        )
        .scalar()
    )
    with op.get_context().autocommit_block():
        if invalid:
            op.execute("DROP INDEX CONCURRENTLY ix_events_executions")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_events_executions ON events (org_id, run_id, component_id) "
            f"WHERE {_OPERATION_EVENTS}"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_events_executions")
    op.execute(_EXECUTIONS_VIEW.format(partition="e.run_id, e.component_id", operation_events=_OPERATION_EVENTS))
