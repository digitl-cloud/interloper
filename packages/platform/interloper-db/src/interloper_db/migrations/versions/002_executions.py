"""Create the ``executions`` view.

One row per ``(run, operation)``: the current status derived from the
lifecycle events (severity first, then recency) plus the queued, started and
completed timestamps. Tables come from ``create_all()`` against the current
models, so this view references the current column names and event
vocabulary and applies as is on a fresh database.

Revision ID: 002
Revises: 001
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
CREATE OR REPLACE VIEW executions AS
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
                CASE e.event_type
                    WHEN 'operation_failed' THEN 1
                    WHEN 'operation_canceled' THEN 2
                    WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4
                    WHEN 'operation_skipped' THEN 5
                    WHEN 'operation_queued' THEN 6
                END,
                e.timestamp DESC
        ) AS rn,
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
          'operation_completed', 'operation_failed', 'operation_canceled'
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
        WHEN 'operation_queued' THEN 'queued'
    END AS status,
    r.started_at,
    r.completed_at,
    r.queued_at AS created_at
FROM ranked r
WHERE r.rn = 1
"""
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS executions CASCADE")
