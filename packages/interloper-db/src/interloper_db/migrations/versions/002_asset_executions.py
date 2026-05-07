"""Create asset_executions view.

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
CREATE OR REPLACE VIEW asset_executions AS
WITH ranked AS (
    SELECT
        e.run_id,
        e.org_id,
        e.asset_id,
        e.asset_key,
        e.event_type,
        e.timestamp,
        row_number() OVER (
            PARTITION BY e.run_id, e.asset_id
            ORDER BY
                CASE e.event_type
                    WHEN 'asset_failed' THEN 1
                    WHEN 'asset_canceled' THEN 2
                    WHEN 'asset_completed' THEN 3
                    WHEN 'asset_started' THEN 4
                    WHEN 'asset_skipped' THEN 5
                    WHEN 'asset_queued' THEN 6
                END,
                e.timestamp DESC
        ) AS rn,
        min(CASE WHEN e.event_type = 'asset_queued' THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.asset_id
        ) AS queued_at,
        min(CASE WHEN e.event_type = 'asset_started' THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.asset_id
        ) AS started_at,
        max(CASE WHEN e.event_type IN ('asset_completed', 'asset_failed', 'asset_canceled')
            THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.asset_id
        ) AS completed_at
    FROM events e
    WHERE e.asset_id IS NOT NULL
)
SELECT
    r.run_id,
    r.org_id,
    r.asset_id,
    r.asset_key,
    CASE r.event_type
        WHEN 'asset_failed' THEN 'failed'
        WHEN 'asset_canceled' THEN 'canceled'
        WHEN 'asset_completed' THEN 'success'
        WHEN 'asset_started' THEN 'running'
        WHEN 'asset_skipped' THEN 'skipped'
        WHEN 'asset_queued' THEN 'queued'
    END AS status,
    r.started_at,
    r.completed_at,
    r.queued_at AS created_at
FROM ranked r
WHERE r.rn = 1
"""
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS asset_executions CASCADE")
