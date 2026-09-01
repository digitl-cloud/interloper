"""Speak operations end to end: rewrite the persisted event vocabulary.

Runs execute operations (an asset materializes, a connection renews);
the persisted events still carried the asset-era vocabulary. This
migration rewrites history so no queryable row speaks the old names:

- lifecycle ``event_type`` strings move from ``asset_*`` to
  ``operation_*``, and the ``data()``-call events from ``asset_exec_*``
  to ``asset_data_*`` (still asset semantics — but ``exec`` collided
  with the executions vocabulary, and it is an abbreviation);
- the human ``message`` prefix on those rows follows;
- the spilled ``asset_qualified_key`` data key becomes ``qualified_key``;
- ``component_kind`` is reconciled against the live components table
  (the old producers stamped every node as ``asset``);
- the ``asset_executions`` view (and its alias columns) is replaced by
  ``executions``, guarded by the lifecycle event types instead of a kind
  filter so every operation's execution is visible, not only assets'.

Event ids are left untouched: they are opaque insert-time dedup keys, and
no future insert can collide with a completed run's. Deploys must drain
in-flight runs before migrating — a run emitting old-vocabulary events
after this rewrite would reintroduce them (the rewrite is idempotent, so
re-running it also heals that).

Revision ID: 016
Revises: 015
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "016"
down_revision: str | None = "015"
branch_labels: str | None = None
depends_on: str | None = None

_LIFECYCLE_RENAMES = {
    "asset_queued": "operation_queued",
    "asset_skipped": "operation_skipped",
    "asset_started": "operation_started",
    "asset_completed": "operation_completed",
    "asset_failed": "operation_failed",
    "asset_canceled": "operation_canceled",
}

_DATA_RENAMES = {
    "asset_exec_started": "asset_data_started",
    "asset_exec_completed": "asset_data_completed",
    "asset_exec_failed": "asset_data_failed",
}

_VIEW_SQL = """
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


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS asset_executions CASCADE")

    for old, new in (_LIFECYCLE_RENAMES | _DATA_RENAMES).items():
        op.execute(f"UPDATE events SET event_type = '{new}' WHERE event_type = '{old}'")

    op.execute(
        """
        UPDATE events
        SET message = 'Operation' || substring(message from 6)
        WHERE event_type IN (
            'operation_queued', 'operation_skipped', 'operation_started',
            'operation_completed', 'operation_failed', 'operation_canceled'
        )
        AND message LIKE 'Asset %'
        """
    )

    op.execute(
        """
        UPDATE events
        SET data = (data - 'asset_qualified_key')
            || jsonb_build_object('qualified_key', data -> 'asset_qualified_key')
        WHERE data ? 'asset_qualified_key'
        """
    )

    op.execute(
        """
        UPDATE events e
        SET component_kind = c.kind
        FROM components c
        WHERE e.component_id = c.id AND e.component_kind IS DISTINCT FROM c.kind
        """
    )

    op.execute(_VIEW_SQL)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS executions CASCADE")

    for old, new in (_LIFECYCLE_RENAMES | _DATA_RENAMES).items():
        op.execute(f"UPDATE events SET event_type = '{old}' WHERE event_type = '{new}'")

    op.execute(
        """
        UPDATE events
        SET message = 'Asset' || substring(message from 10)
        WHERE event_type IN (
            'asset_queued', 'asset_skipped', 'asset_started',
            'asset_completed', 'asset_failed', 'asset_canceled'
        )
        AND message LIKE 'Operation %'
        """
    )

    op.execute(
        """
        UPDATE events
        SET data = (data - 'qualified_key')
            || jsonb_build_object('asset_qualified_key', data -> 'qualified_key')
        WHERE data ? 'qualified_key'
        """
    )

    legacy_view = (
        _VIEW_SQL.replace("VIEW executions", "VIEW asset_executions")
        .replace("operation_", "asset_")
        .replace("r.component_id,", "r.component_id AS asset_id,")
        .replace("r.component_key,", "r.component_key AS asset_key,")
    )
    op.execute(legacy_view)
