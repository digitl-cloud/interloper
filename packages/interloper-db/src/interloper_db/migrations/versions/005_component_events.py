"""Generalize the events table to components.

``asset_id``/``asset_key`` become ``component_id``/``component_key``, joined
by ``component_kind`` — an event can now reference a component of any kind
(hooks, jobs, …), mirroring the single ``components`` table. A ``data``
JSONB column keeps whatever producer metadata the structured columns don't
carry, so new event shapes need no further schema changes.

Also swaps the events realtime trigger onto a size-guarded notify function:
``pg_notify`` caps payloads at ~8KB while event text fields may hold 60KB,
so an oversized record previously made the trigger raise and abort the
INSERT, silently dropping the event.

Idempotent: fresh databases get the new shape from ``create_all()`` (and the
new view from the edited migration 002), so every step no-ops when its work
is already done.

Revision ID: 005
Revises: 004
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | None = None
depends_on: str | None = None

# Same definition as (the edited) migration 002 — re-applied here so
# databases that ran the original asset_id-based version pick up the renamed
# base columns and the kind guard.
_VIEW_SQL = """
CREATE OR REPLACE VIEW asset_executions AS
WITH ranked AS (
    SELECT
        e.run_id,
        e.org_id,
        e.component_id AS asset_id,
        e.component_key AS asset_key,
        e.event_type,
        e.timestamp,
        row_number() OVER (
            PARTITION BY e.run_id, e.component_id
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
            PARTITION BY e.run_id, e.component_id
        ) AS queued_at,
        min(CASE WHEN e.event_type = 'asset_started' THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS started_at,
        max(CASE WHEN e.event_type IN ('asset_completed', 'asset_failed', 'asset_canceled')
            THEN e.timestamp END) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS completed_at
    FROM events e
    WHERE e.component_id IS NOT NULL AND e.component_kind = 'asset'
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

_NOTIFY_EVENT_FN = """
CREATE OR REPLACE FUNCTION notify_event_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    rec jsonb;
BEGIN
    rec := to_jsonb(NEW);
    -- pg_notify rejects payloads over ~8KB, and raising here aborts the
    -- INSERT itself. Cap the unbounded fields instead of losing the event;
    -- subscribers get the full row when they refetch through the API.
    IF octet_length(rec::text) > 7000 THEN
        rec := (rec - 'data') || jsonb_build_object(
            'error', left(NEW.error, 1000),
            'traceback', left(NEW.traceback, 1000),
            'message', left(NEW.message, 1000)
        );
    END IF;
    PERFORM pg_notify('table_changes', jsonb_build_object(
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'org_id', NEW.org_id,
        'record', rec
    )::text);
    RETURN NEW;
END;
$$
"""


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("events")}

    if "asset_id" in columns:
        op.alter_column("events", "asset_id", new_column_name="component_id")
    if "asset_key" in columns:
        op.alter_column("events", "asset_key", new_column_name="component_key")
    if "component_kind" not in columns:
        op.add_column("events", sa.Column("component_kind", sa.String(), nullable=True))
        # Every pre-existing component-linked event row was an asset event.
        op.execute("UPDATE events SET component_kind = 'asset' WHERE component_id IS NOT NULL")
    if "data" not in columns:
        op.add_column("events", sa.Column("data", postgresql.JSONB(), nullable=True))

    op.execute("DROP INDEX IF EXISTS ix_events_asset_lookup")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_events_component_lookup "
        "ON events (run_id, component_id, event_type, timestamp)"
    )

    op.execute(_VIEW_SQL)

    op.execute(_NOTIFY_EVENT_FN)
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_events_notify "
        "AFTER INSERT ON events "
        "FOR EACH ROW EXECUTE FUNCTION notify_event_change()"
    )


def downgrade() -> None:
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_events_notify "
        "AFTER INSERT ON events "
        "FOR EACH ROW EXECUTE FUNCTION notify_table_change()"
    )
    op.execute("DROP FUNCTION IF EXISTS notify_event_change()")

    op.execute("DROP VIEW IF EXISTS asset_executions")

    op.execute("DROP INDEX IF EXISTS ix_events_component_lookup")
    op.drop_column("events", "data")
    op.drop_column("events", "component_kind")
    op.alter_column("events", "component_key", new_column_name="asset_key")
    op.alter_column("events", "component_id", new_column_name="asset_id")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_events_asset_lookup ON events (run_id, asset_key, event_type, timestamp)"
    )
    # Recreate the view in its original asset-column shape.
    op.execute(
        _VIEW_SQL.replace("e.component_id AS asset_id", "e.asset_id")
        .replace("e.component_key AS asset_key", "e.asset_key")
        .replace("PARTITION BY e.run_id, e.component_id", "PARTITION BY e.run_id, e.asset_id")
        .replace("WHERE e.component_id IS NOT NULL AND e.component_kind = 'asset'", "WHERE e.asset_id IS NOT NULL")
    )
