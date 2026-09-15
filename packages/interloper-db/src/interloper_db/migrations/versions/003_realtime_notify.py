"""Notify functions and triggers for realtime.

Row changes fan out on the ``table_changes`` channel via ``pg_notify`` so
the UI stops polling. ``pg_notify`` caps payloads at ~8KB, and a trigger
that raises aborts the write itself, so each table notifies with the
shape it can afford:

- ``component_relations`` notify with the full record;
- ``components`` notify with a slim record (``id``/``kind``/``parent_id``):
  rows carry unbounded payloads (JSONB config, encrypted blobs), and
  subscribers refetch through the API, which also keeps secret payloads
  off the notification channel;
- ``events`` notify with the record, its unbounded text fields capped once
  the payload outgrows the limit, instead of losing the event;
- ``runs`` and ``backfills`` notify with the record plus the target's
  identity (``component_kind``/``component_key``/``component_name``), the
  same shape their APIs serve, so a client never refetches to name the
  target. The identity fields null together exactly when the target is
  gone: deletion nulls ``component_id`` (``ON DELETE SET NULL``), and that
  update fires this same trigger.

Revision ID: 003
Revises: 002
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "003"
down_revision: str | None = "002"
branch_labels: str | None = None
depends_on: str | None = None

_FUNCTIONS = [
    """
    CREATE OR REPLACE FUNCTION notify_table_change()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        payload jsonb;
        rec RECORD;
    BEGIN
        rec := COALESCE(NEW, OLD);
        payload := jsonb_build_object(
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'org_id', rec.org_id,
            'record', row_to_json(rec)::jsonb
        );
        PERFORM pg_notify('table_changes', payload::text);
        RETURN rec;
    END;
    $$
    """,
    """
    CREATE OR REPLACE FUNCTION notify_component_change()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        payload jsonb;
        rec RECORD;
    BEGIN
        rec := COALESCE(NEW, OLD);
        payload := jsonb_build_object(
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'org_id', rec.org_id,
            'record', jsonb_build_object('id', rec.id, 'kind', rec.kind, 'parent_id', rec.parent_id)
        );
        PERFORM pg_notify('table_changes', payload::text);
        RETURN rec;
    END;
    $$
    """,
    """
    CREATE OR REPLACE FUNCTION notify_event_change()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        rec jsonb;
    BEGIN
        rec := to_jsonb(NEW);
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
    """,
    """
    CREATE OR REPLACE FUNCTION notify_target_change()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        target_kind text;
        target_key text;
        target_name text;
    BEGIN
        SELECT kind, key, name INTO target_kind, target_key, target_name
        FROM components WHERE id = NEW.component_id;
        PERFORM pg_notify('table_changes', jsonb_build_object(
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'org_id', NEW.org_id,
            'record', to_jsonb(NEW) || jsonb_build_object(
                'component_kind', target_kind,
                'component_key', target_key,
                'component_name', target_name
            )
        )::text);
        RETURN NEW;
    END;
    $$
    """,
]

_TRIGGERS = [
    # (trigger, table, events, function)
    ("trg_runs_notify", "runs", "INSERT OR UPDATE", "notify_target_change"),
    ("trg_backfills_notify", "backfills", "INSERT OR UPDATE", "notify_target_change"),
    ("trg_events_notify", "events", "INSERT", "notify_event_change"),
    ("trg_components_notify", "components", "INSERT OR UPDATE OR DELETE", "notify_component_change"),
    ("trg_component_relations_notify", "component_relations", "INSERT OR UPDATE OR DELETE", "notify_table_change"),
]


def upgrade() -> None:
    for function in _FUNCTIONS:
        op.execute(function)

    for trigger_name, table, events, function in _TRIGGERS:
        op.execute(
            f"CREATE OR REPLACE TRIGGER {trigger_name} "
            f"AFTER {events} ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {function}()"
        )


def downgrade() -> None:
    for trigger_name, table, _events, _function in _TRIGGERS:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}")
    for function in ("notify_target_change", "notify_event_change", "notify_component_change", "notify_table_change"):
        op.execute(f"DROP FUNCTION IF EXISTS {function}()")
