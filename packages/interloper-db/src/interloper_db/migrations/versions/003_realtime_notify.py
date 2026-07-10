"""Notify functions and triggers for realtime.

Row changes fan out on the ``table_changes`` channel via ``pg_notify`` so
the UI stops polling. Most tables notify with the full record. Component
rows carry unbounded payloads (JSONB config, encrypted blobs) while
``pg_notify`` caps payloads at ~8KB — so ``components`` notifies with a
slim record (``id``/``kind``/``parent_id``; subscribers refetch through
the API, which also keeps secret payloads off the notification channel).

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

_TRIGGERS = [
    # (trigger, table, events, function)
    ("trg_runs_notify", "runs", "INSERT OR UPDATE", "notify_table_change"),
    ("trg_events_notify", "events", "INSERT", "notify_table_change"),
    ("trg_backfills_notify", "backfills", "INSERT OR UPDATE", "notify_table_change"),
    ("trg_components_notify", "components", "INSERT OR UPDATE OR DELETE", "notify_component_change"),
    ("trg_component_relations_notify", "component_relations", "INSERT OR UPDATE OR DELETE", "notify_table_change"),
]


def upgrade() -> None:
    op.execute(
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
        """
    )
    op.execute(
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
        """
    )

    for trigger_name, table, events, function in _TRIGGERS:
        op.execute(
            f"CREATE OR REPLACE TRIGGER {trigger_name} "
            f"AFTER {events} ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {function}()"
        )


def downgrade() -> None:
    for trigger_name, table, _events, _function in _TRIGGERS:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}")
    op.execute("DROP FUNCTION IF EXISTS notify_component_change()")
    op.execute("DROP FUNCTION IF EXISTS notify_table_change()")
