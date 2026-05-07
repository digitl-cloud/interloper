"""Add notify_table_change() function and triggers for realtime.

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
    ("trg_runs_notify", "runs", "INSERT OR UPDATE"),
    ("trg_events_notify", "events", "INSERT"),
    ("trg_backfills_notify", "backfills", "INSERT OR UPDATE"),
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

    for trigger_name, table, events in _TRIGGERS:
        op.execute(
            f"CREATE OR REPLACE TRIGGER {trigger_name} "
            f"AFTER {events} ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION notify_table_change()"
        )


def downgrade() -> None:
    for trigger_name, table, _events in _TRIGGERS:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}")
    op.execute("DROP FUNCTION IF EXISTS notify_table_change()")
