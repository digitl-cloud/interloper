"""Announce row changes on the ``table_changes`` channel, which the API's realtime socket relays.

Runs and backfills notify on insert and update, events on insert,
executions on insert and on an update that changes the row, components and
component relations on insert, update and delete. A run's beat alone does
not notify, so a running run does not broadcast every heartbeat. Payloads
stay small: what the socket's handlers need, never a configuration.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None

_FUNCTIONS = (
    r"""CREATE OR REPLACE FUNCTION public.notify_table_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
    $function$""",
    r"""CREATE OR REPLACE FUNCTION public.notify_component_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
    $function$""",
    r"""CREATE OR REPLACE FUNCTION public.notify_event_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
    $function$""",
    r"""CREATE OR REPLACE FUNCTION public.notify_target_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
    $function$""",
)

_TRIGGERS = (
    """CREATE TRIGGER trg_backfills_notify
    AFTER INSERT OR UPDATE ON public.backfills
    FOR EACH ROW EXECUTE FUNCTION notify_target_change()""",
    """CREATE TRIGGER trg_component_relations_notify
    AFTER INSERT OR DELETE OR UPDATE ON public.component_relations
    FOR EACH ROW EXECUTE FUNCTION notify_table_change()""",
    """CREATE TRIGGER trg_components_notify
    AFTER INSERT OR DELETE OR UPDATE ON public.components
    FOR EACH ROW EXECUTE FUNCTION notify_component_change()""",
    """CREATE TRIGGER trg_events_notify
    AFTER INSERT ON public.events
    FOR EACH ROW EXECUTE FUNCTION notify_event_change()""",
    """CREATE TRIGGER trg_executions_notify_insert
    AFTER INSERT ON public.executions
    FOR EACH ROW EXECUTE FUNCTION notify_table_change()""",
    """CREATE TRIGGER trg_executions_notify_update
    AFTER UPDATE ON public.executions
    FOR EACH ROW WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE FUNCTION notify_table_change()""",
    """CREATE TRIGGER trg_runs_notify_insert
    AFTER INSERT ON public.runs
    FOR EACH ROW EXECUTE FUNCTION notify_target_change()""",
    """CREATE TRIGGER trg_runs_notify_update
    AFTER UPDATE ON public.runs
    FOR EACH ROW WHEN (
        OLD.heartbeat_at IS NOT DISTINCT FROM NEW.heartbeat_at OR OLD.status IS DISTINCT FROM NEW.status
    )
    EXECUTE FUNCTION notify_target_change()""",
)


def upgrade() -> None:
    for statement in (*_FUNCTIONS, *_TRIGGERS):
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_backfills_notify ON backfills")
    op.execute("DROP TRIGGER IF EXISTS trg_component_relations_notify ON component_relations")
    op.execute("DROP TRIGGER IF EXISTS trg_components_notify ON components")
    op.execute("DROP TRIGGER IF EXISTS trg_events_notify ON events")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_notify_insert ON executions")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_notify_update ON executions")
    op.execute("DROP TRIGGER IF EXISTS trg_runs_notify_insert ON runs")
    op.execute("DROP TRIGGER IF EXISTS trg_runs_notify_update ON runs")
    op.execute("DROP FUNCTION IF EXISTS notify_target_change()")
    op.execute("DROP FUNCTION IF EXISTS notify_event_change()")
    op.execute("DROP FUNCTION IF EXISTS notify_component_change()")
    op.execute("DROP FUNCTION IF EXISTS notify_table_change()")
