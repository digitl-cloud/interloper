"""Stamp an execution's partition key in the fold's own insert, the only insert into ``executions``.

0003 stamped it from a ``BEFORE INSERT`` trigger on ``executions``. Since
``fold_execution_event`` is the only writer of that table, its insert now
takes the run's partition key when the event names none, as SQLite's fold
does, and the stamp trigger and its function go.

Revision ID: 0004
Revises: 0003
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None

_FOLD_EXECUTION_EVENT = r"""CREATE OR REPLACE FUNCTION public.fold_execution_event()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    event_attempt int := COALESCE((NEW.data->>'attempt')::int, 1);
    event_status text := CASE NEW.event_type
        WHEN 'operation_failed' THEN 'failed'
        WHEN 'operation_canceled' THEN 'canceled'
        WHEN 'operation_completed' THEN 'success'
        WHEN 'operation_started' THEN 'running'
        WHEN 'operation_skipped' THEN 'skipped'
        WHEN 'operation_retried' THEN 'running'
        WHEN 'operation_queued' THEN 'queued'
    END;
    event_rank int := CASE NEW.event_type WHEN 'operation_retried' THEN 6 ELSE execution_status_rank(event_status) END;
BEGIN
    INSERT INTO executions AS e
        (run_id, component_id, org_id, component_key, status, attempts,
         created_at, started_at, completed_at, partition_key)
    VALUES (
        NEW.run_id, NEW.component_id, NEW.org_id, NEW.component_key, event_status, event_attempt,
        CASE WHEN NEW.event_type = 'operation_queued' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type = 'operation_started' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type IN ('operation_completed', 'operation_failed', 'operation_canceled')
            THEN NEW.timestamp END,
        {partition_key}
    )
    ON CONFLICT (run_id, component_id) DO UPDATE SET
        status = CASE
            WHEN event_attempt > e.attempts
                OR (event_attempt = e.attempts AND event_rank < execution_status_rank(e.status))
            THEN EXCLUDED.status ELSE e.status
        END,
        component_key = CASE
            WHEN event_attempt > e.attempts
                OR (event_attempt = e.attempts AND event_rank < execution_status_rank(e.status))
            THEN EXCLUDED.component_key ELSE e.component_key
        END,
        attempts = GREATEST(e.attempts, EXCLUDED.attempts),
        created_at = LEAST(e.created_at, EXCLUDED.created_at),
        started_at = LEAST(e.started_at, EXCLUDED.started_at),
        completed_at = GREATEST(e.completed_at, EXCLUDED.completed_at);
    RETURN NULL;
END
$function$"""


def upgrade() -> None:
    op.execute(
        _FOLD_EXECUTION_EVENT.format(
            partition_key="COALESCE(NEW.data->>'partition_key', (SELECT partition_key FROM runs WHERE id = NEW.run_id))"
        )
    )
    op.execute("DROP TRIGGER IF EXISTS trg_executions_stamp_partition_key ON executions")
    op.execute("DROP FUNCTION IF EXISTS stamp_execution_partition_key()")


def downgrade() -> None:
    op.execute(r"""CREATE OR REPLACE FUNCTION public.stamp_execution_partition_key()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.partition_key := COALESCE(NEW.partition_key, (SELECT partition_key FROM runs WHERE id = NEW.run_id));
    RETURN NEW;
END
$function$""")
    op.execute("""CREATE TRIGGER trg_executions_stamp_partition_key
    BEFORE INSERT ON public.executions
    FOR EACH ROW EXECUTE FUNCTION stamp_execution_partition_key()""")
    op.execute(_FOLD_EXECUTION_EVENT.format(partition_key="NEW.data->>'partition_key'"))
