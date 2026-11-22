"""Fold events into executions, and executions into partitions, as the events are saved.

``fold_execution_event`` keeps one ``executions`` row per run and operation
from that operation's lifecycle events: the latest attempt first, then
severity (``execution_status_rank``), then recency decides the status, and
the timestamps span every attempt. ``stamp_execution_partition_key`` files
a new execution under the partition its events name, else its run's.
``fold_partition`` keeps one ``partitions`` row per asset and partition
key, the coverage calendar's source. The application never writes either
table.

Revision ID: 0003
Revises: 0002
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None

_FUNCTIONS = (
    r"""CREATE OR REPLACE FUNCTION public.execution_status_rank(status text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT CASE status
        WHEN 'failed' THEN 1
        WHEN 'canceled' THEN 2
        WHEN 'success' THEN 3
        WHEN 'running' THEN 4
        WHEN 'skipped' THEN 5
        WHEN 'queued' THEN 7
    END
$function$""",
    r"""CREATE OR REPLACE FUNCTION public.fold_execution_event()
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
        NEW.data->>'partition_key'
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
$function$""",
    r"""CREATE OR REPLACE FUNCTION public.stamp_execution_partition_key()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.partition_key := COALESCE(NEW.partition_key, (SELECT partition_key FROM runs WHERE id = NEW.run_id));
    RETURN NEW;
END
$function$""",
    r"""CREATE OR REPLACE FUNCTION public.fold_partition()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO partitions AS p (org_id, component_id, partition_key, succeeded, failed_run_id)
    VALUES (
        NEW.org_id, NEW.component_id, NEW.partition_key, NEW.status = 'success',
        CASE WHEN NEW.status = 'failed' THEN NEW.run_id END
    )
    ON CONFLICT (org_id, component_id, partition_key) DO UPDATE SET
        succeeded = p.succeeded OR EXCLUDED.succeeded,
        failed_run_id = GREATEST(p.failed_run_id, EXCLUDED.failed_run_id)
    WHERE (EXCLUDED.succeeded AND NOT p.succeeded)
        OR p.failed_run_id IS DISTINCT FROM GREATEST(p.failed_run_id, EXCLUDED.failed_run_id);
    RETURN NULL;
END
$function$""",
)

_TRIGGERS = (
    """CREATE TRIGGER trg_events_fold_execution
    AFTER INSERT ON public.events
    FOR EACH ROW WHEN (
        NEW.run_id IS NOT NULL
        AND NEW.component_id IS NOT NULL
        AND NEW.event_type IN (
            'operation_queued', 'operation_skipped', 'operation_started', 'operation_completed',
            'operation_failed', 'operation_canceled', 'operation_retried'
        )
    )
    EXECUTE FUNCTION fold_execution_event()""",
    """CREATE TRIGGER trg_executions_fold_partition_insert
    AFTER INSERT ON public.executions
    FOR EACH ROW WHEN (NEW.partition_key IS NOT NULL)
    EXECUTE FUNCTION fold_partition()""",
    """CREATE TRIGGER trg_executions_fold_partition_update
    AFTER UPDATE OF status ON public.executions
    FOR EACH ROW WHEN (
        NEW.partition_key IS NOT NULL
        AND NEW.status IN ('success', 'failed')
        AND OLD.status IS DISTINCT FROM NEW.status
    )
    EXECUTE FUNCTION fold_partition()""",
    """CREATE TRIGGER trg_executions_stamp_partition_key
    BEFORE INSERT ON public.executions
    FOR EACH ROW EXECUTE FUNCTION stamp_execution_partition_key()""",
)


def upgrade() -> None:
    for statement in (*_FUNCTIONS, *_TRIGGERS):
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_events_fold_execution ON events")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_fold_partition_insert ON executions")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_fold_partition_update ON executions")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_stamp_partition_key ON executions")
    op.execute("DROP FUNCTION IF EXISTS fold_partition()")
    op.execute("DROP FUNCTION IF EXISTS stamp_execution_partition_key()")
    op.execute("DROP FUNCTION IF EXISTS fold_execution_event()")
    op.execute("DROP FUNCTION IF EXISTS execution_status_rank(status text)")
