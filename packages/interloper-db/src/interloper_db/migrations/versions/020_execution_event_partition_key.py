"""File an execution under the partition its operation names, else its run's.

A snapshot asset writes the period it observed in, whatever its run's window:
a backfill's run for 26 October observing on the 29th writes the 29th. Its
execution must be filed under the 29th for coverage to read it there, so the
partition key can no longer come from the run alone.

Every operation-lifecycle event now names the partition the operation consumes
(``data->>'partition_key'``) when it consumes one. ``fold_execution_event``
writes it on the row it creates, and ``stamp_execution_partition_key`` keeps
it, falling back to the run's key only when the event names none: an
unpartitioned operation's, and every event written before this revision. A
window-partitioned operation names its run's key, so its rows are unchanged.
The key is written once, on the row's creation: every event of an execution
names the same partition.

Revision ID: 020
Revises: 019
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "020"
down_revision: str | None = "019"
branch_labels: str | None = None
depends_on: str | None = None

_TERMINAL_EVENT_TYPES = "('operation_completed', 'operation_failed', 'operation_canceled')"


def _fold(partition_key: bool) -> str:
    """The fold of migration 012, writing the event's partition key on the rows it creates when asked.

    Args:
        partition_key: Whether the created row takes the event's ``partition_key``.

    Returns:
        The ``CREATE OR REPLACE FUNCTION`` statement.
    """
    columns = ", partition_key" if partition_key else ""
    values = ", NEW.data->>'partition_key'" if partition_key else ""
    return f"""CREATE OR REPLACE FUNCTION fold_execution_event() RETURNS trigger
LANGUAGE plpgsql AS $$
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
        (run_id, component_id, org_id, component_key, status, attempts, created_at, started_at, completed_at{columns})
    VALUES (
        NEW.run_id, NEW.component_id, NEW.org_id, NEW.component_key, event_status, event_attempt,
        CASE WHEN NEW.event_type = 'operation_queued' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type = 'operation_started' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type IN {_TERMINAL_EVENT_TYPES} THEN NEW.timestamp END{values}
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
$$"""


def _stamp(keep_event_key: bool) -> str:
    """The stamp of migration 013, keeping a key the fold wrote when asked.

    Args:
        keep_event_key: Whether a key already on the row wins over the run's.

    Returns:
        The ``CREATE OR REPLACE FUNCTION`` statement.
    """
    run_key = "(SELECT partition_key FROM runs WHERE id = NEW.run_id)"
    key = f"COALESCE(NEW.partition_key, {run_key})" if keep_event_key else run_key
    return f"""CREATE OR REPLACE FUNCTION stamp_execution_partition_key() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.partition_key := {key};
    RETURN NEW;
END
$$"""


def upgrade() -> None:
    op.execute(_stamp(keep_event_key=True))
    op.execute(_fold(partition_key=True))


def downgrade() -> None:
    op.execute(_fold(partition_key=False))
    op.execute(_stamp(keep_event_key=False))
