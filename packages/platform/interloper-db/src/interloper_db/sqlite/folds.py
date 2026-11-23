"""The folds Postgres's migrations hold, as SQLite triggers."""

from __future__ import annotations

from collections.abc import Iterator
from typing import ClassVar


class SQLiteFolds:
    """Postgres's folds as SQLite triggers, under the same names.

    Where SQLite cannot say what Postgres says: there are no functions, so
    the status ranks are written out; ``min()`` and ``max()`` of several
    values are ``NULL`` when one is, so ``LEAST(a, b)`` is
    ``COALESCE(min(a, b), a, b)``; and SQLite 3.51 and older reject a table
    alias inside a trigger, so tables go by their names.
    """

    FOLD_PARTITION: ClassVar[str] = """
    INSERT INTO partitions (org_id, component_id, partition_key, succeeded, failed_run_id)
    VALUES (
        NEW.org_id, NEW.component_id, NEW.partition_key, NEW.status = 'success',
        CASE WHEN NEW.status = 'failed' THEN NEW.run_id END
    )
    ON CONFLICT (org_id, component_id, partition_key) DO UPDATE SET
        succeeded = partitions.succeeded OR EXCLUDED.succeeded,
        failed_run_id = COALESCE(
            max(partitions.failed_run_id, EXCLUDED.failed_run_id), partitions.failed_run_id, EXCLUDED.failed_run_id
        )
    WHERE (EXCLUDED.succeeded AND NOT partitions.succeeded)
        OR partitions.failed_run_id IS NOT COALESCE(
            max(partitions.failed_run_id, EXCLUDED.failed_run_id), partitions.failed_run_id, EXCLUDED.failed_run_id
        );"""

    TRIGGERS: ClassVar[dict[str, str]] = {
        "trg_events_fold_execution": """CREATE TRIGGER trg_events_fold_execution
AFTER INSERT ON events
WHEN NEW.run_id IS NOT NULL
    AND NEW.component_id IS NOT NULL
    AND NEW.event_type IN (
        'operation_queued', 'operation_skipped', 'operation_started', 'operation_completed',
        'operation_failed', 'operation_canceled', 'operation_retried'
    )
BEGIN
    INSERT INTO executions
        (run_id, component_id, org_id, component_key, status, attempts,
         created_at, started_at, completed_at, partition_key)
    VALUES (
        NEW.run_id, NEW.component_id, NEW.org_id, NEW.component_key,
        CASE NEW.event_type
            WHEN 'operation_failed' THEN 'failed'
            WHEN 'operation_canceled' THEN 'canceled'
            WHEN 'operation_completed' THEN 'success'
            WHEN 'operation_started' THEN 'running'
            WHEN 'operation_skipped' THEN 'skipped'
            WHEN 'operation_retried' THEN 'running'
            WHEN 'operation_queued' THEN 'queued'
        END,
        COALESCE(CAST(json_extract(NEW.data, '$.attempt') AS INTEGER), 1),
        CASE WHEN NEW.event_type = 'operation_queued' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type = 'operation_started' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type IN ('operation_completed', 'operation_failed', 'operation_canceled')
            THEN NEW.timestamp END,
        COALESCE(json_extract(NEW.data, '$.partition_key'), (SELECT partition_key FROM runs WHERE id = NEW.run_id))
    )
    ON CONFLICT (run_id, component_id) DO UPDATE SET
        status = CASE
            WHEN EXCLUDED.attempts > executions.attempts OR (EXCLUDED.attempts = executions.attempts AND
                CASE NEW.event_type
                    WHEN 'operation_failed' THEN 1 WHEN 'operation_canceled' THEN 2 WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4 WHEN 'operation_skipped' THEN 5 WHEN 'operation_retried' THEN 6
                    WHEN 'operation_queued' THEN 7
                END < CASE executions.status
                    WHEN 'failed' THEN 1 WHEN 'canceled' THEN 2 WHEN 'success' THEN 3 WHEN 'running' THEN 4
                    WHEN 'skipped' THEN 5 WHEN 'queued' THEN 7
                END)
            THEN EXCLUDED.status ELSE executions.status
        END,
        component_key = CASE
            WHEN EXCLUDED.attempts > executions.attempts OR (EXCLUDED.attempts = executions.attempts AND
                CASE NEW.event_type
                    WHEN 'operation_failed' THEN 1 WHEN 'operation_canceled' THEN 2 WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4 WHEN 'operation_skipped' THEN 5 WHEN 'operation_retried' THEN 6
                    WHEN 'operation_queued' THEN 7
                END < CASE executions.status
                    WHEN 'failed' THEN 1 WHEN 'canceled' THEN 2 WHEN 'success' THEN 3 WHEN 'running' THEN 4
                    WHEN 'skipped' THEN 5 WHEN 'queued' THEN 7
                END)
            THEN EXCLUDED.component_key ELSE executions.component_key
        END,
        attempts = max(executions.attempts, EXCLUDED.attempts),
        created_at = COALESCE(
            min(executions.created_at, EXCLUDED.created_at), executions.created_at, EXCLUDED.created_at
        ),
        started_at = COALESCE(
            min(executions.started_at, EXCLUDED.started_at), executions.started_at, EXCLUDED.started_at
        ),
        completed_at = COALESCE(
            max(executions.completed_at, EXCLUDED.completed_at), executions.completed_at, EXCLUDED.completed_at
        );
END""",
        "trg_executions_fold_partition_insert": f"""CREATE TRIGGER trg_executions_fold_partition_insert
AFTER INSERT ON executions
WHEN NEW.partition_key IS NOT NULL
BEGIN{FOLD_PARTITION}
END""",
        "trg_executions_fold_partition_update": f"""CREATE TRIGGER trg_executions_fold_partition_update
AFTER UPDATE OF status ON executions
WHEN NEW.partition_key IS NOT NULL
    AND NEW.status IN ('success', 'failed')
    AND OLD.status IS NOT NEW.status
BEGIN{FOLD_PARTITION}
END""",
    }

    def statements(self) -> Iterator[str]:
        """The statements that (re)create the fold triggers, each dropped first.

        Yields:
            One DDL statement at a time.
        """
        for name, definition in self.TRIGGERS.items():
            yield f"DROP TRIGGER IF EXISTS {name}"
            yield definition
