"""The folds migration 0003 runs on Postgres, as SQLite triggers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import ClassVar


class SQLiteFolds:
    """Fold events into executions, and executions into partitions, as the events are saved.

    The same folds as 0003's functions on Postgres, statement for statement,
    under the same trigger names. Three things SQLite cannot say as Postgres
    does shape them:

    * a trigger cannot assign ``NEW``, so the partition-key stamp, 0003's
      ``BEFORE INSERT`` trigger on ``executions``, is the fold's own insert,
      the only one into that table;
    * there are no functions, so the status ranks are written into the
      statement from `STATUS_RANK`;
    * ``min()`` and ``max()`` of several values are ``NULL`` when any of them
      is, where ``LEAST`` and ``GREATEST`` skip ``NULL``, so each is wrapped
      to skip it too.
    """

    EVENT_STATUS: ClassVar[Mapping[str, str]] = {
        "operation_failed": "failed",
        "operation_canceled": "canceled",
        "operation_completed": "success",
        "operation_started": "running",
        "operation_skipped": "skipped",
        "operation_retried": "running",
        "operation_queued": "queued",
    }
    STATUS_RANK: ClassVar[Mapping[str, int]] = {
        "failed": 1,
        "canceled": 2,
        "success": 3,
        "running": 4,
        "skipped": 5,
        "queued": 7,
    }
    RETRIED_RANK: ClassVar[int] = 6
    """A retry outranks only a queued execution of its attempt: it reopens the operation, it decides nothing."""

    def statements(self) -> Iterator[str]:
        """The statements that (re)create the fold triggers, each dropped first.

        Yields:
            One DDL statement at a time.
        """
        for name, definition in self.triggers().items():
            yield f"DROP TRIGGER IF EXISTS {name}"
            yield definition

    def triggers(self) -> dict[str, str]:
        """Each fold trigger's definition, by name.

        Returns:
            The ``CREATE TRIGGER`` statement of each trigger, keyed by its name.
        """
        event_types = ", ".join(f"'{event_type}'" for event_type in self.EVENT_STATUS)
        event_status = self._case("NEW.event_type", self.EVENT_STATUS)
        wins = (
            "excluded.attempts > executions.attempts OR (excluded.attempts = executions.attempts "
            f"AND {self._case('NEW.event_type', self._event_ranks())} "
            f"< {self._case('executions.status', self.STATUS_RANK)})"
        )
        created_at = self._least("executions.created_at", "excluded.created_at")
        started_at = self._least("executions.started_at", "excluded.started_at")
        completed_at = self._greatest("executions.completed_at", "excluded.completed_at")
        failed_run_id = self._greatest("partitions.failed_run_id", "excluded.failed_run_id")
        fold_partition = f"""
    INSERT INTO partitions (org_id, component_id, partition_key, succeeded, failed_run_id)
    VALUES (
        NEW.org_id, NEW.component_id, NEW.partition_key, NEW.status = 'success',
        CASE WHEN NEW.status = 'failed' THEN NEW.run_id END
    )
    ON CONFLICT (org_id, component_id, partition_key) DO UPDATE SET
        succeeded = partitions.succeeded OR excluded.succeeded,
        failed_run_id = {failed_run_id}
    WHERE (excluded.succeeded AND NOT partitions.succeeded)
        OR partitions.failed_run_id IS NOT {failed_run_id};"""
        return {
            "trg_events_fold_execution": f"""CREATE TRIGGER trg_events_fold_execution
AFTER INSERT ON events
WHEN NEW.run_id IS NOT NULL
    AND NEW.component_id IS NOT NULL
    AND NEW.event_type IN ({event_types})
BEGIN
    INSERT INTO executions
        (run_id, component_id, org_id, component_key, status, attempts,
         created_at, started_at, completed_at, partition_key)
    VALUES (
        NEW.run_id, NEW.component_id, NEW.org_id, NEW.component_key, {event_status},
        COALESCE(CAST(json_extract(NEW.data, '$.attempt') AS INTEGER), 1),
        CASE WHEN NEW.event_type = 'operation_queued' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type = 'operation_started' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type IN ('operation_completed', 'operation_failed', 'operation_canceled')
            THEN NEW.timestamp END,
        COALESCE(json_extract(NEW.data, '$.partition_key'), (SELECT partition_key FROM runs WHERE id = NEW.run_id))
    )
    ON CONFLICT (run_id, component_id) DO UPDATE SET
        status = CASE WHEN {wins} THEN excluded.status ELSE executions.status END,
        component_key = CASE WHEN {wins} THEN excluded.component_key ELSE executions.component_key END,
        attempts = max(executions.attempts, excluded.attempts),
        created_at = {created_at},
        started_at = {started_at},
        completed_at = {completed_at};
END""",
            "trg_executions_fold_partition_insert": f"""CREATE TRIGGER trg_executions_fold_partition_insert
AFTER INSERT ON executions
WHEN NEW.partition_key IS NOT NULL
BEGIN{fold_partition}
END""",
            "trg_executions_fold_partition_update": f"""CREATE TRIGGER trg_executions_fold_partition_update
AFTER UPDATE OF status ON executions
WHEN NEW.partition_key IS NOT NULL
    AND NEW.status IN ('success', 'failed')
    AND OLD.status IS NOT NEW.status
BEGIN{fold_partition}
END""",
        }

    # -- Internals -------------------------------------------------------------

    def _event_ranks(self) -> dict[str, int]:
        """Each lifecycle event's rank against an execution of the same attempt: its status's, but a retry's own.

        Returns:
            The rank of each event type.
        """
        return {
            event_type: self.RETRIED_RANK if event_type == "operation_retried" else self.STATUS_RANK[status]
            for event_type, status in self.EVENT_STATUS.items()
        }

    @staticmethod
    def _case(subject: str, outcomes: Mapping[str, str] | Mapping[str, int]) -> str:
        """Render a ``CASE`` mapping each value of ``subject`` to its outcome; any other value gives ``NULL``.

        Args:
            subject: The expression compared.
            outcomes: The outcome of each value, quoted when it is text.

        Returns:
            The ``CASE`` expression.
        """
        branches = " ".join(
            f"WHEN '{value}' THEN " + (f"'{outcome}'" if isinstance(outcome, str) else str(outcome))
            for value, outcome in outcomes.items()
        )
        return f"CASE {subject} {branches} END"

    @staticmethod
    def _least(first: str, second: str) -> str:
        """Render Postgres's ``LEAST`` of two values: the smaller, skipping ``NULL``.

        Args:
            first: One expression.
            second: The other.

        Returns:
            The expression.
        """
        return f"COALESCE(min({first}, {second}), {first}, {second})"

    @staticmethod
    def _greatest(first: str, second: str) -> str:
        """Render Postgres's ``GREATEST`` of two values: the larger, skipping ``NULL``.

        Args:
            first: One expression.
            second: The other.

        Returns:
            The expression.
        """
        return f"COALESCE(max({first}, {second}), {first}, {second})"
