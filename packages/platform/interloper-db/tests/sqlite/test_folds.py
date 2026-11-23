"""Tests for ``interloper_db.sqlite.folds``: events folded into executions and partitions on a SQLite database.

Every test goes through the triggers, as the platform does: it saves events
and reads what the folds made of them. The same sequences were run against
Postgres by hand, to the same rows.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db.models import Event, Execution, Partition, Run
from interloper_db.provision import bootstrap

_ORG = uuid4()
_ASSET = uuid4()
_T0 = datetime(2026, 11, 22, 10, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Engine]:
    """A SQLite database brought up as ``interloper dev`` brings one up.

    Args:
        tmp_path: Pytest's temporary directory, which holds the file.

    Yields:
        The engine bound to it, disposed once the test finishes.
    """
    engine = bootstrap(f"sqlite:///{tmp_path}/interloper.db")
    try:
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None


def _run(engine: Engine, *, partition_key: str | None = "2026-11-21", run_id: UUID | None = None) -> UUID:
    run_id = run_id or uuid4()
    with Session(engine) as session:
        session.add(Run(id=run_id, org_id=_ORG, partition_key=partition_key, root_run_id=run_id))
        session.commit()
    return run_id


def _save(
    engine: Engine,
    run_id: UUID | None,
    event_type: str,
    minute: int,
    *,
    component_id: UUID | None = _ASSET,
    key: str = "a",
    **data: Any,
) -> None:
    with Session(engine) as session:
        session.add(
            Event(
                org_id=_ORG,
                run_id=run_id,
                component_id=component_id,
                component_key=key,
                event_type=event_type,
                data=data or None,
                timestamp=_T0 + timedelta(minutes=minute),
            )
        )
        session.commit()


def _executions(engine: Engine) -> list[Execution]:
    with Session(engine) as session:
        return list(session.exec(select(Execution)).all())


def _execution(engine: Engine) -> Execution:
    (execution,) = _executions(engine)
    return execution


def _partitions(engine: Engine) -> list[Partition]:
    with Session(engine) as session:
        return list(session.exec(select(Partition)).all())


class TestExecutionFold:
    """One execution per run and operation, from the operation's lifecycle events."""

    def test_a_lifecycle_folds_into_one_execution(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_queued", 0)
        _save(database, run_id, "operation_started", 1)
        _save(database, run_id, "operation_completed", 2)

        execution = _execution(database)

        assert (execution.run_id, execution.component_id, execution.org_id) == (run_id, _ASSET, _ORG)
        assert (execution.status, execution.attempts, execution.component_key) == ("success", 1, "a")
        assert execution.created_at == _T0
        assert execution.started_at == _T0 + timedelta(minutes=1)
        assert execution.completed_at == _T0 + timedelta(minutes=2)

    def test_events_arriving_out_of_order_fold_to_the_same_execution(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_completed", 2)
        _save(database, run_id, "operation_started", 1)
        _save(database, run_id, "operation_queued", 0)

        execution = _execution(database)

        assert execution.status == "success"
        assert (execution.created_at, execution.started_at, execution.completed_at) == (
            _T0,
            _T0 + timedelta(minutes=1),
            _T0 + timedelta(minutes=2),
        )

    def test_a_later_attempt_decides_the_status_and_the_timestamps_span_every_attempt(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_started", 0, attempt=1)
        _save(database, run_id, "operation_failed", 1, attempt=1)
        _save(database, run_id, "operation_retried", 2, attempt=2)
        _save(database, run_id, "operation_started", 3, attempt=2)
        _save(database, run_id, "operation_completed", 4, attempt=2)

        execution = _execution(database)

        assert (execution.status, execution.attempts) == ("success", 2)
        assert execution.started_at == _T0
        assert execution.completed_at == _T0 + timedelta(minutes=4)

    def test_an_earlier_attempt_arriving_late_decides_nothing(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_completed", 4, attempt=2)
        _save(database, run_id, "operation_failed", 1, attempt=1)

        execution = _execution(database)

        assert (execution.status, execution.attempts) == ("success", 2)
        assert execution.completed_at == _T0 + timedelta(minutes=4)

    @pytest.mark.parametrize(
        "order", [("operation_completed", "operation_failed"), ("operation_failed", "operation_completed")]
    )
    def test_within_an_attempt_the_most_severe_status_wins(self, database: Engine, order: tuple[str, str]) -> None:
        run_id = _run(database)
        for minute, event_type in enumerate(order):
            _save(database, run_id, event_type, minute)

        assert _execution(database).status == "failed"

    @pytest.mark.parametrize(("first", "status"), [("operation_queued", "running"), ("operation_skipped", "skipped")])
    def test_a_retry_outranks_only_a_queued_execution_of_its_attempt(
        self, database: Engine, first: str, status: str
    ) -> None:
        run_id = _run(database)
        _save(database, run_id, first, 0, attempt=2)
        _save(database, run_id, "operation_retried", 1, attempt=2)

        assert _execution(database).status == status

    def test_the_component_key_follows_the_event_that_decides(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_started", 0, key="a", attempt=1)
        _save(database, run_id, "operation_started", 1, key="b", attempt=2)
        _save(database, run_id, "operation_failed", 2, key="c", attempt=1)

        assert _execution(database).component_key == "b"

    def test_events_outside_an_operation_fold_nothing(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, None, "operation_started", 0)
        _save(database, run_id, "operation_started", 0, component_id=None)
        _save(database, run_id, "asset_materializing", 0)

        assert _executions(database) == []

    def test_the_partition_key_is_the_events_else_the_runs(self, database: Engine) -> None:
        run_id = _run(database, partition_key="2026-11-21")
        other = uuid4()
        _save(database, run_id, "operation_started", 0, partition_key="2026-11-20")
        _save(database, run_id, "operation_started", 0, component_id=other)

        keys = {execution.component_id: execution.partition_key for execution in _executions(database)}

        assert keys == {_ASSET: "2026-11-20", other: "2026-11-21"}

    def test_the_partition_key_is_stamped_when_the_execution_is_made(self, database: Engine) -> None:
        run_id = _run(database, partition_key="2026-11-21")
        _save(database, run_id, "operation_started", 0)
        _save(database, run_id, "operation_completed", 1, partition_key="2026-11-20")

        assert _execution(database).partition_key == "2026-11-21"


class TestPartitionFold:
    """One partition per asset and key ever attempted, from its executions."""

    def test_an_attempted_partition_has_a_row_before_any_outcome(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_started", 0)

        (partition,) = _partitions(database)

        assert (partition.org_id, partition.component_id, partition.partition_key) == (_ORG, _ASSET, "2026-11-21")
        assert (partition.succeeded, partition.failed_run_id) == (False, None)

    def test_a_success_marks_the_partition_succeeded(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_started", 0)
        _save(database, run_id, "operation_completed", 1)

        (partition,) = _partitions(database)

        assert (partition.succeeded, partition.failed_run_id) == (True, None)

    def test_the_greatest_failed_run_is_kept(self, database: Engine) -> None:
        low = _run(database, run_id=UUID(int=1))
        high = _run(database, run_id=UUID("ffffffff-ffff-4fff-bfff-ffffffffffff"))
        _save(database, high, "operation_failed", 0)
        _save(database, low, "operation_failed", 1)

        (partition,) = _partitions(database)

        assert (partition.succeeded, partition.failed_run_id) == (False, high)

    def test_a_success_after_a_failure_keeps_the_failed_run(self, database: Engine) -> None:
        failed = _run(database)
        succeeded = _run(database)
        _save(database, failed, "operation_failed", 0)
        _save(database, succeeded, "operation_completed", 1)

        (partition,) = _partitions(database)

        assert (partition.succeeded, partition.failed_run_id) == (True, failed)

    def test_a_retried_failure_that_succeeds_keeps_the_failed_run(self, database: Engine) -> None:
        run_id = _run(database)
        _save(database, run_id, "operation_failed", 0, attempt=1)
        _save(database, run_id, "operation_completed", 1, attempt=2)

        (partition,) = _partitions(database)

        assert (partition.succeeded, partition.failed_run_id) == (True, run_id)

    def test_an_execution_without_a_partition_key_folds_no_partition(self, database: Engine) -> None:
        run_id = _run(database, partition_key=None)
        _save(database, run_id, "operation_completed", 0)

        assert _executions(database) != []
        assert _partitions(database) == []
