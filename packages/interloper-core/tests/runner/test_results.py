"""Tests for ``interloper.runner.results``."""

from __future__ import annotations

import datetime as dt

import pytest

from interloper.operation.base import OperationResult
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runner.results import ExecutionInfo, ExecutionStatus, RunResult


def _info(status: ExecutionStatus = ExecutionStatus.QUEUED, key: str = "source.asset") -> ExecutionInfo:
    return ExecutionInfo(component_id=key, component_key=key.split(".")[-1], status=status)


class TestExecutionInfoTransitions:
    """Each transition stamps the timestamp its status implies."""

    def test_running_stamps_only_the_start(self) -> None:
        info = _info()

        info.mark_running()

        assert info.status is ExecutionStatus.RUNNING
        assert info.start_time is not None
        assert info.end_time is None
        assert info.is_terminal is False

    def test_completed_stamps_the_end(self) -> None:
        info = _info()
        info.mark_running()

        info.mark_completed()

        assert info.status is ExecutionStatus.COMPLETED
        assert info.end_time is not None
        assert info.is_terminal is True

    def test_failed_records_the_error_and_the_original_exception(self) -> None:
        info = _info()
        exception = ValueError("upstream 500")

        info.mark_failed("upstream 500", tb="Traceback...", exception=exception)

        assert info.status is ExecutionStatus.FAILED
        assert info.error == "upstream 500"
        assert info.traceback == "Traceback..."
        # In-process runners keep the exception so ``reraise`` re-raises it faithfully.
        assert info.exception is exception
        assert info.is_terminal is True

    def test_a_cross_process_failure_carries_only_its_message(self) -> None:
        info = _info()

        info.mark_failed("worker died")

        assert info.error == "worker died"
        assert info.traceback is None
        assert info.exception is None

    def test_canceled_is_terminal(self) -> None:
        info = _info()

        info.mark_canceled()

        assert info.status is ExecutionStatus.CANCELED
        assert info.end_time is not None
        assert info.is_terminal is True

    @pytest.mark.parametrize(
        ("status", "terminal"),
        [
            (ExecutionStatus.QUEUED, False),
            (ExecutionStatus.READY, False),
            (ExecutionStatus.RUNNING, False),
            (ExecutionStatus.COMPLETED, True),
            (ExecutionStatus.FAILED, True),
            (ExecutionStatus.CANCELED, True),
            (ExecutionStatus.SKIPPED, True),
        ],
    )
    def test_terminality_per_status(self, status: ExecutionStatus, terminal: bool) -> None:
        assert _info(status).is_terminal is terminal


class TestExecutionTime:
    """The computed duration needs both ends."""

    def test_measured_from_both_timestamps(self) -> None:
        info = _info()
        info.start_time = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        info.end_time = info.start_time + dt.timedelta(seconds=2, milliseconds=500)

        assert info.execution_time == 2.5

    def test_unknown_while_still_running(self) -> None:
        info = _info()
        info.mark_running()

        assert info.execution_time is None

    def test_unknown_for_a_node_that_never_ran(self) -> None:
        assert _info(ExecutionStatus.SKIPPED).execution_time is None


class TestToDict:
    """Serialization drops the in-memory-only fields."""

    def test_carries_status_timestamps_and_error(self) -> None:
        info = _info(key="demo.a")
        info.start_time = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        info.mark_failed("nope", tb="tb", exception=ValueError("nope"))
        info.effects = OperationResult(config={"x": 1})

        payload = info.to_dict()

        assert payload["component_id"] == "demo.a"
        assert payload["component_key"] == "a"
        assert payload["status"] == "failed"
        assert payload["start_time"] == "2026-01-01T00:00:00+00:00"
        assert payload["end_time"] is not None
        assert payload["execution_time"] is not None
        assert payload["error"] == "nope"
        assert payload["traceback"] == "tb"
        # ``exception`` and ``effects`` never leave the process.
        assert "exception" not in payload
        assert "effects" not in payload

    def test_null_timestamps_stay_null(self) -> None:
        payload = _info().to_dict()

        assert payload["start_time"] is None
        assert payload["end_time"] is None
        assert payload["execution_time"] is None


def _result(**statuses: ExecutionStatus) -> RunResult:
    return RunResult(executions={key: _info(status, key) for key, status in statuses.items()})


class TestRunResultIdBuckets:
    """Executions are bucketed by terminal status."""

    def test_partitions_ids_by_status(self) -> None:
        result = _result(
            a=ExecutionStatus.COMPLETED,
            b=ExecutionStatus.FAILED,
            c=ExecutionStatus.CANCELED,
            d=ExecutionStatus.SKIPPED,
        )

        assert result.completed_ids == ["a"]
        assert result.failed_ids == ["b"]
        assert result.canceled_ids == ["c"]

    def test_an_empty_run_has_empty_buckets(self) -> None:
        result = RunResult()

        assert (result.completed_ids, result.failed_ids, result.canceled_ids) == ([], [], [])
        assert result.status is ExecutionStatus.COMPLETED


class TestRunResultStr:
    """The human-friendly summary."""

    def test_unpartitioned_run(self) -> None:
        result = _result(a=ExecutionStatus.COMPLETED)

        rendered = str(result)

        assert rendered.startswith("RunResult(")
        assert "partition=None" in rendered
        assert "completed=1" in rendered
        assert "failed=0" in rendered
        assert "time=0.00s" in rendered

    def test_names_the_partition(self) -> None:
        result = RunResult(partition_or_window=TimePartition.from_key("2026-06-01"))

        assert "partition=2026-06-01" in str(result)

    def test_names_the_window(self) -> None:
        window = TimePartitionWindow(start=dt.date(2026, 6, 1), end=dt.date(2026, 6, 3))
        result = RunResult(partition_or_window=window)

        rendered = str(result)

        assert "window=2026-06-01:2026-06-03" in rendered
        assert "partition=" not in rendered

    def test_lists_the_failed_and_canceled_ids(self) -> None:
        result = _result(
            a=ExecutionStatus.FAILED, b=ExecutionStatus.FAILED, c=ExecutionStatus.CANCELED
        )

        rendered = str(result)

        assert "failed=[a, b]" in rendered
        assert "canceled=[c]" in rendered

    def test_long_id_lists_are_truncated_with_a_remainder(self) -> None:
        failed = {f"f{i}": ExecutionStatus.FAILED for i in range(7)}
        canceled = {f"c{i}": ExecutionStatus.CANCELED for i in range(9)}
        result = _result(**failed, **canceled)

        rendered = str(result)

        assert "failed=[f0, f1, f2, f3, f4 +2 more]" in rendered
        assert "canceled=[c0, c1, c2, c3, c4 +4 more]" in rendered
