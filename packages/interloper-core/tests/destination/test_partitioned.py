"""Tests for ``interloper.destination.partitioned``."""

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext, PartitionedDestination
from interloper.partitioning.base import Partition
from interloper.partitioning.time import TimePartition, TimePartitionWindow


class RecordingScopes(PartitionedDestination):
    """Destination capturing every scope-hook call."""

    calls: ClassVar[list[tuple[str, Any, Any]]] = []

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "calls", [])

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        self.calls.append(("write", partition.id if partition else None, data))

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        self.calls.append(("read", partition.id if partition else None, None))
        return {"scope": partition.id if partition else None}


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


def ctx(asset: il.Asset, scope=None) -> IOContext:  # noqa: D103
    return IOContext(asset=asset, partition_or_window=scope)


class TestWriteDispatch:
    """The three-way write dispatch with window splitting."""

    def test_unpartitioned_write_is_one_scope(self):
        dest = RecordingScopes(id="d")
        dest.write(ctx(plain_asset()), [{"a": 1}])
        assert dest.calls == [("write", None, [{"a": 1}])]

    def test_partition_write_passes_data_unsplit(self):
        dest = RecordingScopes(id="d")
        rows = [{"date": "2024-01-01"}, {"date": "2024-01-02"}]
        dest.write(ctx(partitioned_asset(), TimePartition(datetime.date(2024, 1, 1))), rows)
        assert dest.calls == [("write", "2024-01-01", rows)]

    def test_window_write_splits_per_partition(self):
        dest = RecordingScopes(id="d")
        rows = [
            {"date": "2024-01-01", "v": 1},
            {"date": "2024-01-02", "v": 2},
            {"date": "2024-01-02", "v": 3},
        ]
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        dest.write(ctx(partitioned_asset(), window), rows)
        by_scope = {scope: data for kind, scope, data in dest.calls}
        assert by_scope["2024-01-01"] == [{"date": "2024-01-01", "v": 1}]
        assert by_scope["2024-01-02"] == [{"date": "2024-01-02", "v": 2}, {"date": "2024-01-02", "v": 3}]

    def test_window_write_splits_dataframes_natively(self):
        pd = pytest.importorskip("pandas")

        dest = RecordingScopes(id="d")
        df = pd.DataFrame([{"date": "2024-01-01", "v": 1}, {"date": "2024-01-02", "v": 2}])
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        dest.write(ctx(partitioned_asset(), window), df)
        for _, _, data in dest.calls:
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 1

    def test_window_write_passes_unsplittable_data_as_is(self):
        dest = RecordingScopes(id="d")
        sentinel = object()
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 1))
        dest.write(ctx(partitioned_asset(), window), sentinel)
        assert dest.calls == [("write", "2024-01-01", sentinel)]


class TestReadDispatch:
    """The three-way read dispatch."""

    def test_unpartitioned_read(self):
        assert RecordingScopes(id="d").read(ctx(plain_asset())) == {"scope": None}

    def test_partition_read(self):
        result = RecordingScopes(id="d").read(ctx(partitioned_asset(), TimePartition(datetime.date(2024, 1, 2))))
        assert result == {"scope": "2024-01-02"}

    def test_window_read_returns_one_result_per_partition(self):
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        result = RecordingScopes(id="d").read(ctx(partitioned_asset(), window))
        assert {r["scope"] for r in result} == {"2024-01-01", "2024-01-02"}


class TestHookContract:
    """Hooks are required unless the corresponding template is overridden."""

    def test_missing_hooks_raise_with_guidance(self):
        class Bare(PartitionedDestination):
            pass

        with pytest.raises(NotImplementedError, match="_write_scope.*or override write"):
            Bare(id="b").write(ctx(plain_asset()), [])
        with pytest.raises(NotImplementedError, match="_read_scope.*or override read"):
            Bare(id="b").read(ctx(plain_asset()))
