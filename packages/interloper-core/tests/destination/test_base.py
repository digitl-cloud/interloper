"""Tests for ``interloper.destination.base``."""

# Note: no ``from __future__ import annotations``: an annotation naming a
# component class declares a relation, and the collector needs it as a real
# class, not a lazy string.

import datetime
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.base import DestinationDefinition
from interloper.partitioning.base import Partition
from interloper.partitioning.time import TimePartition, TimePartitionWindow


class FakeConnection(il.Connection):
    """Connection fixture used as a destination's relation target."""

    token: str = il.SecretField(default="")

    @il.fetch_field_provider
    def datasets(self) -> list[dict[str, str]]:
        """List the datasets the credentials can reach.

        Returns:
            One entry per dataset, as the fetch field's option payload.
        """
        return [{"id": "one", "name": "One"}]

    def not_a_provider(self) -> list[dict[str, str]]:
        """Look like a provider without being marked as one.

        Returns:
            An empty option list.
        """
        return []


class FakeDestination(il.MemoryDestination):
    """Destination fixture declaring its connection as an annotation."""

    connection: FakeConnection


class TestDefinition:
    def test_kind_and_registration(self):
        assert il.Destination.kind == "destination"
        assert il.KINDS.get("destination") is il.Destination

    def test_definition_returns_destination_definition(self):
        assert isinstance(FakeDestination.definition(), DestinationDefinition)

    def test_anchor_declares_no_relation(self):
        assert il.Destination.relations == {}

    def test_annotation_declares_a_relation(self):
        relation = FakeDestination.relations["connection"]
        assert (relation.kind, relation.key, relation.target) == ("connection", "fake_connection", FakeConnection)

    def test_relations_reach_the_definition(self):
        assert FakeDestination.definition().relations["connection"].target is FakeConnection

    def test_a_relation_is_not_a_config_field(self):
        assert "connection" not in FakeDestination.definition().config_schema.get("properties", {})


class TestFetchProviderValidation:
    """``FetchField(provider=...)`` resolves through the declared relations."""

    def test_provider_on_a_declared_relation_is_accepted(self):
        class Valid(il.MemoryDestination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="connection.datasets", value_key="id")

        fetch = Valid.definition().config_schema["properties"]["dataset"]["x-fetch"]
        assert fetch["provider"] == "connection.datasets"

    def test_provider_on_an_undeclared_relation_is_rejected(self):
        class Undeclared(il.MemoryDestination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="other.datasets")

        with pytest.raises(TypeError, match="not declared"):
            Undeclared.definition()

    def test_provider_naming_an_unmarked_method_is_rejected(self):
        class Unmarked(il.MemoryDestination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="connection.not_a_provider")

        with pytest.raises(TypeError, match="not a @fetch_field_provider"):
            Unmarked.definition()


# -- Partition dispatch ----------------------------------------------------------

class RecordingPartitions(il.Destination):
    """Destination capturing every partition-hook call."""

    calls: ClassVar[list[tuple[str, Any, Any]]] = []

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "calls", [])

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        self.calls.append(("write", partition.id if partition else None, data))

    def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
        self.calls.append(("read", partition.id if partition else None, None))
        return {"partition": partition.id if partition else None}

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        return {}


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


def io_context(asset: il.Asset, partition_or_window=None) -> IOContext:  # noqa: D103
    return IOContext(asset=asset, partition_or_window=partition_or_window)


class TestWriteDispatch:
    """The three-way write dispatch with window splitting."""

    def test_unpartitioned_write_is_one_call(self):
        destination = RecordingPartitions(id="d")
        destination.write(io_context(plain_asset()), [{"a": 1}])
        assert destination.calls == [("write", None, [{"a": 1}])]

    def test_partition_write_passes_data_unsplit(self):
        destination = RecordingPartitions(id="d")
        rows = [{"date": "2024-01-01"}, {"date": "2024-01-02"}]
        destination.write(io_context(partitioned_asset(), TimePartition(datetime.date(2024, 1, 1))), rows)
        assert destination.calls == [("write", "2024-01-01", rows)]

    def test_window_write_splits_per_partition(self):
        destination = RecordingPartitions(id="d")
        rows = [
            {"date": "2024-01-01", "v": 1},
            {"date": "2024-01-02", "v": 2},
            {"date": "2024-01-02", "v": 3},
        ]
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        destination.write(io_context(partitioned_asset(), window), rows)
        by_partition = {partition: data for kind, partition, data in destination.calls}
        assert by_partition["2024-01-01"] == [{"date": "2024-01-01", "v": 1}]
        assert by_partition["2024-01-02"] == [{"date": "2024-01-02", "v": 2}, {"date": "2024-01-02", "v": 3}]

    def test_monthly_window_slices_rows_by_period(self):
        # Rows carry daily dates; each monthly partition's slice is its whole
        # month, which id equality on the period start would miss entirely.
        @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH))
        def monthly(context: il.ExecutionContext) -> list:
            return []

        destination = RecordingPartitions(id="d")
        rows = [
            {"date": "2024-01-15", "v": 1},
            {"date": "2024-02-10", "v": 2},
            {"date": "2024-02-20", "v": 3},
        ]
        window = TimePartitionWindow(
            datetime.date(2024, 1, 1), datetime.date(2024, 2, 1), il.TimeGranularity.MONTH
        )
        destination.write(io_context(monthly(), window), rows)
        by_partition = {partition: data for kind, partition, data in destination.calls}
        assert by_partition["2024-01"] == [{"date": "2024-01-15", "v": 1}]
        assert by_partition["2024-02"] == [{"date": "2024-02-10", "v": 2}, {"date": "2024-02-20", "v": 3}]

    def test_window_write_splits_dataframes_natively(self):
        pd = pytest.importorskip("pandas")

        destination = RecordingPartitions(id="d")
        df = pd.DataFrame([{"date": "2024-01-01", "v": 1}, {"date": "2024-01-02", "v": 2}])
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        destination.write(io_context(partitioned_asset(), window), df)
        for _, _, data in destination.calls:
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 1

    def test_window_write_refuses_unsplittable_data(self):
        from interloper.errors import RepresentationError

        destination = RecordingPartitions(id="d")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 1))
        with pytest.raises(RepresentationError, match="object"):
            destination.write(io_context(partitioned_asset(), window), object())
        assert destination.calls == []


class TestReadDispatch:
    """The three-way read dispatch."""

    def test_unpartitioned_read(self):
        assert RecordingPartitions(id="d").read(io_context(plain_asset())) == {"partition": None}

    def test_partition_read(self):
        partition = TimePartition(datetime.date(2024, 1, 2))
        result = RecordingPartitions(id="d").read(io_context(partitioned_asset(), partition))
        assert result == {"partition": "2024-01-02"}

    def test_window_read_returns_one_result_per_partition(self):
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        result = RecordingPartitions(id="d").read(io_context(partitioned_asset(), window))
        assert {r["partition"] for r in result} == {"2024-01-01", "2024-01-02"}


class TestHookContract:
    """The three partition hooks are the contract, enforced at instantiation."""

    def test_a_destination_without_the_hooks_cannot_be_instantiated(self):
        class Bare(il.Destination):
            pass

        with pytest.raises(TypeError, match="abstract.*(partition_row_counts|read_partition|write_partition)"):
            Bare(id="b")

    def test_the_three_hooks_are_the_whole_contract(self):
        class Minimal(il.Destination):
            def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
                pass

            def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
                return []

            def partition_row_counts(self, context: IOContext) -> dict[str, int]:
                return {}

        assert Minimal(id="m").read(io_context(plain_asset())) == []
