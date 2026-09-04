"""Tests for ``interloper.destination.file``."""

import datetime
from pathlib import Path

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.file import FileDestination
from interloper.partitioning.time import TimePartition, TimePartitionWindow


@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


def _partition(day: int) -> TimePartition:
    return TimePartition(datetime.date(2024, 1, day))


class TestRoundtrip:
    """Pickled write/read roundtrips."""

    def test_unpartitioned_roundtrip(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        context = IOContext(asset=plain_asset())

        dest.write(context, [{"a": 1}])

        assert dest.read(context) == [{"a": 1}]

    def test_partitioned_roundtrip(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        context = IOContext(asset=asset, partition_or_window=_partition(1))

        dest.write(context, [{"date": "2024-01-01", "v": 1}])

        assert dest.read(context) == [{"date": "2024-01-01", "v": 1}]

    def test_each_partition_keeps_its_own_data(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        first = IOContext(asset=asset, partition_or_window=_partition(1))
        second = IOContext(asset=asset, partition_or_window=_partition(2))

        dest.write(first, [{"date": "2024-01-01"}])
        dest.write(second, [{"date": "2024-01-02"}])

        assert dest.read(first) == [{"date": "2024-01-01"}]
        assert dest.read(second) == [{"date": "2024-01-02"}]

    def test_arbitrary_python_objects_survive(self, tmp_path: Path):
        # Pickle, not a tabular format: whatever the asset returned comes back.
        dest = FileDestination(id="file", base_path=str(tmp_path))
        context = IOContext(asset=plain_asset())
        payload = {"nested": [datetime.date(2024, 1, 1), {1, 2}]}

        dest.write(context, payload)

        assert dest.read(context) == payload

    def test_a_rewrite_replaces_the_previous_data(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        context = IOContext(asset=plain_asset())

        dest.write(context, [{"a": 1}])
        dest.write(context, [{"a": 2}])

        assert dest.read(context) == [{"a": 2}]


class TestWindows:
    """Window writes are split per partition by ``PartitionedDestination``."""

    def test_tabular_rows_are_split_by_the_partition_column(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [
            {"date": "2024-01-01", "v": 1},
            {"date": "2024-01-02", "v": 2},
            {"date": "2024-01-02", "v": 3},
        ]

        dest.write(IOContext(asset=asset, partition_or_window=window), rows)

        assert dest.read(IOContext(asset=asset, partition_or_window=_partition(1))) == [
            {"date": "2024-01-01", "v": 1}
        ]
        assert dest.read(IOContext(asset=asset, partition_or_window=_partition(2))) == [
            {"date": "2024-01-02", "v": 2},
            {"date": "2024-01-02", "v": 3},
        ]

    def test_a_window_read_returns_one_result_per_partition(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        dest.write(IOContext(asset=asset, partition_or_window=window), [{"date": "2024-01-01", "v": 1}])
        dest.write(IOContext(asset=asset, partition_or_window=_partition(2)), [{"date": "2024-01-02", "v": 2}])

        pages = dest.read(IOContext(asset=asset, partition_or_window=window))

        assert pages == [[{"date": "2024-01-02", "v": 2}], [{"date": "2024-01-01", "v": 1}]]

    def test_a_non_tabular_payload_is_stored_whole_under_each_partition(self, tmp_path: Path):
        # No representation can slice an arbitrary object, so it passes through
        # the split unchanged rather than being dropped.
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        payload = {"blob": object.__doc__}

        dest.write(IOContext(asset=asset, partition_or_window=window), payload)

        assert dest.read(IOContext(asset=asset, partition_or_window=_partition(1))) == payload
        assert dest.read(IOContext(asset=asset, partition_or_window=_partition(2))) == payload


class TestPathLayout:
    """Where the data file lands on disk."""

    def test_dataset_and_table_become_directories(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = plain_asset()
        asset.dataset = "analytics"

        dest.write(IOContext(asset=asset), [{"a": 1}])

        assert (tmp_path / "analytics" / asset.table / "data.pkl").is_file()

    def test_a_datasetless_asset_gets_no_dataset_segment(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = plain_asset()
        asset.dataset = ""

        dest.write(IOContext(asset=asset), [{"a": 1}])

        assert (tmp_path / asset.table / "data.pkl").is_file()

    def test_a_partition_adds_a_hive_subdirectory(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()

        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), [{"a": 1}])

        expected = tmp_path / asset.dataset / asset.table / "date=2024-01-01" / "data.pkl"
        assert expected.is_file()


class TestMissingData:
    """Reads of data that was never written."""

    def test_read_names_the_missing_path(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))

        with pytest.raises(FileNotFoundError, match="No data file for"):
            dest.read(IOContext(asset=plain_asset()))

    def test_an_unwritten_partition_is_missing_on_its_own(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), [{"a": 1}])

        with pytest.raises(FileNotFoundError, match="date=2024-01-02"):
            dest.read(IOContext(asset=asset, partition_or_window=_partition(2)))


class TestPartitionRowCounts:
    """Row counts scanned back off the tree ``write`` produced."""

    def test_counts_rows_per_partition(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), [{"v": 1}, {"v": 2}])
        dest.write(IOContext(asset=asset, partition_or_window=_partition(2)), [{"v": 3}])

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 2, "2024-01-02": 1}

    def test_a_non_list_payload_counts_as_one_row(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), {"v": 1})

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 1}

    def test_nothing_written_yet_is_empty(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))

        assert dest.partition_row_counts(IOContext(asset=partitioned_asset())) == {}

    def test_the_unpartitioned_file_is_not_counted(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), [{"v": 1}])
        dest.write(IOContext(asset=asset), [{"v": 9}])

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 1}

    def test_unrelated_and_empty_directories_are_ignored(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        dest.write(IOContext(asset=asset, partition_or_window=_partition(1)), [{"v": 1}])
        base = tmp_path / asset.dataset / asset.table
        (base / "not_a_partition").mkdir()
        (base / "date=2024-01-09").mkdir()

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 1}
