"""Tests for ``interloper.destination.file``."""

import datetime
import pickle
from pathlib import Path

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.destination.file import FileDestination
from interloper.partitioning.time import TimePartition


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def partitioned_asset(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def plain_asset() -> list:  # noqa: D103
    return []


class TestRoundtrip:
    """Pickled write/read roundtrips."""

    def test_unpartitioned_roundtrip(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        context = IOContext(asset=plain_asset())

        dest.write(context, [{"a": 1}])

        assert dest.read(context) == [{"a": 1}]

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

    def test_partitions_share_one_file(self, tmp_path: Path):
        # FileDestination extends Destination, not PartitionedDestination, so
        # the scope never reaches the path: two partitions overwrite each other.
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        first = IOContext(asset=asset, partition_or_window=TimePartition(datetime.date(2024, 1, 1)))
        second = IOContext(asset=asset, partition_or_window=TimePartition(datetime.date(2024, 1, 2)))

        dest.write(first, [{"date": "2024-01-01"}])
        dest.write(second, [{"date": "2024-01-02"}])

        assert dest.read(first) == [{"date": "2024-01-02"}]


class TestPathLayout:
    """Where the data file lands on disk."""

    def test_dataset_and_table_become_directories(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = plain_asset()
        asset.dataset = "analytics"

        dest.write(IOContext(asset=asset), [{"a": 1}])

        assert (tmp_path / "analytics" / asset.table / "data.pkl").is_file()

    def test_a_datasetless_asset_falls_back_to_its_table(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = plain_asset()
        asset.dataset = ""

        dest.write(IOContext(asset=asset), [{"a": 1}])

        assert (tmp_path / asset.table / asset.table / "data.pkl").is_file()


class TestMissingData:
    """Reads of data that was never written."""

    def test_read_names_the_missing_path(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))

        with pytest.raises(FileNotFoundError, match="No data file for"):
            dest.read(IOContext(asset=plain_asset()))


class TestPartitionRowCounts:
    """Row counts scanned back off a hive-partitioned tree.

    ``write`` above produces a single unpartitioned file, so these layouts
    are built by hand — this method reads a tree some other writer produced.
    """

    @staticmethod
    def _hive(tmp_path: Path, asset, **partitions: object) -> Path:
        base = tmp_path / (asset.dataset or asset.table) / asset.table
        for value, data in partitions.items():
            directory = base / f"date={value.replace('_', '-')}"
            directory.mkdir(parents=True)
            (directory / "data.pkl").write_bytes(pickle.dumps(data))
        base.mkdir(parents=True, exist_ok=True)
        return base

    def test_counts_rows_per_partition_directory(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        self._hive(tmp_path, asset, **{"2024_01_01": [{"v": 1}, {"v": 2}], "2024_01_02": [{"v": 3}]})

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 2, "2024-01-02": 1}

    def test_a_non_list_payload_counts_as_one_row(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        self._hive(tmp_path, asset, **{"2024_01_01": {"v": 1}})

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 1}

    def test_nothing_written_yet_is_empty(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))

        assert dest.partition_row_counts(IOContext(asset=partitioned_asset())) == {}

    def test_unrelated_and_empty_directories_are_ignored(self, tmp_path: Path):
        dest = FileDestination(id="file", base_path=str(tmp_path))
        asset = partitioned_asset()
        base = self._hive(tmp_path, asset, **{"2024_01_01": [{"v": 1}]})
        (base / "not_a_partition").mkdir()
        (base / "date=2024-01-09").mkdir()
        (base / "data.pkl").write_bytes(pickle.dumps([{"v": 9}]))

        counts = dest.partition_row_counts(IOContext(asset=asset))

        assert counts == {"2024-01-01": 1}
