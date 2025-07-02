"""This module contains tests for the FileIO class."""
import datetime as dt
import pickle
from pathlib import Path
from unittest.mock import Mock

import pytest

from interloper.io.base import IOContext
from interloper.io.file import FileIO
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import TimePartitionWindow


@pytest.fixture
def asset():
    """Return a mock asset."""
    asset = Mock()
    asset.name = "asset1"
    asset.dataset = None
    return asset


@pytest.fixture
def asset_with_dataset():
    """Return a mock asset with a dataset."""
    asset = Mock()
    asset.name = "asset1"
    asset.dataset = "ds"
    return asset


@pytest.fixture
def partition():
    """Return a mock partition."""
    return Partition(value="part1")


@pytest.fixture
def partition_window():
    """Return a mock partition window."""
    return TimePartitionWindow(dt.date(2024, 1, 1), dt.date(2024, 1, 2))


@pytest.fixture
def file_io(tmp_path):
    """Return a FileIO instance."""
    base_dir = tmp_path / "data"
    base_dir.mkdir()
    return FileIO(str(base_dir))


def test_write_and_read_roundtrip(file_io, asset):
    """Test that data can be written and read back."""
    context = IOContext(asset=asset)
    data = {"foo": 123}
    file_io.write(context, data)
    result = file_io.read(context)
    assert result == data


def test_write_and_read_with_partition(file_io, asset, partition):
    """Test that data can be written and read back with a partition."""
    context = IOContext(asset=asset, partition=partition)
    data = [1, 2, 3]
    file_io.write(context, data)
    result = file_io.read(context)
    assert result == data


def test_write_and_read_with_dataset(file_io, asset_with_dataset):
    """Test that data can be written and read back with a dataset."""
    context = IOContext(asset=asset_with_dataset)
    data = "hello"
    file_io.write(context, data)
    result = file_io.read(context)
    assert result == data


def test_write_and_read_with_dataset_and_partition(file_io, asset_with_dataset, partition):
    """Test that data can be written and read back with a dataset and partition."""
    context = IOContext(asset=asset_with_dataset, partition=partition)
    data = 42
    file_io.write(context, data)
    result = file_io.read(context)
    assert result == data


def test_write_raises_if_base_dir_missing(tmp_path, asset):
    """Test that write raises an error if the base directory is missing."""
    # Don't create the base dir
    base_dir = tmp_path / "missing"
    fileio = FileIO(str(base_dir))
    context = IOContext(asset=asset)
    with pytest.raises(FileNotFoundError):
        fileio.write(context, "data")


def test_write_raises_on_partition_window(file_io, asset, partition_window):
    """Test that write raises an error on a partition window."""
    context = IOContext(asset=asset, partition=partition_window)
    with pytest.raises(RuntimeError, match="Partition windows are not supported"):
        file_io.write(context, "data")


def test_read_raises_on_partition_window(file_io, asset, partition_window):
    """Test that read raises an error on a partition window."""
    context = IOContext(asset=asset, partition=partition_window)
    with pytest.raises(RuntimeError, match="Partition windows are not supported"):
        file_io.read(context)


def test_file_content_is_pickled(file_io, asset):
    """Test that the file content is pickled."""
    context = IOContext(asset=asset)
    data = {"foo": "bar"}
    file_io.write(context, data)
    # Check the file content is pickled
    path = Path(file_io.folder) / asset.name
    with open(path, "rb") as f:
        raw = f.read()
    assert pickle.loads(raw) == data
