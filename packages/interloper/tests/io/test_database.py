"""This module contains tests for the DatabaseIO class."""
from unittest.mock import MagicMock, Mock

import pytest

from interloper.execution.strategy import MaterializationStrategy
from interloper.io.base import IOContext
from interloper.io.database import DatabaseClient, DatabaseIO
from interloper.partitioning.config import PartitionConfig
from interloper.partitioning.partition import Partition
from tests.fixtures import handler, handler_with_reconciler, reconciler, schema  # noqa: F401


@pytest.fixture
def asset(schema):
    """Return a mock asset."""
    asset = MagicMock()
    asset.name = "asset1"
    asset.data_type = str
    asset.schema = schema
    asset.dataset = "ds"
    asset.partitioning = PartitionConfig(column="foo")
    asset.materialization_strategy = MaterializationStrategy.FLEXIBLE
    return asset


@pytest.fixture
def context(asset):
    """Return a mock IO context."""
    return IOContext(asset=asset)


@pytest.fixture
def database_client():
    """Return a mock database client."""
    client = Mock(spec=DatabaseClient)
    client.table_exists.return_value = False
    client.table_schema.return_value = {"foo": "VARCHAR", "bar": "INTEGER"}
    return client


@pytest.fixture
def database_io(database_client, handler):
    """Return a mock database IO."""
    return DatabaseIO(database_client, [handler])


def test_write_creates_table_if_not_exists(database_io, database_client, context):
    """Test that write creates the table if it does not exist."""
    database_io.write(context, "data")
    database_client.create_table.assert_called_once_with(
        context.asset.name,
        context.asset.schema,
        context.asset.dataset,
        context.asset.partitioning,
    )


def test_write_deletes_partition_if_present(database_io, database_client, context):
    """Test that write deletes the partition if it is present."""
    partition = Partition(value="part1")
    context = IOContext(asset=context.asset, partition=partition)
    database_io.write(context, "data")
    database_client.delete_partition.assert_called_once_with(
        context.asset.name,
        context.asset.partitioning.column,
        partition,
        context.asset.dataset,
    )


def test_write_raises_if_schema_missing(database_io, context):
    """Test that write raises an error if the schema is missing."""
    context.asset.schema = None
    with pytest.raises(RuntimeError, match="Schema is required"):
        database_io.write(context, "data")


def test_write_reconciles_if_flexible_and_reconciler(database_client, context, handler_with_reconciler):
    """Test that write reconciles if the strategy is flexible and a reconciler is present."""
    database_io = DatabaseIO(database_client, [handler_with_reconciler])
    database_io.write(context, "data")
    # The handler should have received the reconciled data
    assert handler_with_reconciler._written[1] == "reconciled_data"


def test_write_skips_reconcile_if_no_reconciler_and_flexible(database_io, handler, context, caplog):
    """Test that write skips reconciliation if no reconciler is present and the strategy is flexible."""
    context.asset.materialization_strategy = MaterializationStrategy.FLEXIBLE
    with caplog.at_level("WARNING"):
        database_io.write(context, "data")
    assert any("No reconciler found" in m for m in caplog.messages)


def test_write_warns_if_strict(database_io, handler, context, caplog):
    """Test that write warns if the strategy is strict."""
    context.asset.materialization_strategy = MaterializationStrategy.STRICT
    with caplog.at_level("WARNING"):
        database_io.write(context, "data")
    assert any("<STRICT> Skipping schema reconciliation" in m for m in caplog.messages)


def test_read_calls_handler_and_verifies_type(database_io, handler, context):
    """Test that read calls the handler and verifies the type."""
    handler._read = "handler_data"
    result = database_io.read(context)
    assert result == "handler_data"


def test_read_raises_if_handler_returns_wrong_type(database_io, handler, context):
    """Test that read raises an error if the handler returns the wrong type."""
    handler._read = 123  # not a str
    with pytest.raises(ValueError, match="Data type int is not supported"):
        database_io.read(context)


def test_write_propagates_handler_error(database_io, handler, context):
    """Test that write propagates handler errors."""
    def fail_write(context, data):
        raise Exception("fail")

    handler.write = fail_write
    with pytest.raises(Exception, match="fail"):
        database_io.write(context, "data")


def test_read_propagates_handler_error(database_io, handler, context):
    """Test that read propagates handler errors."""
    def fail_read(context):
        raise Exception("fail_read")

    handler.read = fail_read
    with pytest.raises(Exception, match="fail_read"):
        database_io.read(context)
