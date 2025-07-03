"""This module contains tests for the date asset parameter classes."""

import datetime as dt
from unittest.mock import Mock

import pytest

from interloper.execution.context import AssetExecutionContext
from interloper.params.date import Date, DateWindow
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow


class TestDate:
    """Test the Date class."""

    @pytest.fixture
    def mock_context(self):
        """Return a mock asset execution context."""
        context = Mock(spec=AssetExecutionContext)
        context.executed_asset = Mock()
        context.executed_asset.is_partitioned = True
        context.partition = TimePartition(dt.date(2023, 1, 1))
        return context

    def test_date_resolve_success(self, mock_context):
        """Test successful Date resolution."""
        date_param = Date()
        result = date_param.resolve(mock_context)
        assert result == dt.date(2023, 1, 1)

    def test_date_resolve_non_partitioned_asset(self, mock_context):
        """Test Date resolution with non-partitioned asset."""
        mock_context.executed_asset.is_partitioned = False
        date_param = Date()
        with pytest.raises(
            ValueError, match="Asset param of type Date requires the executed asset to support partitioning"
        ):
            date_param.resolve(mock_context)

    def test_date_resolve_no_partition(self, mock_context):
        """Test Date resolution with no partition."""
        mock_context.partition = None
        date_param = Date()
        with pytest.raises(
            ValueError, match="Asset param of type Date requires the execution context to have a TimePartition"
        ):
            date_param.resolve(mock_context)

    def test_date_resolve_wrong_partition_type(self, mock_context):
        """Test Date resolution with wrong partition type."""
        mock_context.partition = Mock()  # Not a TimePartition
        date_param = Date()
        with pytest.raises(
            ValueError, match="Asset param of type Date requires the execution context to have a TimePartition"
        ):
            date_param.resolve(mock_context)


class TestDateWindow:
    """Test the DateWindow class."""

    @pytest.fixture
    def mock_context(self):
        """Return a mock asset execution context."""
        context = Mock(spec=AssetExecutionContext)
        context.executed_asset = Mock()
        context.executed_asset.is_partitioned = True
        context.partition = TimePartition(dt.date(2023, 1, 1))
        return context

    def test_date_window_resolve_with_time_partition(self, mock_context):
        """Test DateWindow resolution with TimePartition."""
        date_window_param = DateWindow()
        result = date_window_param.resolve(mock_context)
        assert result == (dt.date(2023, 1, 1), dt.date(2023, 1, 1))

    def test_date_window_resolve_with_time_partition_window(self, mock_context):
        """Test DateWindow resolution with TimePartitionWindow."""
        mock_context.partition = TimePartitionWindow(dt.date(2023, 1, 1), dt.date(2023, 1, 7))
        date_window_param = DateWindow()
        result = date_window_param.resolve(mock_context)
        assert result == (dt.date(2023, 1, 1), dt.date(2023, 1, 7))

    def test_date_window_resolve_non_partitioned_asset(self, mock_context):
        """Test DateWindow resolution with non-partitioned asset."""
        mock_context.executed_asset.is_partitioned = False
        date_window_param = DateWindow()
        with pytest.raises(
            ValueError, match="Asset param of type DateWindow requires the executed asset to support partitioning"
        ):
            date_window_param.resolve(mock_context)

    def test_date_window_resolve_no_partition(self, mock_context):
        """Test DateWindow resolution with no partition."""
        mock_context.partition = None
        date_window_param = DateWindow()
        with pytest.raises(
            ValueError,
            match="Asset param of type DateWindow requires the context to have a TimePartition or TimePartitionWindow",
        ):
            date_window_param.resolve(mock_context)

    def test_date_window_resolve_wrong_partition_type(self, mock_context):
        """Test DateWindow resolution with wrong partition type."""
        mock_context.partition = Mock()  # Not a TimePartition or TimePartitionWindow
        date_window_param = DateWindow()
        with pytest.raises(
            ValueError,
            match="Asset param of type DateWindow requires the context to have a TimePartition or TimePartitionWindow",
        ):
            date_window_param.resolve(mock_context)
