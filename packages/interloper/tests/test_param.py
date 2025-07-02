"""This module contains tests for the asset parameter classes."""
import datetime as dt
import os
from unittest.mock import Mock, patch

import pytest

from interloper import errors
from interloper.execution.context import AssetExecutionContext
from interloper.io.base import IO, IOContext
from interloper.param import (
    AssetParam,
    ContextualAssetParam,
    Date,
    DateWindow,
    Env,
    UpstreamAsset,
)
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow


class TestAssetParam:
    """Test the AssetParam class."""

    def test_asset_param_abstract_methods(self):
        """Test that AssetParam is an abstract base class."""
        with pytest.raises(TypeError):
            AssetParam()

    def test_asset_param_concrete_implementation(self):
        """Test concrete implementation of AssetParam."""

        class ConcreteAssetParam(AssetParam[str]):
            def resolve(self) -> str:
                return "test"

        param = ConcreteAssetParam()
        assert param.resolve() == "test"


class TestContextualAssetParam:
    """Test the ContextualAssetParam class."""

    def test_contextual_asset_param_abstract_methods(self):
        """Test that ContextualAssetParam is an abstract base class."""
        with pytest.raises(TypeError):
            ContextualAssetParam()

    def test_contextual_asset_param_concrete_implementation(self):
        """Test concrete implementation of ContextualAssetParam."""

        class ConcreteContextualAssetParam(ContextualAssetParam[str]):
            def resolve(self, context: AssetExecutionContext) -> str:
                return "test"

        param = ConcreteContextualAssetParam()
        context = Mock(spec=AssetExecutionContext)
        assert param.resolve(context) == "test"


class TestEnv:
    """Test the Env class."""

    def test_env_resolve_with_existing_variable(self):
        """Test Env.resolve with existing environment variable."""
        with patch.dict(os.environ, {"TEST_KEY": "test_value"}):
            env_param = Env("TEST_KEY")
            result = env_param.resolve()
            assert result == "test_value"

    def test_env_resolve_with_default(self):
        """Test Env.resolve with default value when variable doesn't exist."""
        with patch.dict(os.environ, {}, clear=True):
            env_param = Env("MISSING_KEY", default="default_value")
            result = env_param.resolve()
            assert result == "default_value"

    def test_env_resolve_without_default_raises_error(self):
        """Test Env.resolve raises error when variable doesn't exist and no default."""
        with patch.dict(os.environ, {}, clear=True):
            env_param = Env("MISSING_KEY")
            with pytest.raises(errors.AssetParamResolutionError, match="Environment variable MISSING_KEY is not set"):
                env_param.resolve()

    def test_env_initialization(self):
        """Test Env initialization."""
        env_param = Env("TEST_KEY", default="default_value")
        assert env_param.key == "TEST_KEY"
        assert env_param.default == "default_value"


class TestUpstreamAsset:
    """Test the UpstreamAsset class."""

    @pytest.fixture
    def mock_context(self):
        """Return a mock asset execution context."""
        context = Mock(spec=AssetExecutionContext)
        context.executed_asset = Mock()
        context.executed_asset.id = "test_asset"
        context.executed_asset.deps = {}
        context.executed_asset.is_partitioned = False
        context.assets = {}
        context.partition = None
        return context

    @pytest.fixture
    def mock_upstream_asset(self):
        """Return a mock upstream asset."""
        asset = Mock()
        asset.id = "upstream_asset"
        asset.has_io = True
        asset.is_partitioned = False
        asset.io = Mock(spec=IO)
        asset.io.read.return_value = "upstream_data"
        return asset

    def test_upstream_asset_resolve_success(self, mock_context, mock_upstream_asset):
        """Test successful UpstreamAsset resolution."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}

        upstream_param = UpstreamAsset("upstream")
        result = upstream_param.resolve(mock_context)

        assert result == "upstream_data"
        mock_upstream_asset.io.read.assert_called_once()

    def test_upstream_asset_resolve_with_type_check(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with type checking."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}

        upstream_param = UpstreamAsset("upstream", type=str)
        result = upstream_param.resolve(mock_context)

        assert result == "upstream_data"

    def test_upstream_asset_resolve_with_type_mismatch(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with type mismatch."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.io.read.return_value = 123  # wrong type

        upstream_param = UpstreamAsset("upstream", type=str)
        with pytest.raises(TypeError, match="Expected data of type str"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_key_not_in_deps(self, mock_context):
        """Test UpstreamAsset resolution when key is not in dependencies."""
        upstream_param = UpstreamAsset("missing")
        with pytest.raises(errors.UpstreamAssetError, match="is not a dependency"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_asset_not_in_context(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution when asset is not in context."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {}  # empty assets

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(errors.UpstreamAssetError, match="is not found among the assets"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_no_io_configured(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution when upstream asset has no IO."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.has_io = False

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(errors.UpstreamAssetError, match="does not have any IO configured"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_with_default_io_key(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with default IO key."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.default_io_key = "default"
        mock_upstream_asset.io = {"default": mock_upstream_asset.io}

        upstream_param = UpstreamAsset("upstream")
        result = upstream_param.resolve(mock_context)

        assert result == "upstream_data"

    def test_upstream_asset_resolve_with_default_io_key_missing(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution when default IO key is missing."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.default_io_key = "missing"
        mock_upstream_asset.io = {"other": mock_upstream_asset.io}

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(errors.UpstreamAssetError, match="does not have an IO configuration for IO key"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_with_multiple_io_no_default(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with multiple IO configs and no default."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.default_io_key = None
        mock_upstream_asset.io = {"io1": Mock(spec=IO), "io2": Mock(spec=IO)}

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(errors.UpstreamAssetError, match="has multiple IO configurations"):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_with_single_io_no_default(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with single IO config and no default."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.default_io_key = None
        mock_upstream_asset.io = {"io1": mock_upstream_asset.io}

        upstream_param = UpstreamAsset("upstream")
        result = upstream_param.resolve(mock_context)

        assert result == "upstream_data"

    def test_upstream_asset_resolve_with_partition(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution with partition."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_context.executed_asset.is_partitioned = True
        mock_upstream_asset.is_partitioned = True
        mock_context.partition = TimePartition(dt.date(2024, 1, 1))

        upstream_param = UpstreamAsset("upstream")
        result = upstream_param.resolve(mock_context)

        assert result == "upstream_data"
        # Verify IOContext was called with partition
        mock_upstream_asset.io.read.assert_called_once()
        call_args = mock_upstream_asset.io.read.call_args[0][0]
        assert isinstance(call_args, IOContext)
        assert call_args.partition == mock_context.partition

    def test_upstream_asset_resolve_non_partitioned_with_partitioned_upstream(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution when non-partitioned asset has partitioned upstream."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_context.executed_asset.is_partitioned = False
        mock_upstream_asset.is_partitioned = True
        mock_context.partition = TimePartition(dt.date(2024, 1, 1))

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(
            errors.UpstreamAssetError, match="non-partitioned asset cannot have a partitioned upstream asset"
        ):
            upstream_param.resolve(mock_context)

    def test_upstream_asset_resolve_io_read_error(self, mock_context, mock_upstream_asset):
        """Test UpstreamAsset resolution when IO read fails."""
        mock_context.executed_asset.deps = {"upstream": mock_upstream_asset}
        mock_context.assets = {"upstream_asset": mock_upstream_asset}
        mock_upstream_asset.io.read.side_effect = Exception("IO error")

        upstream_param = UpstreamAsset("upstream")
        with pytest.raises(errors.UpstreamAssetError, match="Cannot load data from upstream asset"):
            upstream_param.resolve(mock_context)


class TestDate:
    """Test the Date asset parameter."""

    @pytest.fixture
    def mock_context(self):
        """Return a mock asset execution context."""
        context = Mock(spec=AssetExecutionContext)
        context.executed_asset = Mock()
        context.executed_asset.is_partitioned = True
        context.partition = None
        return context

    def test_date_resolve_success(self, mock_context):
        """Test successful Date resolution."""
        mock_context.partition = TimePartition(dt.date(2024, 1, 1))

        date_param = Date()
        result = date_param.resolve(mock_context)

        assert result == dt.date(2024, 1, 1)

    def test_date_resolve_non_partitioned_asset(self, mock_context):
        """Test Date resolution with non-partitioned asset."""
        mock_context.executed_asset.is_partitioned = False

        date_param = Date()
        with pytest.raises(ValueError, match="requires the executed asset to support partitioning"):
            date_param.resolve(mock_context)

    def test_date_resolve_no_partition(self, mock_context):
        """Test Date resolution with no partition."""
        date_param = Date()
        with pytest.raises(ValueError, match="requires the execution context to have a TimePartition"):
            date_param.resolve(mock_context)

    def test_date_resolve_wrong_partition_type(self, mock_context):
        """Test Date resolution with wrong partition type."""
        mock_context.partition = Mock()  # not a TimePartition

        date_param = Date()
        with pytest.raises(ValueError, match="requires the execution context to have a TimePartition"):
            date_param.resolve(mock_context)


class TestDateWindow:
    """Test the DateWindow asset parameter."""

    @pytest.fixture
    def mock_context(self):
        """Return a mock asset execution context."""
        context = Mock(spec=AssetExecutionContext)
        context.executed_asset = Mock()
        context.executed_asset.is_partitioned = True
        context.partition = None
        return context

    def test_date_window_resolve_with_time_partition(self, mock_context):
        """Test DateWindow resolution with TimePartition."""
        mock_context.partition = TimePartition(dt.date(2024, 1, 1))

        date_window_param = DateWindow()
        result = date_window_param.resolve(mock_context)

        assert result == (dt.date(2024, 1, 1), dt.date(2024, 1, 1))

    def test_date_window_resolve_with_time_partition_window(self, mock_context):
        """Test DateWindow resolution with TimePartitionWindow."""
        mock_context.partition = TimePartitionWindow(dt.date(2024, 1, 1), dt.date(2024, 1, 3))

        date_window_param = DateWindow()
        result = date_window_param.resolve(mock_context)

        assert result == (dt.date(2024, 1, 1), dt.date(2024, 1, 3))

    def test_date_window_resolve_non_partitioned_asset(self, mock_context):
        """Test DateWindow resolution with non-partitioned asset."""
        mock_context.executed_asset.is_partitioned = False

        date_window_param = DateWindow()
        with pytest.raises(ValueError, match="requires the executed asset to support partitioning"):
            date_window_param.resolve(mock_context)

    def test_date_window_resolve_no_partition(self, mock_context):
        """Test DateWindow resolution with no partition."""
        date_window_param = DateWindow()
        with pytest.raises(ValueError, match="requires the context to have a TimePartition or TimePartitionWindow"):
            date_window_param.resolve(mock_context)

    def test_date_window_resolve_wrong_partition_type(self, mock_context):
        """Test DateWindow resolution with wrong partition type."""
        mock_context.partition = Mock()  # not a TimePartition or TimePartitionWindow

        date_window_param = DateWindow()
        with pytest.raises(ValueError, match="requires the context to have a TimePartition or TimePartitionWindow"):
            date_window_param.resolve(mock_context)
