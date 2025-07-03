"""This module contains tests for the upstream asset parameter classes."""
import datetime as dt
from unittest.mock import Mock

import pytest

from interloper import errors
from interloper.execution.context import AssetExecutionContext
from interloper.io.base import IO, IOContext
from interloper.params.upstream_asset import UpstreamAsset
from interloper.partitioning.partition import TimePartition


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