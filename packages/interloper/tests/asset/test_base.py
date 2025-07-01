"""This module contains tests for the Asset class."""
from inspect import Parameter, signature
from typing import Any
from unittest.mock import Mock, patch

import pytest

import interloper as itlp

from ..fixtures import asset, asset_param, contextual_asset_param, io, normalizer, source  # noqa: F401


class TestAssetDefinition:
    """Test asset definitions."""

    def test_abstract_instance_fails(self):
        """Test that instantiating an abstract asset fails."""
        with pytest.raises(TypeError):
            itlp.Asset()

    def test_definition_from_class(self):
        """Test asset definition from a class."""

        class MyAsset(itlp.Asset):
            def data(self): ...

        my_asset = MyAsset(name="my_asset")
        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_class_name_required(self):
        """Test that the name is required when defining an asset from a class."""

        class MyAsset(itlp.Asset):
            def data(self): ...

        with pytest.raises(TypeError):
            MyAsset()

    def test_definition_from_decorator(self):
        """Test asset definition from a decorator."""

        @itlp.asset
        def my_asset(): ...

        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_decorator_with_options(self):
        """Test asset definition from a decorator with options."""

        @itlp.asset(name="new_name")
        def my_asset(): ...

        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "new_name"

    def test_hash(self):
        """Test that the asset hash is correct."""

        @itlp.asset
        def my_asset(): ...

        assert hash(my_asset) == hash("my_asset")


class TestAssetProperties:
    """Test asset properties."""

    def test_id(self, asset: itlp.Asset):
        """Test the asset ID."""
        assert asset.id == "asset"

    def test_id_with_dataset(self, asset: itlp.Asset):
        """Test the asset ID with a dataset."""
        asset.dataset = "dataset"
        assert asset.id == "dataset.asset"

    def test_id_with_dataset_from_source(self, source: itlp.Source):
        """Test the asset ID with a dataset from a source."""
        assert source.asset.id == "source.asset"

    def test_dataset(self, asset: itlp.Asset):
        """Test the asset dataset."""
        asset.dataset = "dataset"
        assert asset.dataset == "dataset"

    def test_dataset_from_source(self, source: itlp.Source):
        """Test the asset dataset from a source."""
        assert source.asset.dataset == "source"

        source.dataset = "something"
        assert source.asset.dataset == "something"

    def test_io(self, asset: itlp.Asset, io: itlp.IO):
        """Test the asset IO."""
        asset.io = io
        assert asset.io == io

        asset.io = {"foo": io}
        assert asset.io == {"foo": io}

    def test_io_from_source(self, source: itlp.Source, io: io):
        """Test the asset IO from a source."""
        source.io = io
        assert source.asset.io == io

        source.io = {"foo": io}
        assert source.asset.io == {"foo": io}

    def test_default_io_key(self, asset: itlp.Asset):
        """Test the asset default IO key."""
        asset.default_io_key = "whatever"
        assert asset.default_io_key == "whatever"

    def test_default_io_key_from_source(self, source: itlp.Source):
        """Test the asset default IO key from a source."""
        source.default_io_key = "whatever"
        assert source.asset.default_io_key == "whatever"

    def test_normalizer(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        """Test the asset normalizer."""
        asset.normalizer = normalizer
        assert asset.normalizer == normalizer

    def test_normalizer_from_source(self, source: itlp.Source):
        """Test the asset normalizer from a source."""
        source.normalizer = normalizer
        assert source.asset.normalizer == normalizer

    def test_normalizer_setter_fails(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        """Test that the normalizer setter fails with an invalid value."""
        with pytest.raises(itlp.errors.AssetValueError, match="Normalizer must be an instance of Normalizer, got str"):
            asset.normalizer = "normalizer"

    def test_materializable_default(self, asset: itlp.Asset):
        """Test that the asset is materializable by default."""
        assert asset.materializable

    def test_materializable_false(self, asset: itlp.Asset):
        """Test that the asset can be set to not materializable."""
        asset.materializable = False
        assert not asset.materializable

    def test_materializable_from_source(self, source: itlp.Source):
        """Test that the materializable property is inherited from the source."""
        source.materializable = False
        assert not source.asset.materializable

    def test_has_io(self, asset: itlp.Asset):
        """Test that has_io is true when IO is configured."""
        asset.io = {"what": "ever"}
        assert asset.has_io

    def test_has_io_false(self, asset: itlp.Asset):
        """Test that has_io is false when no IO is configured."""
        assert not asset.has_io

    def test_allows_partition_window(self, asset: itlp.Asset):
        """Test that allows_partition_window is true when configured."""
        asset.partitioning = itlp.TimePartitionConfig(column="date", allow_window=True)
        assert asset.allows_partition_window

    def test_allows_partition_window_false(self, asset: itlp.Asset):
        """Test that allows_partition_window is false when not configured."""
        assert not asset.allows_partition_window

        asset.partitioning = itlp.PartitionConfig(column="date")
        assert not asset.allows_partition_window

        asset.partitioning = itlp.TimePartitionConfig(column="date")
        assert not asset.allows_partition_window

    def test_materialization_strategy(self, asset: itlp.Asset):
        """Test the materialization strategy."""
        assert asset.materialization_strategy == itlp.MaterializationStrategy.FLEXIBLE

        asset.materialization_strategy = itlp.MaterializationStrategy.STRICT
        assert asset.materialization_strategy == itlp.MaterializationStrategy.STRICT

    def test_materialization_strategy_from_source(self, source: itlp.Source):
        """Test that the materialization strategy is inherited from the source."""
        source.materialization_strategy = itlp.MaterializationStrategy.STRICT
        assert source.asset.materialization_strategy == itlp.MaterializationStrategy.STRICT

    def test_data_type(self, asset: itlp.Asset):
        """Test the data type of the asset."""
        assert asset.data_type is str

    def test_data_type_no_return_type(self):
        """Test the data type when no return type is specified."""

        @itlp.asset
        def asset(who: str):
            pass

        assert asset.data_type is None


class TestAssetCall:
    """Test the asset call method."""

    def test_call(self, asset: itlp.Asset):
        """Test calling an asset."""
        asset.io = {"what": "ever"}
        asset.default_io_key = "whatever"

        copy = asset()

        assert id(copy) != id(asset)
        assert copy.io == {"what": "ever"}
        assert copy.default_io_key == "whatever"

    def test_call_with_options(self, asset: itlp.Asset):
        """Test calling an asset with options."""
        asset.io = {"what": "ever"}
        asset.default_io_key = "whatever"

        copy = asset(
            who="world",
            io={"something": "else"},
            default_io_key="something",
        )

        assert copy.io == {"something": "else"}
        assert copy.default_io_key == "something"
        assert signature(copy.data).parameters["who"].default == "world"


class TestAssetRun:
    """Test the asset run method."""

    def test_run(self):
        """Test running an asset."""

        @itlp.asset
        def my_asset():
            return "data"

        assert my_asset.run() == "data"

    def test_run_with_param(self, asset: itlp.Asset):
        """Test running an asset with a parameter."""
        assert asset.run(who="world") == "hello world"

    def test_run_with_param_without_kw_fails(self, asset: itlp.Asset):
        """Test that running an asset with a positional argument fails."""
        with pytest.raises(itlp.errors.AssetParamResolutionError, match="Cannot resolve parameter who for asset asset"):
            asset.run("world")

    def test_run_with_type_check_return_type_any(self):
        """Test that type checking passes with a return type of Any."""

        @itlp.asset
        def my_asset() -> Any:
            return 123

        assert my_asset.run() == 123

    def test_run_with_type_check_fails(self):
        """Test that type checking fails with an invalid return type."""

        @itlp.asset
        def my_asset() -> str:
            return 123

        with pytest.raises(itlp.errors.AssetValueError, match="Asset my_asset returned data of type int, expected str"):
            my_asset.run()

    def test_run_with_normalizer(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        """Test running an asset with a normalizer."""
        asset.normalizer = normalizer

        assert asset.run(who="world") == "normalized hello world"
        assert asset.schema.equals(itlp.AssetSchema.from_dict({"whatever": str}))

    def test_run_with_normalizer_fails(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        """Test that running an asset with a failing normalizer fails."""
        normalizer.normalize = Mock(side_effect=Exception("error"))
        asset.normalizer = normalizer

        @itlp.asset(normalizer=normalizer)
        def my_asset():
            return "data"

        with pytest.raises(
            itlp.errors.AssetNormalizationError, match="Failed to normalize data for asset my_asset: error"
        ):
            my_asset.run(who="world")

    def test_run_schema_inference_fails(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        """Test that running an asset with failing schema inference fails."""
        normalizer.infer_schema = Mock(side_effect=Exception("error"))
        asset.normalizer = normalizer

        @itlp.asset(normalizer=normalizer)
        def my_asset():
            return "data"

        with pytest.raises(itlp.errors.AssetSchemaError, match="Failed to infer schema for asset my_asset: error"):
            my_asset.run()

    def test_run_with_normalizer_inferred_schema_match(self, asset: itlp.Asset, normalizer: itlp.Normalizer, caplog):
        """Test that a matching inferred schema passes."""
        asset.schema = itlp.AssetSchema.from_dict({"whatever": str})
        asset.normalizer = normalizer

        with caplog.at_level("DEBUG"):
            assert asset.run(who="world") == "normalized hello world"
        assert "Asset asset schema inferred from data (Schema check passed ✔)" in caplog.text

    def test_run_with_normalizer_inferred_schema_mismatch(
        self, asset: itlp.Asset, normalizer: itlp.Normalizer, caplog
    ):
        """Test that a mismatching inferred schema logs a warning."""
        asset.schema = itlp.AssetSchema.from_dict({"something_else": str})
        asset.normalizer = normalizer

        with caplog.at_level("DEBUG"):
            assert asset.run(who="world") == "normalized hello world"
        assert "Schema mismatch for asset asset between provided and inferred schemas" in caplog.text

    # TODO: test run with AssetParam passed as an overriding parameter -> should be resolved


class TestAssetMaterialize:
    """Test the asset materialize method."""

    def test_materialize(self, asset: itlp.Asset, io: itlp.IO):
        """Test materializing an asset."""
        asset.io = io
        asset.bind(who="world")

        asset.materialize()

        io.write.assert_called_once_with(
            itlp.IOContext(asset=asset, partition=None),
            "hello world",
        )

    def test_materialize_with_context(self, asset: itlp.Asset, io: itlp.IO):
        """Test materializing an asset with a context."""
        asset.io = io
        asset.bind(who="world")

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
            partition=None,
        )
        asset.materialize(context)

        io.write.assert_called_once_with(
            itlp.IOContext(asset=asset, partition=None),
            "hello world",
        )

    def test_materialize_with_context_with_partition(self, asset: itlp.Asset, io: itlp.IO):
        """Test materializing an asset with a context and partition."""
        asset.io = io
        asset.partitioning = itlp.PartitionConfig(column="date")
        asset.bind(who="world")

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
            partition=itlp.Partition(value="whatever"),
        )
        asset.materialize(context)

        io.write.assert_called_once_with(
            itlp.IOContext(asset=asset, partition=itlp.Partition(value="whatever")),
            "hello world",
        )

    def test_not_materializable(self, asset: itlp.Asset, io: itlp.IO):
        """Test that a non-materializable asset is not materialized."""
        asset.materializable = False

        asset.materialize()

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            with patch.object(asset, "_normalize", wraps=asset._normalize) as normalize:
                with patch.object(asset, "_write", wraps=asset._write) as write:
                    asset.materialize()

        extract.assert_not_called()
        normalize.assert_not_called()
        write.assert_not_called()

    def test_materialize_fails_no_io(self, asset: itlp.Asset):
        """Test that materializing an asset with no IO fails."""
        with pytest.raises(itlp.errors.AssetMaterializationError, match="Asset asset does not have any IO configured"):
            asset.materialize()

    def test_materialize_multiple_io(self, asset: itlp.Asset):
        """Test materializing an asset with multiple IOs."""
        asset.io = {
            "simple": Mock(),
            "other": Mock(),
        }
        asset.bind(who="world")

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize()

        extract.assert_called_once_with(None)
        asset.io["simple"].write.assert_called_once()
        asset.io["other"].write.assert_called_once()

    def test_materialize_with_strategy_strict(self, asset: itlp.Asset, io: itlp.IO, normalizer: itlp.Normalizer):
        """Test materializing an asset with a strict strategy."""
        asset.io = io
        asset.normalizer = normalizer
        asset.schema = itlp.AssetSchema.from_dict({"whatever": str})
        asset.materialization_strategy = itlp.MaterializationStrategy.STRICT
        asset.bind(who="world")

        asset.materialize()

    def test_materialize_with_strategy_strict_fails(
        self, asset: itlp.Asset, io: itlp.IO, normalizer: itlp.Normalizer
    ):
        """Test that materializing an asset with a strict strategy and mismatching schema fails."""
        asset.io = io
        asset.normalizer = normalizer
        asset.schema = itlp.AssetSchema.from_dict({"something_else": str})
        asset.materialization_strategy = itlp.MaterializationStrategy.STRICT
        asset.bind(who="world")

        with pytest.raises(
            itlp.errors.AssetNormalizationError, match="The data does not match the provided schema for asset asset"
        ):
            asset.materialize()

    def test_materialize_with_strategy_flexible(self, asset: itlp.Asset, io: itlp.IO):
        """Test materializing an asset with a flexible strategy."""
        asset.materialization_strategy = itlp.MaterializationStrategy.FLEXIBLE
        asset.io = io
        asset.bind(who="world")

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize()

        extract.assert_called_once_with(None)


class TestAssetBind:
    """Test the asset bind method."""

    def test_bind(self, asset: itlp.Asset):
        """Test binding a parameter to an asset."""
        asset.bind(who="world")

        signature(asset.data).parameters["who"].default == "world"

    def test_bind_invalid_param(self, asset: itlp.Asset):
        """Test that binding an invalid parameter fails."""
        with pytest.raises(
            itlp.errors.AssetValueError, match="Parameter what is not a valid parameter for asset asset"
        ):
            asset.bind(what="ever")

    def test_bind_invalid_param_ignored(self, asset: itlp.Asset):
        """Test that binding an invalid parameter is ignored when specified."""
        asset.bind(what="ever", ignore_unknown_params=True)

        assert "what" not in signature(asset.data).parameters


# TODO: move those tests under one of the class above? (test only public methods?)
class TestAssetResolveParameters:
    """Test the asset resolve parameters method."""

    def test_resolve_parameters_no_param(self, asset: itlp.Asset):
        """Test resolving parameters with no parameters."""

        @itlp.asset
        def asset(): ...

        params, return_type = asset._resolve_parameters()

        assert params == {}
        assert return_type is Parameter.empty

    def test_resolve_parameters_with_return_type(self):
        """Test resolving parameters with a return type."""

        @itlp.asset
        def asset() -> str: ...

        params, return_type = asset._resolve_parameters()

        assert params == {}
        assert return_type is str

    def test_resolve_parameters_has_default_value(self):
        """Test resolving parameters with a default value."""

        @itlp.asset
        def asset(what="ever"): ...

        params, return_type = asset._resolve_parameters()

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_defined(self):
        """Test resolving parameters with a user-defined parameter."""

        @itlp.asset
        def asset(what): ...

        params, return_type = asset._resolve_parameters(what="ever")

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_asset_param(self, asset_param: itlp.AssetParam):
        """Test resolving parameters with an asset parameter."""

        @itlp.asset
        def asset(what=asset_param): ...

        params, return_type = asset._resolve_parameters()

        assert params == {"what": "resolved"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_asset_param_user_defined(self, asset_param: itlp.AssetParam):
        """Test resolving parameters with a user-defined asset parameter."""

        @itlp.asset
        def asset(what): ...

        params, return_type = asset._resolve_parameters(what=asset_param)

        assert params == {"what": "resolved"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_asset_param_fails(self, asset_param: itlp.AssetParam):
        """Test that resolving parameters with a failing asset parameter fails."""

        class AssetParam(itlp.AssetParam):
            def resolve(self) -> Any:
                raise Exception("error")

        @itlp.asset
        def asset(what=AssetParam()): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError, match="Failed to resolve parameter what for asset asset: error"
        ):
            asset._resolve_parameters()

    def test_resolve_parameters_contextual_asset_param(self, contextual_asset_param: itlp.ContextualAssetParam):
        """Test resolving parameters with a contextual asset parameter."""

        @itlp.asset
        def asset(what=contextual_asset_param): ...

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
        )
        params, return_type = asset._resolve_parameters(context)

        assert params == {"what": "resolved with context"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_contextual_asset_param_fails(
        self, contextual_asset_param: itlp.ContextualAssetParam
    ):
        """Test that resolving parameters with a failing contextual asset parameter fails."""

        class ContextualAssetParam(itlp.ContextualAssetParam):
            def resolve(self, context) -> Any:
                raise Exception("error")

        @itlp.asset
        def asset(what=ContextualAssetParam()): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError, match="Failed to resolve parameter what for asset asset: error"
        ):
            context = itlp.AssetExecutionContext(
                assets={"asset": asset},
                executed_asset=asset,
            )
            asset._resolve_parameters(context)

    def test_resolve_parameters_contextual_asset_param_fails_missing_context(
        self, contextual_asset_param: itlp.ContextualAssetParam
    ):
        """Test that resolving parameters with a contextual asset parameter and no context fails."""

        @itlp.asset
        def asset(what=contextual_asset_param): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError,
            match="Cannot resolve parameter what for asset asset \\(missing execution context\\)",
        ):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_invalid_return_type_none(self):
        """Test that resolving parameters with a return type of None fails."""

        @itlp.asset
        def asset(what) -> None: ...

        with pytest.raises(itlp.errors.AssetDefinitionError, match="None is not a valid return type for asset asset"):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_invalid_return_type_generic(self):
        """Test that resolving parameters with a generic return type fails."""

        @itlp.asset
        def asset() -> list[int]: ...

        with pytest.raises(
            itlp.errors.AssetDefinitionError, match="Generic return type list\\[int\\] is not allowed for asset asset"
        ):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_param_missing(self):
        """Test that resolving parameters with a missing parameter fails."""

        @itlp.asset
        def asset(what): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError, match="Cannot resolve parameter what for asset asset"
        ):
            asset._resolve_parameters()
