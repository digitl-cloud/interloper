from inspect import Parameter, signature
from typing import Any
from unittest.mock import Mock, patch

import pytest

import interloper as itlp

from .fixtures import io


@pytest.fixture
def asset() -> itlp.Asset:
    @itlp.asset
    def asset(who: str):
        return f"hello {who}"

    return asset


@pytest.fixture
def source(asset: itlp.Asset) -> itlp.Source:
    @itlp.source
    def source():
        return (asset,)

    return source


@pytest.fixture
def normalizer() -> itlp.Normalizer:
    class Normalizer(itlp.Normalizer):
        def normalize(self, data: Any) -> str:
            return f"normalized {data}"

        def infer_schema(self, data) -> type[itlp.AssetSchema]:
            return itlp.AssetSchema.from_dict({"whatever": str})

    return Normalizer()


@pytest.fixture
def asset_param() -> itlp.AssetParam:
    class AssetParam(itlp.AssetParam):
        def resolve(self) -> Any:
            return "resolved"

    return AssetParam()


@pytest.fixture
def contextual_asset_param() -> itlp.AssetParam:
    class ContextualAssetParam(itlp.ContextualAssetParam):
        def resolve(self, context) -> Any:
            return "resolved with context"

    return ContextualAssetParam()


class TestAssetDefinition:
    def test_abstract_instance_fails(self):
        with pytest.raises(TypeError):
            itlp.Asset()

    def test_definition_from_class(self):
        class MyAsset(itlp.Asset):
            def data(self): ...

        my_asset = MyAsset(name="my_asset")
        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_class_name_required(self):
        class MyAsset(itlp.Asset):
            def data(self): ...

        with pytest.raises(TypeError):
            MyAsset()

    def test_definition_from_decorator(self):
        @itlp.asset
        def my_asset(): ...

        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_decorator_with_options(self):
        @itlp.asset(name="new_name")
        def my_asset(): ...

        assert isinstance(my_asset, itlp.Asset)
        assert my_asset.name == "new_name"

    def test_hash(self):
        @itlp.asset
        def my_asset(): ...

        assert hash(my_asset) == hash("my_asset")


class TestAssetProperties:
    def test_id(self, asset: itlp.Asset):
        assert asset.id == "asset"

    def test_id_with_dataset(self, asset: itlp.Asset):
        asset.dataset = "dataset"
        assert asset.id == "dataset.asset"

    def test_id_with_dataset_from_source(self, source: itlp.Source):
        assert source.asset.id == "source.asset"

    def test_dataset(self, asset: itlp.Asset):
        asset.dataset = "dataset"
        assert asset.dataset == "dataset"

    def test_dataset_from_source(self, source: itlp.Source):
        assert source.asset.dataset == "source"

        source.dataset = "something"
        assert source.asset.dataset == "something"

    def test_io(self, asset: itlp.Asset, io: itlp.IO):
        asset.io = io
        assert asset.io == io

        asset.io = {"foo": io}
        assert asset.io == {"foo": io}

    def test_io_from_source(self, source: itlp.Source, io: io):
        source.io = io
        assert source.asset.io == io

        source.io = {"foo": io}
        assert source.asset.io == {"foo": io}

    def test_default_io_key(self, asset: itlp.Asset):
        asset.default_io_key = "whatever"
        assert asset.default_io_key == "whatever"

    def test_default_io_key_from_source(self, source: itlp.Source):
        source.default_io_key = "whatever"
        assert source.asset.default_io_key == "whatever"

    def test_normalizer(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        asset.normalizer = normalizer
        assert asset.normalizer == normalizer

    def test_normalizer_from_source(self, source: itlp.Source):
        source.normalizer = normalizer
        assert source.asset.normalizer == normalizer

    def test_normalizer_setter_fails(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        with pytest.raises(itlp.errors.AssetValueError, match="Normalizer must be an instance of Normalizer, got str"):
            asset.normalizer = "normalizer"

    def test_materializable_default(self, asset: itlp.Asset):
        assert asset.materializable

    def test_materializable_false(self, asset: itlp.Asset):
        asset.materializable = False
        assert not asset.materializable

    def test_materializable_from_source(self, source: itlp.Source):
        source.materializable = False
        assert not source.asset.materializable

    def test_has_io(self, asset: itlp.Asset):
        asset.io = {"what": "ever"}
        assert asset.has_io

    def test_has_io_false(self, asset: itlp.Asset):
        assert not asset.has_io

    def test_allows_partition_window(self, asset: itlp.Asset):
        asset.partitioning = itlp.TimePartitionConfig(column="date", allow_window=True)
        assert asset.allows_partition_window

    def test_allows_partition_window_false(self, asset: itlp.Asset):
        assert not asset.allows_partition_window

        asset.partitioning = itlp.PartitionConfig(column="date")
        assert not asset.allows_partition_window

        asset.partitioning = itlp.TimePartitionConfig(column="date")
        assert not asset.allows_partition_window


class TestAssetCall:
    def test_call(self, asset: itlp.Asset):
        asset.io = {"what": "ever"}
        asset.default_io_key = "whatever"

        copy = asset()

        assert id(copy) != id(asset)
        assert copy.io == {"what": "ever"}
        assert copy.default_io_key == "whatever"

    def test_call_with_options(self, asset: itlp.Asset):
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
    def test_run(self):
        @itlp.asset
        def my_asset():
            return "data"

        assert my_asset.run() == "data"

    def test_run_with_param(self, asset: itlp.Asset):
        assert asset.run(who="world") == "hello world"

    def test_run_with_param_without_kw_fails(self, asset: itlp.Asset):
        with pytest.raises(itlp.errors.AssetParamResolutionError, match="Cannot resolve parameter who for asset asset"):
            asset.run("world")

    def test_run_with_type_check_return_type_any(self):
        @itlp.asset
        def my_asset() -> Any:
            return 123

        assert my_asset.run() == 123

    def test_run_with_type_check_fails(self):
        @itlp.asset
        def my_asset() -> str:
            return 123

        with pytest.raises(itlp.errors.AssetValueError, match="Asset my_asset returned data of type int, expected str"):
            my_asset.run()

    def test_run_with_normalizer(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
        asset.normalizer = normalizer

        assert asset.run(who="world") == "normalized hello world"
        assert asset.schema.equals(itlp.AssetSchema.from_dict({"whatever": str}))

    def test_run_with_normalizer_fails(self, asset: itlp.Asset, normalizer: itlp.Normalizer):
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
        normalizer.infer_schema = Mock(side_effect=Exception("error"))
        asset.normalizer = normalizer

        @itlp.asset(normalizer=normalizer)
        def my_asset():
            return "data"

        with pytest.raises(itlp.errors.AssetSchemaError, match="Failed to infer schema for asset my_asset: error"):
            my_asset.run()

    def test_run_with_normalizer_inferred_schema_match(self, asset: itlp.Asset, normalizer: itlp.Normalizer, caplog):
        asset.schema = itlp.AssetSchema.from_dict({"whatever": str})
        asset.normalizer = normalizer

        with caplog.at_level("DEBUG"):
            assert asset.run(who="world") == "normalized hello world"
        assert "Asset asset schema inferred from data (Schema check passed ✔)" in caplog.text

    def test_run_with_normalizer_inferred_schema_mismatch(self, asset: itlp.Asset, normalizer: itlp.Normalizer, caplog):
        asset.schema = itlp.AssetSchema.from_dict({"something_else": str})
        asset.normalizer = normalizer

        with caplog.at_level("DEBUG"):
            assert asset.run(who="world") == "normalized hello world"
        assert "Schema mismatch for asset asset between provided and inferred schemas" in caplog.text

    # TODO: test run with AssetParam passed as an overriding parameter -> should be resolved


class TestAssetMaterialize:
    def test_materialize(self, asset: itlp.Asset, io: itlp.IO):
        asset.io = {"simple": io}
        asset.bind(who="world")

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize()
            extract.assert_called_once_with(None)

    def test_materialize_with_context(self, asset: itlp.Asset, io: itlp.IO):
        asset.io = {"simple": io}
        asset.bind(who="world")

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
            partition=None,
        )

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize(context)
            extract.assert_called_once_with(context)

    def test_materialize_with_context_with_partition(self, asset: itlp.Asset, io: itlp.IO):
        asset.io = {"simple": io}
        asset.partitioning = itlp.PartitionConfig(column="date")
        asset.bind(who="world")

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
            partition=itlp.Partition(value="whatever"),
        )

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize(context)
            extract.assert_called_once_with(context)

    @pytest.mark.skip(
        "Current approach: an asset can be materialized if the context "
        "has a partition but the asset does not support partitioning"
    )
    def test_materialize_with_context_with_partition_fails_missing_stategy(self, asset: itlp.Asset, io: itlp.IO):
        asset.io = {"simple": io}
        asset.bind(who="world")

        context = itlp.ExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
            partition=itlp.Partition(value="whatever"),
        )

        with pytest.raises(
            itlp.errors.AssetMaterializationError,
            match="Asset asset does not support partitioning \\(missing partitioning config\\)",
        ):
            asset.materialize(context)

    def test_not_materializable(self, asset: itlp.Asset):
        asset.materializable = False

        with patch.object(asset, "_execute", wraps=asset._execute) as extract:
            asset.materialize()
            extract.assert_not_called()

    def test_materialize_fails_no_io(self, asset: itlp.Asset):
        with pytest.raises(itlp.errors.AssetMaterializationError, match="Asset asset does not have any IO configured"):
            asset.materialize()

    def test_materialize_multiple_io(self, asset: itlp.Asset):
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


class TestAssetBind:
    def test_bind(self, asset: itlp.Asset):
        asset.bind(who="world")

        signature(asset.data).parameters["who"].default == "world"

    def test_bind_invalid_param(self, asset: itlp.Asset):
        with pytest.raises(
            itlp.errors.AssetValueError, match="Parameter what is not a valid parameter for asset asset"
        ):
            asset.bind(what="ever")

    def test_bind_invalid_param_ignored(self, asset: itlp.Asset):
        asset.bind(what="ever", ignore_unknown_params=True)

        assert "what" not in signature(asset.data).parameters


class TestAssetResolveParameters:
    def test_resolve_parameters_no_param(self):
        @itlp.asset
        def asset(): ...

        params, return_type = asset._resolve_parameters()

        assert params == {}
        assert return_type is Parameter.empty

    def test_resolve_parameters_with_return_type(self):
        @itlp.asset
        def asset() -> str: ...

        params, return_type = asset._resolve_parameters()

        assert params == {}
        assert return_type is str

    def test_resolve_parameters_has_default_value(self):
        @itlp.asset
        def asset(what="ever"): ...

        params, return_type = asset._resolve_parameters()

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_defined(self):
        @itlp.asset
        def asset(what): ...

        params, return_type = asset._resolve_parameters(what="ever")

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_asset_param(self, asset_param):
        @itlp.asset
        def asset(what=asset_param): ...

        params, return_type = asset._resolve_parameters()

        assert params == {"what": "resolved"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_contextual_asset_param(self, contextual_asset_param):
        @itlp.asset
        def asset(what=contextual_asset_param): ...

        context = itlp.AssetExecutionContext(
            assets={"asset": asset},
            executed_asset=asset,
        )
        params, return_type = asset._resolve_parameters(context)

        assert params == {"what": "resolved with context"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_contextual_asset_param_fails_missing_context(self, contextual_asset_param):
        @itlp.asset
        def asset(what=contextual_asset_param): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError, match="Cannot resolve parameter what for asset asset"
        ):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_invalid_return_type_none(self):
        @itlp.asset
        def asset(what) -> None: ...

        with pytest.raises(itlp.errors.AssetDefinitionError, match="None is not a valid return type for asset asset"):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_invalid_return_type_generic(self):
        @itlp.asset
        def asset() -> list[int]: ...

        with pytest.raises(
            itlp.errors.AssetDefinitionError, match="Generic return type list\\[int\\] is not allowed for asset asset"
        ):
            asset._resolve_parameters()

    def test_resolve_parameters_fails_param_missing(self):
        @itlp.asset
        def asset(what): ...

        with pytest.raises(
            itlp.errors.AssetParamResolutionError, match="Cannot resolve parameter what for asset asset"
        ):
            asset._resolve_parameters()
