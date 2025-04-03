from inspect import Parameter, signature
from typing import Any
from unittest.mock import Mock, patch

import pytest

from interloper import errors
from interloper.asset import Asset, asset
from interloper.io.base import IO, IOContext
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam
from interloper.partitioning.partitions import Partition
from interloper.partitioning.strategies import PartitionStrategy, TimePartitionStrategy
from interloper.execution.pipeline import ExecutionContext
from interloper.schema import AssetSchema


@pytest.fixture
def simple_asset() -> Asset:
    @asset
    def simple_asset(who: str):
        return f"hello {who}"

    return simple_asset


@pytest.fixture
def simple_normalizer() -> Normalizer:
    class SimpleNormalizer(Normalizer):
        def normalize(self, data: Any) -> str:
            return f"normalized {data}"

        def infer_schema(self, data) -> type[AssetSchema]:
            return AssetSchema.from_dict({"whatever": str})

    return SimpleNormalizer()


@pytest.fixture
def simple_io() -> IO:
    class SimpleIO(IO):
        def write(self, context: IOContext, data: Any) -> None:
            pass

        def read(self, context: IOContext) -> Any:
            "data"

    return SimpleIO()


@pytest.fixture
def simple_asset_param() -> AssetParam:
    class SimpleAssetParam(AssetParam):
        def resolve(self) -> Any:
            return "resolved"

    return SimpleAssetParam()


@pytest.fixture
def simple_contextual_asset_param() -> AssetParam:
    class SimpleContextualAssetParam(ContextualAssetParam):
        def resolve(self, context) -> Any:
            return "resolved with context"

    return SimpleContextualAssetParam()


class TestAssetDefinition:
    def test_abstract_instance_fails(self):
        with pytest.raises(TypeError):
            Asset()

    def test_definition_from_class(self):
        class MyAsset(Asset):
            def data(self): ...

        my_asset = MyAsset(name="my_asset")
        assert isinstance(my_asset, Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_class_name_required(self):
        class MyAsset(Asset):
            def data(self): ...

        with pytest.raises(TypeError):
            MyAsset()

    def test_definition_from_decorator(self):
        @asset
        def my_asset(): ...

        assert isinstance(my_asset, Asset)
        assert my_asset.name == "my_asset"

    def test_definition_from_decorator_with_options(self):
        @asset(name="new_name")
        def my_asset(): ...

        assert isinstance(my_asset, Asset)
        assert my_asset.name == "new_name"

    def test_hash(self):
        @asset
        def my_asset(): ...

        assert hash(my_asset) == hash("my_asset")


class TestAssetProperties:
    def test_dataset_setter(self, simple_asset: Asset):
        simple_asset.dataset = "my_dataset"
        assert simple_asset.dataset == "my_dataset"

    def test_io_setter(self, simple_asset: Asset):
        simple_asset.io = {"what": "ever"}
        assert simple_asset.io == {"what": "ever"}

    def test_default_io_key_setter(self, simple_asset: Asset):
        simple_asset.default_io_key = "whatever"
        assert simple_asset.default_io_key == "whatever"

    def test_normalizer_setter(self, simple_asset: Asset, simple_normalizer: Normalizer):
        simple_asset.normalizer = simple_normalizer
        assert simple_asset.normalizer == simple_normalizer

    def test_normalizer_setter_fails(self, simple_asset: Asset, simple_normalizer: Normalizer):
        with pytest.raises(errors.AssetValueError, match="Normalizer must be an instance of Normalizer, got str"):
            simple_asset.normalizer = "normalizer"

    def test_has_io(self, simple_asset: Asset):
        simple_asset.io = {"what": "ever"}
        assert simple_asset.has_io

    def test_has_io_false(self, simple_asset: Asset):
        assert not simple_asset.has_io

    def test_allows_partition_window(self, simple_asset: Asset):
        simple_asset.partition_strategy = TimePartitionStrategy(column="date", allow_window=True)
        assert simple_asset.allows_partition_window

    def test_allows_partition_window_false(self, simple_asset: Asset):
        assert not simple_asset.allows_partition_window

        simple_asset.partition_strategy = PartitionStrategy(column="date")
        assert not simple_asset.allows_partition_window

        simple_asset.partition_strategy = TimePartitionStrategy(column="date")
        assert not simple_asset.allows_partition_window


class TestAssetCall:
    def test_call(self, simple_asset: Asset):
        simple_asset.io = {"what": "ever"}
        simple_asset.default_io_key = "whatever"

        copy = simple_asset()

        assert id(copy) != id(simple_asset)
        assert copy.io == {"what": "ever"}
        assert copy.default_io_key == "whatever"

    def test_call_with_options(self, simple_asset: Asset):
        simple_asset.io = {"what": "ever"}
        simple_asset.default_io_key = "whatever"

        copy = simple_asset(
            who="world",
            io={"something": "else"},
            default_io_key="something",
        )

        assert copy.io == {"something": "else"}
        assert copy.default_io_key == "something"
        assert signature(copy.data).parameters["who"].default == "world"


class TestAssetRun:
    def test_run(self):
        @asset
        def my_asset():
            return "data"

        assert my_asset.run() == "data"

    def test_run_with_param(self, simple_asset: Asset):
        assert simple_asset.run(who="world") == "hello world"

    def test_run_with_param_without_kw_fails(self, simple_asset: Asset):
        with pytest.raises(
            errors.AssetParamResolutionError, match="Cannot resolve parameter who for asset simple_asset"
        ):
            simple_asset.run("world")

    def test_run_with_type_check_fails(self):
        @asset
        def my_asset() -> str:
            return 123

        with pytest.raises(errors.AssetValueError, match="Asset my_asset returned data of type int, expected str"):
            my_asset.run()

    def test_run_with_normalizer(self, simple_asset: Asset, simple_normalizer: Normalizer):
        simple_asset.normalizer = simple_normalizer

        assert simple_asset.run(who="world") == "normalized hello world"
        assert simple_asset.schema.equals(AssetSchema.from_dict({"whatever": str}))

    def test_run_with_normalizer_fails(self, simple_asset: Asset, simple_normalizer: Normalizer):
        simple_normalizer.normalize = Mock(side_effect=Exception("error"))
        simple_asset.normalizer = simple_normalizer

        @asset(normalizer=simple_normalizer)
        def my_asset():
            return "data"

        with pytest.raises(
            errors.AssetMaterializationError, match="Failed to normalize data for asset my_asset: error"
        ):
            my_asset.run(who="world")

    def test_run_schema_inference_fails(self, simple_asset: Asset, simple_normalizer: Normalizer):
        simple_normalizer.infer_schema = Mock(side_effect=Exception("error"))
        simple_asset.normalizer = simple_normalizer

        @asset(normalizer=simple_normalizer)
        def my_asset():
            return "data"

        with pytest.raises(errors.AssetSchemaError, match="Failed to infer schema for asset my_asset: error"):
            my_asset.run()

    def test_run_with_normalizer_inferred_schema_match(
        self, simple_asset: Asset, simple_normalizer: Normalizer, caplog
    ):
        simple_asset.schema = AssetSchema.from_dict({"whatever": str})
        simple_asset.normalizer = simple_normalizer

        with caplog.at_level("DEBUG"):
            assert simple_asset.run(who="world") == "normalized hello world"
        assert "Asset simple_asset schema inferred from data (Schema check passed ✔)" in caplog.text

    def test_run_with_normalizer_inferred_schema_mismatch(
        self, simple_asset: Asset, simple_normalizer: Normalizer, caplog
    ):
        simple_asset.schema = AssetSchema.from_dict({"something_else": str})
        simple_asset.normalizer = simple_normalizer

        with caplog.at_level("DEBUG"):
            assert simple_asset.run(who="world") == "normalized hello world"
        assert "Schema mismatch for asset simple_asset between provided and inferred schemas" in caplog.text


class TestAssetMaterialize:
    def test_materialize(self, simple_asset: Asset, simple_io: IO):
        simple_asset.io = {"simple": simple_io}
        simple_asset.bind(who="world")

        with patch.object(simple_asset, "_extract", wraps=simple_asset._execute) as extract:
            simple_asset.materialize()
            extract.assert_called_once_with(None)

    def test_materialize_with_context(self, simple_asset: Asset, simple_io: IO):
        simple_asset.io = {"simple": simple_io}
        simple_asset.bind(who="world")

        context = ExecutionContext(
            assets={"simple_asset": simple_asset},
            executed_asset=simple_asset,
            partition=None,
        )

        with patch.object(simple_asset, "_extract", wraps=simple_asset._execute) as extract:
            simple_asset.materialize(context)
            extract.assert_called_once_with(context)

    def test_materialize_with_context_with_partition(self, simple_asset: Asset, simple_io: IO):
        simple_asset.io = {"simple": simple_io}
        simple_asset.partition_strategy = PartitionStrategy(column="date")
        simple_asset.bind(who="world")

        context = ExecutionContext(
            assets={"simple_asset": simple_asset},
            executed_asset=simple_asset,
            partition=Partition(value="whatever"),
        )

        with patch.object(simple_asset, "_extract", wraps=simple_asset._execute) as extract:
            simple_asset.materialize(context)
            extract.assert_called_once_with(context)

    def test_materialize_with_context_with_partition_fails_missing_stategy(self, simple_asset: Asset, simple_io: IO):
        simple_asset.io = {"simple": simple_io}
        simple_asset.bind(who="world")

        context = ExecutionContext(
            assets={"simple_asset": simple_asset},
            executed_asset=simple_asset,
            partition=Partition(value="whatever"),
        )

        with pytest.raises(
            errors.AssetMaterializationError,
            match="Asset simple_asset does not support partitioning \\(missing partition_strategy config\\)",
        ):
            simple_asset.materialize(context)

    def test_not_materializable(self, simple_asset: Asset):
        simple_asset.materializable = False

        with patch.object(simple_asset, "_extract", wraps=simple_asset._execute) as extract:
            simple_asset.materialize()
            extract.assert_not_called()

    def test_materialize_fails_no_io(self, simple_asset: Asset):
        with pytest.raises(
            errors.AssetMaterializationError, match="Asset simple_asset does not have any IO configured"
        ):
            simple_asset.materialize()

    def test_materialize_multiple_io(self, simple_asset: Asset):
        simple_asset.io = {
            "simple": Mock(),
            "other": Mock(),
        }
        simple_asset.bind(who="world")

        with patch.object(simple_asset, "_extract", wraps=simple_asset._execute) as extract:
            simple_asset.materialize()
            extract.assert_called_once_with(None)
        simple_asset.io["simple"].write.assert_called_once()
        simple_asset.io["other"].write.assert_called_once()


class TestAssetBind:
    def test_bind(self, simple_asset: Asset):
        simple_asset.bind(who="world")

        signature(simple_asset.data).parameters["who"].default == "world"

    def test_bind_invalid_param(self, simple_asset: Asset):
        with pytest.raises(
            errors.AssetValueError, match="Parameter what is not a valid parameter for asset simple_asset"
        ):
            simple_asset.bind(what="ever")

    def test_bind_invalid_param_ignored(self, simple_asset: Asset):
        simple_asset.bind(what="ever", ignore_unknown_params=True)

        assert "what" not in signature(simple_asset.data).parameters


class TestAssetResolveParameters:
    def test_resolve_parameters_no_param(self):
        @asset
        def simple_asset(): ...

        params, return_type = simple_asset._resolve_parameters()

        assert params == {}
        assert return_type is Parameter.empty

    def test_resolve_parameters_with_return_type(self):
        @asset
        def simple_asset() -> str: ...

        params, return_type = simple_asset._resolve_parameters()

        assert params == {}
        assert return_type is str

    def test_resolve_parameters_has_default_value(self):
        @asset
        def simple_asset(what="ever"): ...

        params, return_type = simple_asset._resolve_parameters()

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_defined(self):
        @asset
        def simple_asset(what): ...

        params, return_type = simple_asset._resolve_parameters(what="ever")

        assert params == {"what": "ever"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_asset_param(self, simple_asset_param):
        @asset
        def simple_asset(what=simple_asset_param): ...

        params, return_type = simple_asset._resolve_parameters()

        assert params == {"what": "resolved"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_contextual_asset_param(self, simple_contextual_asset_param):
        @asset
        def simple_asset(what=simple_contextual_asset_param): ...

        context = ExecutionContext(
            assets={"simple_asset": simple_asset},
            executed_asset=simple_asset,
        )
        params, return_type = simple_asset._resolve_parameters(context)

        assert params == {"what": "resolved with context"}
        assert return_type is Parameter.empty

    def test_resolve_parameters_user_contextual_asset_param_fails_missing_context(self, simple_contextual_asset_param):
        @asset
        def simple_asset(what=simple_contextual_asset_param): ...

        with pytest.raises(
            errors.AssetParamResolutionError, match="Cannot resolve parameter what for asset simple_asset"
        ):
            simple_asset._resolve_parameters()

    def test_resolve_parameters_fails_invalid_return_type(self):
        @asset
        def simple_asset(what) -> None: ...

        with pytest.raises(errors.AssetDefinitionError, match="None is not a valid return type for asset simple_asset"):
            simple_asset._resolve_parameters()

    def test_resolve_parameters_fails_param_missing(self):
        @asset
        def simple_asset(what): ...

        with pytest.raises(
            errors.AssetParamResolutionError, match="Cannot resolve parameter what for asset simple_asset"
        ):
            simple_asset._resolve_parameters()
