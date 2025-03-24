from inspect import signature
from typing import Any

import pytest

from interloper import errors
from interloper.asset import Asset, asset
from interloper.normalizer import Normalizer


@pytest.fixture
def simple_asset() -> Asset:
    @asset
    def simple_asset(who: str):
        return f"hello {who}"

    return simple_asset


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


class TestAssetCall:
    def test_copy(self, simple_asset: Asset):
        simple_asset.io = {"what": "ever"}
        simple_asset.default_io_key = "whatever"

        print(simple_asset.io)

        copy = simple_asset()

        assert id(copy) != id(simple_asset)
        assert copy.io == {"what": "ever"}
        assert copy.default_io_key == "whatever"

    def test_copy_with_param_override(self, simple_asset: Asset):
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

    def test_run_with_normalizer(self):
        class SimpleNormalizer(Normalizer):
            def normalize(self, data: Any) -> Any:
                return f"normalized {data}"

            def infer_schema(self, data: Any) -> Any: ...

        @asset(normalizer=SimpleNormalizer())
        def my_asset():
            return "data"

        assert my_asset.run() == "normalized data"

    def test_run_with_normalizer_fails(self):
        class SimpleNormalizer(Normalizer):
            def normalize(self, data: Any) -> Any:
                raise Exception("error")

            def infer_schema(self, data: Any) -> Any: ...

        @asset(normalizer=SimpleNormalizer())
        def my_asset():
            return "data"

        with pytest.raises(
            errors.AssetMaterializationError, match="Failed to normalize data for asset my_asset: error"
        ):
            my_asset.run()


# TODO
class TestAssetMaterialize: ...
