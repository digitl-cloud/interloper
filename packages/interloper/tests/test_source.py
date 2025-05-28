from inspect import signature
from typing import Any

import pytest

import interloper as itlp

from .fixtures import io


@pytest.fixture
def source() -> itlp.Source:
    @itlp.source
    def source():
        @itlp.asset
        def asset(who: str = "world"):
            return f"hello {who}"

        return (asset,)

    return source


@pytest.fixture
def normalizer() -> itlp.Normalizer:
    class Normalizer(itlp.Normalizer):
        def normalize(self, data: Any) -> Any:
            return f"normalized {data}"

        def infer_schema(self, data): ...

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


class TestSourceDefinition:
    def test_abstract_instance_fails(self):
        with pytest.raises(
            TypeError, match="Can't instantiate abstract class Source with abstract method asset_definitions"
        ):
            itlp.Source()

    def test_from_class(self):
        class Asset(itlp.Asset):
            def data(self):
                return "hello"

        class Source(itlp.Source):
            def asset_definitions(self):
                return (Asset(name="asset"),)

        source = Source(name="source")
        assert isinstance(source, itlp.Source)
        assert source._initialized is False

        assert isinstance(source.asset, itlp.Asset)
        assert source.name == "source"
        assert source.asset.name == "asset"
        assert source.asset.dataset == "source"
        assert source.asset.default_io_key is None
        assert source.asset.normalizer is None
        assert source._initialized

    def test_from_class_name_required(self):
        class Source(itlp.Source):
            def asset_definitions(self):
                return {}

        with pytest.raises(TypeError):
            Source()

    def test_from_decorator(self, source):
        assert isinstance(source, itlp.Source)
        assert source._initialized is False

        assert isinstance(source.asset, itlp.Asset)
        assert source.name == "source"
        assert source.asset.name == "asset"
        assert source.asset.dataset == "source"
        assert source.asset.default_io_key is None
        assert source.asset.normalizer is None
        assert source._initialized is True

    def test_from_decorator_with_options(self):
        @itlp.source(name="new_name")
        def source():
            return {}

        assert isinstance(source, itlp.Source)
        assert source.name == "new_name"

    def test_assets_are_readonly(self, source):
        # Source needs to be initialized first. Accessing asset will trigger initialization.
        # TODO: We should find a way force initialization of the source if we're accessing attributes on an
        #       uninitialized source. But this is tricky because of recursion with __getattr__ and __setattr__.
        source.asset

        with pytest.raises(itlp.errors.SourceDefinitionError, match="Asset asset is read-only"):
            source.asset = None

    def test_propagate_dataset(self, source):
        source.dataset = "new_dataset"
        assert source.asset.dataset == "new_dataset"

    def test_propagate_io(self, source):
        source.io = {"what": "ever"}
        assert source.asset.io == {"what": "ever"}

    def test_propagate_default_io_key(self, source):
        source.default_io_key = "whatever"
        assert source.asset.default_io_key == "whatever"

    def test_propagate_materializable(self, source):
        source.materializable = False
        assert source.asset.materializable is False

    def test_propage_normalizer(self, source, normalizer):
        source.normalizer = normalizer
        assert source.asset.normalizer == normalizer


class TestSourceProperties:
    def test_assets(self, source: itlp.Source):
        assert len(source.assets) == 1
        assert source.asset.name == "asset"

    def test_io(self, source: itlp.Source, io: io):
        source.io = io
        assert source.io == io

        source.io = {"foo": io}
        assert source.io == {"foo": io}


class TestGetItem:
    def test_get_item(self, source: itlp.Source):
        assert isinstance(source["asset"], itlp.Asset)
        assert source["asset"].name == "asset"

    def test_get_item_with_invalid_name(self, source: itlp.Source):
        with pytest.raises(itlp.errors.SourceValueError, match="Asset invalid_asset not found in source source"):
            source["invalid_asset"]


class TestSourceCall:
    def test_call(self, source: itlp.Source):
        copy = source()

        assert isinstance(copy, itlp.Source)
        assert copy.name == "source"
        assert len(copy.assets) == 1
        assert copy.asset.name == "asset"

    def test_call_with_args(self, source: itlp.Source):
        copy = source(
            dataset="new_dataset",
            io={"what": "ever"},
            default_io_key="whatever",
            default_assets_args={"who": "world"},
        )

        assert isinstance(copy, itlp.Source)
        assert copy.name == "source"
        assert copy.dataset == "new_dataset"
        assert len(copy.assets) == 1
        assert copy.asset.name == "asset"
        assert copy.asset.dataset == "new_dataset"
        assert copy.asset.io == {"what": "ever"}
        assert copy.asset.default_io_key == "whatever"
        assert signature(copy.asset.data).parameters["who"].default == "world"

    def test_call_source_that_has_params(self):
        @itlp.source
        def source(key: str):
            @itlp.asset
            def asset(who: str):
                return f"hello {who}"

            return (asset,)

        copy = source(key="new_key")
        assert isinstance(copy, itlp.Source)
        assert copy.name == "source"
        assert len(copy.assets) == 1
        assert copy.asset.name == "asset"
        assert signature(copy.asset_definitions).parameters["key"].default == "new_key"


class TestSourceBind:
    def test_bind(self):
        @itlp.source
        def source(key: str): ...

        source.bind(key="new_key")
        assert signature(source.asset_definitions).parameters["key"].default == "new_key"

    def test_bind_invalid_param(self, source: itlp.Source):
        with pytest.raises(
            itlp.errors.SourceValueError, match="Parameter invalid_param is not a valid parameter for source source"
        ):
            source.bind(invalid_param="new_key")


class TestSourceBuildAssets:
    def test_build_assets(self, source: itlp.Source):
        assert len(source.assets) == 1
        assert source.asset.name == "asset"
        assert source.asset.dataset == "source"
        assert source.asset.default_io_key is None
        assert source.asset.normalizer is None

    def test_build_assets_with_default_assets_args(self, source: itlp.Source):
        source.default_assets_args = {"who": "something_else"}
        source._build_assets()

        assert len(source.assets) == 1
        assert source.asset.name == "asset"
        assert source.asset.dataset == "source"
        assert source.asset.default_io_key is None
        assert source.asset.normalizer is None
        assert signature(source.asset.data).parameters["who"].default == "something_else"

    def test_build_assets_with_invalid_asset_definition(self, source: itlp.Source):
        source.asset_definitions = lambda: (1, 2, 3)
        with pytest.raises(itlp.errors.SourceValueError, match="Expected an instance of Asset, but got <class 'int'>"):
            source._build_assets()

    def test_build_assets_with_duplicate_asset_name(self, source: itlp.Source):
        @itlp.source
        def source():
            @itlp.asset(name="asset")
            def asset_a(): ...

            @itlp.asset(name="asset")
            def asset_b(): ...

            return (asset_a, asset_b)

        with pytest.raises(itlp.errors.SourceValueError, match="Duplicate asset name 'asset'"):
            source._build_assets()

    def test_build_asset_with_auto_asset_deps(self, source: itlp.Source):
        @itlp.source
        def source():
            @itlp.asset
            def asset_a(): ...

            @itlp.asset
            def asset_b(a=itlp.UpstreamAsset("asset_a")): ...

            return (asset_a, asset_b)

        assert source.asset_b.deps == {"asset_a": "source.asset_a"}

    def test_build_asset_with_auto_asset_deps_disabled(self):
        @itlp.source(auto_asset_deps=False)
        def source():
            @itlp.asset
            def asset_a(): ...

            @itlp.asset
            def asset_b(a=itlp.UpstreamAsset("asset_a")): ...

            return (asset_a, asset_b)

        assert source.asset_b.deps == {}


class TestSourceResolveParameters:
    def test_resolve_parameters(self):
        @itlp.source
        def source(key: str = "KEY"):
            @itlp.asset
            def asset_a(): ...

            return (asset_a,)

        params = source._resolve_parameters()

        assert params == {"key": "KEY"}

    def test_resolve_parameters_with_asset_param(self, asset_param: itlp.AssetParam):
        @itlp.source
        def source(x=asset_param):
            @itlp.asset
            def asset_a(): ...

            return (asset_a,)

        params = source._resolve_parameters()
        assert params == {"x": "resolved"}

    def test_resolve_parameters_with_contextual_asset_param_fails(
        self, contextual_asset_param: itlp.ContextualAssetParam
    ):
        with pytest.raises(
            itlp.errors.SourceParamError, match="ContextualAssetParam x not supported in source parameters"
        ):

            @itlp.source
            def source(x=contextual_asset_param):
                @itlp.asset
                def asset_a(): ...

                return (asset_a,)

            source._resolve_parameters()
