from inspect import signature
from typing import Any

import pytest

from interloper import errors, source
from interloper.asset import Asset, asset
from interloper.normalizer import Normalizer
from interloper.param import AssetParam, ContextualAssetParam, UpstreamAsset
from interloper.source import Source


@pytest.fixture
def simple_source() -> Source:
    @source
    def simple_source(key: str = "KEY"):
        @asset
        def simple_asset(who: str = "world"):
            return f"hello {who}"

        return (simple_asset,)

    return simple_source


@pytest.fixture
def simple_normalizer() -> Normalizer:
    class SimpleNormalizer(Normalizer):
        def normalize(self, data: Any) -> Any:
            return f"normalized {data}"

        def infer_schema(self, data): ...

    return SimpleNormalizer()


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


class TestSourceDefinition:
    def test_abstract_instance_fails(self):
        with pytest.raises(TypeError):
            Source()

    def test_definition_from_class(self):
        class SimpleAsset(Asset):
            def data(self):
                return "hello"

        class SimpleSource(Source):
            def asset_definitions(self):
                return (SimpleAsset(name="simple_asset"),)

        simple_source = SimpleSource(name="simple_source")
        assert isinstance(simple_source, Source)
        assert isinstance(simple_source.simple_asset, Asset)
        assert simple_source.name == "simple_source"
        assert simple_source.simple_asset.name == "simple_asset"
        assert simple_source.simple_asset.dataset == "simple_source"
        assert simple_source.simple_asset.default_io_key is None
        assert simple_source.simple_asset.normalizer is None

    def test_definition_from_class_name_required(self):
        class SimpleSource(Source):
            def asset_definitions(self):
                return {}

        with pytest.raises(TypeError):
            SimpleSource()

    def test_definition_from_decorator(self, simple_source):
        assert isinstance(simple_source, Source)
        assert isinstance(simple_source.simple_asset, Asset)
        assert simple_source.name == "simple_source"
        assert simple_source.simple_asset.name == "simple_asset"
        assert simple_source.simple_asset.dataset == "simple_source"
        assert simple_source.simple_asset.default_io_key is None
        assert simple_source.simple_asset.normalizer is None

    def test_definition_from_decorator_with_options(self):
        @source(name="new_name")
        def simple_source():
            return {}

        assert isinstance(simple_source, Source)
        assert simple_source.name == "new_name"

    def test_definition_param_default_value_required(self):
        with pytest.raises(
            errors.SourceParamError, match="Source simple_source requires a default value for parameter key"
        ):

            @source
            def simple_source(key: str): ...

    def test_definition_assets_are_readonly(self, simple_source):
        with pytest.raises(errors.SourceDefinitionError, match="Asset simple_asset is read-only"):
            simple_source.simple_asset = None

    def test_definition_propagate_dataset(self, simple_source):
        simple_source.dataset = "new_dataset"
        assert simple_source.simple_asset.dataset == "new_dataset"

    def test_definition_propagate_io(self, simple_source):
        simple_source.io = {"what": "ever"}
        assert simple_source.simple_asset.io == {"what": "ever"}

    def test_definition_propagate_default_io_key(self, simple_source):
        simple_source.default_io_key = "whatever"
        assert simple_source.simple_asset.default_io_key == "whatever"

    def test_definition_propage_normalizer(self, simple_source, simple_normalizer):
        simple_source.normalizer = simple_normalizer
        assert simple_source.simple_asset.normalizer == simple_normalizer


class TestSourceProperties:
    def test_assets(self, simple_source: Source):
        assert len(simple_source.assets) == 1
        assert simple_source.simple_asset.name == "simple_asset"


class TestSourceCall:
    def test_call(self, simple_source: Source):
        copy = simple_source()

        assert isinstance(copy, Source)
        assert copy.name == "simple_source"
        assert len(copy.assets) == 1
        assert copy.simple_asset.name == "simple_asset"

    def test_call_with_options(self, simple_source: Source):
        copy = simple_source(
            dataset="new_dataset",
            io={"what": "ever"},
            default_io_key="whatever",
            default_assets_args={"who": "world"},
        )

        assert isinstance(copy, Source)
        assert copy.name == "simple_source"
        assert copy.dataset == "new_dataset"
        assert len(copy.assets) == 1
        assert copy.simple_asset.name == "simple_asset"
        assert copy.simple_asset.dataset == "new_dataset"
        assert copy.simple_asset.io == {"what": "ever"}
        assert copy.simple_asset.default_io_key == "whatever"
        assert signature(copy.simple_asset.data).parameters["who"].default == "world"


class TestSourceBind:
    def test_bind(self, simple_source: Source):
        simple_source.bind(key="new_key")

        assert signature(simple_source.asset_definitions).parameters["key"].default == "new_key"

    def test_bind_invalid_param(self, simple_source: Source):
        with pytest.raises(
            errors.SourceValueError, match="Parameter invalid_param is not a valid parameter for source simple_source"
        ):
            simple_source.bind(invalid_param="new_key")


class TestSourceBuildAssets:
    def test_build_assets(self, simple_source: Source):
        assert len(simple_source.assets) == 1
        assert simple_source.simple_asset.name == "simple_asset"
        assert simple_source.simple_asset.dataset == "simple_source"
        assert simple_source.simple_asset.default_io_key is None
        assert simple_source.simple_asset.normalizer is None

    def test_build_assets_with_default_assets_args(self, simple_source: Source):
        simple_source.default_assets_args = {"who": "something_else"}
        simple_source._build_assets()

        assert len(simple_source.assets) == 1
        assert simple_source.simple_asset.name == "simple_asset"
        assert simple_source.simple_asset.dataset == "simple_source"
        assert simple_source.simple_asset.default_io_key is None
        assert simple_source.simple_asset.normalizer is None
        assert signature(simple_source.simple_asset.data).parameters["who"].default == "something_else"

    def test_build_assets_with_invalid_asset_definition(self, simple_source: Source):
        simple_source.asset_definitions = lambda: (1, 2, 3)
        with pytest.raises(errors.SourceValueError, match="Expected an instance of Asset, but got <class 'int'>"):
            simple_source._build_assets()

    def test_build_assets_with_duplicate_asset_name(self, simple_source: Source):
        simple_source.asset_definitions = lambda: (simple_source.simple_asset, simple_source.simple_asset)
        with pytest.raises(errors.SourceValueError, match="Duplicate asset name 'simple_asset'"):
            simple_source._build_assets()

    def test_build_asset_with_auto_asset_deps(self, simple_source: Source):
        @source
        def simple_source():
            @asset
            def asset_a(): ...

            @asset
            def asset_b(a=UpstreamAsset("asset_a")): ...

            return (asset_a, asset_b)

        assert simple_source.asset_b.deps == {"asset_a": "asset_a"}

    def test_build_asset_with_auto_asset_deps_disabled(self):
        @source(auto_asset_deps=False)
        def simple_source():
            @asset
            def asset_a(): ...

            @asset
            def asset_b(a=UpstreamAsset("asset_a")): ...

            return (asset_a, asset_b)

        assert simple_source.asset_b.deps == {}


class TestSourceResolveParameters:
    def test_resolve_parameters(self, simple_source: Source):
        params = simple_source._resolve_parameters()

        assert params == {"key": "KEY"}

    def test_resolve_parameters_with_asset_param(self, simple_source: Source, simple_asset_param: AssetParam):
        @source
        def simple_source(x=simple_asset_param):
            @asset
            def asset_a(): ...

            return (asset_a,)

        params = simple_source._resolve_parameters()
        assert params == {"x": "resolved"}

    def test_resolve_parameters_with_contextual_asset_param_fails(
        self, simple_source: Source, simple_contextual_asset_param: ContextualAssetParam
    ):
        with pytest.raises(errors.SourceParamError, match="ContextualAssetParam x not supported in source parameters"):

            @source
            def simple_source(x=simple_contextual_asset_param):
                @asset
                def asset_a(): ...

                return (asset_a,)
