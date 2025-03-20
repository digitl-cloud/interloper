from typing import Any

import pytest

from interloper.asset import Asset, asset
from interloper.normalizer import Normalizer
from interloper.source import Source, source


@pytest.fixture
def simple_source() -> Source:
    @source
    def simple_source():
        @asset
        def simple_asset():
            return "hello"

        return (simple_asset,)

    return simple_source


@pytest.fixture
def simple_normalizer() -> Normalizer:
    class SimpleNormalizer(Normalizer):
        def normalize(self, data: Any) -> Any:
            return f"normalized {data}"

        def infer_schema(self, data): ...

    return SimpleNormalizer()


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
