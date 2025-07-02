"""This module contains shared fixtures for tests."""
from typing import Any
from unittest.mock import Mock

import pytest

import interloper as itlp
from interloper.io.base import IOHandler
from interloper.schema import AssetSchema


@pytest.fixture
def schema() -> type[AssetSchema]:
    """Return a mock asset schema."""

    class Schema(AssetSchema):
        foo: str
        bar: int
    return Schema


@pytest.fixture
def handler() -> IOHandler[str]:
    """Return a mock IO handler."""

    class Handler(IOHandler[str]):
        def __init__(self, reconciler=None):
            super().__init__(type=str, reconciler=reconciler)
            self._written = None
            self._read = "handler_data"
        
        def write(self, context, data):
            self._written = (context, data)
        
        def read(self, context):
            return self._read
    
    return Handler()


@pytest.fixture
def handler_with_reconciler(reconciler) -> IOHandler[str]:
    """Return a mock IO handler with a reconciler."""

    class Handler(IOHandler[str]):
        def __init__(self, reconciler=None):
            super().__init__(type=str, reconciler=reconciler)
            self._written = None
            self._read = "handler_data"
        
        def write(self, context, data):
            self._written = (context, data)
        
        def read(self, context):
            return self._read
    
    return Handler(reconciler=reconciler)


@pytest.fixture
def reconciler() -> Any:
    """Return a mock reconciler."""

    class Reconciler:
        def reconcile(self, data, table_schema):
            return f"reconciled_{data}"
    
    return Reconciler()


@pytest.fixture
def asset() -> itlp.Asset:
    """Return a mock asset."""

    @itlp.asset
    def asset(who: str) -> str:
        return f"hello {who}"

    return asset


@pytest.fixture
def source(asset: itlp.Asset) -> itlp.Source:
    """Return a mock source."""

    @itlp.source
    def source():
        return (asset,)

    return source


@pytest.fixture
def normalizer() -> itlp.Normalizer:
    """Return a mock normalizer."""

    class Normalizer(itlp.Normalizer):
        def normalize(self, data: Any) -> str:
            return f"normalized {data}"

        def infer_schema(self, data) -> type[itlp.AssetSchema]:
            return itlp.AssetSchema.from_dict({"whatever": str})

    return Normalizer()


@pytest.fixture
def asset_param() -> itlp.AssetParam:
    """Return a mock asset parameter."""

    class AssetParam(itlp.AssetParam):
        def resolve(self) -> Any:
            return "resolved"

    return AssetParam()


@pytest.fixture
def contextual_asset_param() -> itlp.AssetParam:
    """Return a mock contextual asset parameter."""

    class ContextualAssetParam(itlp.ContextualAssetParam):
        def resolve(self, context) -> Any:
            return "resolved with context"

    return ContextualAssetParam()


@pytest.fixture
def io() -> itlp.IO:
    """Return a mock IO."""

    class IO(itlp.IO):
        def write(self, context: itlp.IOContext, data: Any) -> None:
            pass

        def read(self, context: itlp.IOContext) -> Any:
            return "data"

    io = IO()
    io.write = Mock(wraps=io.write)
    io.read = Mock(wraps=io.read)
    return io
