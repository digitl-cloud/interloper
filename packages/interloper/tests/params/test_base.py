"""This module contains tests for the base asset parameter classes."""
from unittest.mock import Mock

import pytest

from interloper.execution.context import AssetExecutionContext
from interloper.params.base import AssetParam, ContextualAssetParam


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