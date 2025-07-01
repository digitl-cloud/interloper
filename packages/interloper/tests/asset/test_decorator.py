"""This module contains tests for the asset decorator."""
from interloper.asset.decorator import ConcreteAsset


def test_concrete_asset_repr():
    """Test the repr of a concrete asset."""
    asset = ConcreteAsset(name="test_asset")
    assert repr(asset) == f"<Asset test_asset at {hex(id(asset))}>"
