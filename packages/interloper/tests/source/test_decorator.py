"""This module contains tests for the source decorator."""
from interloper.source.decorator import ConcreteSource


def test_concrete_source_repr():
    """Test the repr of a concrete source."""
    source = ConcreteSource(name="test_source")
    assert repr(source) == f"<Source test_source at {hex(id(source))}>"
