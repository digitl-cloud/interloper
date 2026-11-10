"""Tests for ``interloper.utils.imports``."""

from __future__ import annotations

import pytest

from interloper.utils import get_object_path, import_from_path


class Marker:
    """Target class for the import-path helpers."""

    class Nested:
        """Nested target, reached through an attribute chain."""


def marker_function() -> str:
    """Target function for the import-path helpers.

    Returns:
        A fixed string.
    """
    return "called"


MODULE = Marker.__module__


class TestImportFromPath:
    """Both supported path forms."""

    def test_dotted_path(self) -> None:
        assert import_from_path(f"{MODULE}.Marker") is Marker

    def test_composite_path(self) -> None:
        assert import_from_path(f"{MODULE}:Marker") is Marker

    def test_composite_path_walks_an_attribute_chain(self) -> None:
        # The form ``Asset.classpath`` emits, so a source-owned asset is
        # reachable without instantiating its parent source.
        assert import_from_path(f"{MODULE}:Marker.Nested") is Marker.Nested

    def test_an_unknown_module_raises(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import_from_path("interloper_not_a_module.Thing")

    def test_an_unknown_attribute_raises(self) -> None:
        with pytest.raises(AttributeError):
            import_from_path(f"{MODULE}.NotDefinedHere")


class TestGetObjectPath:
    """The inverse of the dotted form."""

    def test_a_class_round_trips(self) -> None:
        path = get_object_path(Marker)

        assert path == f"{MODULE}.Marker"
        assert import_from_path(path) is Marker

    def test_a_function_round_trips(self) -> None:
        path = get_object_path(marker_function)

        assert import_from_path(path) is marker_function
