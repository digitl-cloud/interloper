"""Tests for ``interloper.utils.imports``."""

from __future__ import annotations

import importlib.util
from typing import Any

import pytest
from typing_extensions import Self

import interloper as il
from interloper.utils.imports import get_object_path, import_from_path, require_import


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

    def test_a_matching_target_type_passes(self) -> None:
        assert import_from_path(f"{MODULE}.Marker", type) is Marker

    def test_a_mismatched_target_type_is_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"is not a Source"):
            import_from_path(f"{MODULE}.Marker", il.Source)

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


class TestRequireImport:
    """The decorator that defers an ``ImportError`` until first use."""

    def test_a_present_package_lets_a_function_run(self) -> None:
        @require_import("json", "json is required")
        def wrapped() -> str:
            return "ran"

        assert wrapped() == "ran"

    def test_a_missing_package_fails_at_call_time_not_decoration(self) -> None:
        @require_import("interloper_not_a_package", "install the thing")
        def wrapped() -> str:
            return "ran"

        with pytest.raises(ImportError, match="install the thing"):
            wrapped()

    def test_a_present_package_lets_a_class_instantiate(self) -> None:
        @require_import("json", "json is required")
        class Guarded:
            """Plain class whose ``__new__`` is object's."""

        assert isinstance(Guarded(), Guarded)

    def test_a_missing_package_fails_at_instantiation(self) -> None:
        @require_import("interloper_not_a_package", "install the thing")
        class Guarded:
            """Plain class whose instantiation is guarded."""

        with pytest.raises(ImportError, match="install the thing"):
            Guarded()

    def test_a_custom_new_is_still_called(self) -> None:
        created: list[str] = []

        @require_import("json", "json is required")
        class WithCustomNew:
            """Class with its own ``__new__``, which the guard must preserve."""

            def __new__(cls, *args: Any, **kwargs: Any) -> Self:
                """Record the construction and delegate to ``object``.

                Args:
                    *args: Ignored positional arguments.
                    **kwargs: Ignored keyword arguments.

                Returns:
                    The new instance.
                """
                created.append("new")
                return super().__new__(cls)

        WithCustomNew()

        assert created == ["new"]

    def test_the_guard_is_re_evaluated_on_every_use(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The check is deferred, not cached: an extra installed later works
        # without re-importing the decorated module.
        @require_import("json", "json is required")
        def wrapped() -> str:
            return "ran"

        monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
        with pytest.raises(ImportError):
            wrapped()

        monkeypatch.undo()
        assert wrapped() == "ran"
