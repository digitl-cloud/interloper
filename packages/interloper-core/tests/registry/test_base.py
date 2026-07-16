"""Tests for ``interloper.registry.base``."""

import threading
import time
from unittest import mock

import pytest

from interloper.registry import Registry


class TestRegister:
    def test_register_and_get(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        assert registry.get("alpha") == "A"
        assert registry.get("missing") is None

    def test_first_wins(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "first")
        registry.register("alpha", "second")
        assert registry.get("alpha") == "first"

    def test_contains(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        assert "alpha" in registry
        assert "missing" not in registry


class TestViews:
    def test_sorted_views(self):
        registry: Registry[int] = Registry()
        registry.register("beta", 2)
        registry.register("alpha", 1)
        assert registry.keys() == ("alpha", "beta")
        assert registry.values() == (1, 2)
        assert registry.items() == (("alpha", 1), ("beta", 2))


class TestEntryPoints:
    def _entry_point(self, name: str, obj: object) -> mock.Mock:
        entry = mock.Mock()
        entry.name = name
        entry.load.return_value = obj
        return entry

    def test_lazy_load_on_first_lookup(self):
        registry: Registry[str] = Registry("test.group")
        with mock.patch("interloper.registry.base.entry_points", return_value=[self._entry_point("alpha", "A")]) as eps:
            assert registry.get("alpha") == "A"
            assert registry.keys() == ("alpha",)
        eps.assert_called_once_with(group="test.group")

    def test_no_group_never_loads(self):
        registry: Registry[str] = Registry()
        with mock.patch("interloper.registry.base.entry_points") as eps:
            assert registry.get("anything") is None
        eps.assert_not_called()

    def test_explicit_registration_wins_over_loaded(self):
        registry: Registry[str] = Registry("test.group")
        registry.register("alpha", "explicit")
        with mock.patch("interloper.registry.base.entry_points", return_value=[self._entry_point("alpha", "loaded")]):
            assert registry.get("alpha") == "explicit"

    def test_adopt_transforms_loaded_entries(self):
        registry: Registry[str] = Registry("test.group", adopt=lambda name, obj: (f"{name}!", obj.upper()))
        with mock.patch("interloper.registry.base.entry_points", return_value=[self._entry_point("alpha", "a")]):
            assert registry.get("alpha!") == "A"

    def test_adopt_errors_propagate(self):
        def adopt(name: str, obj: object) -> tuple[str, str]:
            raise TypeError(f"bad entry: {name}")

        registry: Registry[str] = Registry("test.group", adopt=adopt)
        with (
            mock.patch("interloper.registry.base.entry_points", return_value=[self._entry_point("alpha", "a")]),
            pytest.raises(TypeError, match="bad entry: alpha"),
        ):
            registry.get("alpha")

    def test_failed_load_retries_on_next_lookup(self):
        # A transient load failure must not latch the registry empty forever.
        registry: Registry[str] = Registry("test.group")
        with (
            mock.patch("interloper.registry.base.entry_points", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            registry.get("alpha")
        with mock.patch("interloper.registry.base.entry_points", return_value=[self._entry_point("alpha", "A")]):
            assert registry.get("alpha") == "A"


class TestThreadSafety:
    def test_concurrent_first_lookups_never_see_a_partial_registry(self):
        # Regression: a reader arriving while another thread was mid-load used
        # to observe an empty registry (KeyError "'rows' is not registered
        # (available: )") because _loaded flipped before the entries landed.
        registry: Registry[str] = Registry("test.group")

        entry = mock.Mock()
        entry.name = "alpha"
        entry.load.side_effect = lambda: time.sleep(0.05) or "A"

        results: dict[str, object] = {}

        def lookup(tid: str) -> None:
            try:
                results[tid] = registry["alpha"]
            except KeyError as e:
                results[tid] = e

        with mock.patch("interloper.registry.base.entry_points", return_value=[entry]):
            first = threading.Thread(target=lookup, args=("first",))
            second = threading.Thread(target=lookup, args=("second",))
            first.start()
            time.sleep(0.01)  # let the first thread enter the load
            second.start()
            first.join()
            second.join()

        assert results == {"first": "A", "second": "A"}


class TestErrorMessage:
    def test_unknown_name_lists_available(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        with pytest.raises(KeyError, match=r"'missing' is not registered \(available: alpha\)"):
            registry["missing"]

    def test_unknown_name_names_the_entry_point_group(self):
        registry: Registry[str] = Registry("test.group")
        entry = mock.Mock()
        entry.name = "alpha"
        entry.load.return_value = "A"
        with (
            mock.patch("interloper.registry.base.entry_points", return_value=[entry]),
            pytest.raises(KeyError, match=r"'missing' is not registered in entry-point group 'test.group'"),
        ):
            registry["missing"]

    def test_empty_group_hints_at_missing_package(self):
        registry: Registry[str] = Registry("test.group")
        with (
            mock.patch("interloper.registry.base.entry_points", return_value=[]),
            pytest.raises(KeyError, match=r"no entries discovered — is the package declaring it installed\?"),
        ):
            registry["rows"]
