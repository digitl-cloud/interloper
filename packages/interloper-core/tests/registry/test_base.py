"""Tests for ``interloper.registry.base``."""

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
