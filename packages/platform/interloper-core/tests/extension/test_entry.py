"""Tests for ``interloper.extension.entry``."""

from __future__ import annotations

import json

import pytest

from interloper.extension import ExtensionEntry


class TestFromSection:
    def test_a_single_import_string_is_named_by_the_extension_key(self):
        assert ExtensionEntry.from_section("acme_ext", "acme", "components", "acme_ext") == (
            ExtensionEntry("acme_ext", "acme", "components", "acme", "acme_ext"),
        )

    def test_a_list_entry_is_named_by_its_import_string(self):
        assert ExtensionEntry.from_section("acme_ext", "acme", "kinds", ["acme_ext:Widget", "acme_ext:Gadget"]) == (
            ExtensionEntry("acme_ext", "acme", "kinds", "acme_ext:Widget", "acme_ext:Widget"),
            ExtensionEntry("acme_ext", "acme", "kinds", "acme_ext:Gadget", "acme_ext:Gadget"),
        )

    def test_a_map_entry_is_named_by_its_key(self):
        assert ExtensionEntry.from_section("acme_ext", "acme", "runners", {"ray": "acme_ext:RayRunner"}) == (
            ExtensionEntry("acme_ext", "acme", "runners", "ray", "acme_ext:RayRunner"),
        )

    @pytest.mark.parametrize("value", [3, None, ["acme_ext", 3], {"ray": 3}, {"ray": ["acme_ext"]}])
    def test_any_other_shape_is_refused(self, value):
        with pytest.raises(ValueError, match="an import string, a list of them, or a map of names to them"):
            ExtensionEntry.from_section("acme_ext", "acme", "runners", value)


class TestLoad:
    def test_a_module_path_loads_the_module(self):
        assert ExtensionEntry("std", "std", "things", "json", "json").load() is json

    def test_an_attribute_path_loads_the_attribute(self):
        assert ExtensionEntry("std", "std", "things", "dumps", "json:dumps").load() is json.dumps

    def test_a_missing_module_raises(self):
        with pytest.raises(ModuleNotFoundError):
            ExtensionEntry("std", "std", "things", "nope", "no_such_module:thing").load()
