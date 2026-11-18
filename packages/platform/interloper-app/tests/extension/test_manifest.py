"""Tests for ``interloper_app.extension.manifest``."""

from __future__ import annotations

from pathlib import Path

import pytest
from interloper.extension import ExtensionEntry

from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.manifest import ManifestFile, Shape

SETTINGS = ("settings", "map", "acme", "acme_interloper.settings.acme:AcmeSettings")
SOURCE = ("components", "list", "acme", "acme_interloper.acme.source:Acme")
ROUTES = ("routes", "single", "acme", "acme_interloper.routes:router")


def _manifest(text: str) -> ManifestFile:
    return ManifestFile(Path("extension.yaml"), text)


def _entry(section: str, shape: Shape, name: str, path: str) -> tuple[ExtensionEntry, Shape]:
    return ExtensionEntry("acme_interloper", "acme", section, path if shape == "list" else name, path), shape


class TestMissingSection:
    def test_a_map_section_is_appended(self):
        edited = _manifest("key: acme\n").with_entry(*_entry(*SETTINGS))

        assert edited.text == "key: acme\nsettings:\n  acme: acme_interloper.settings.acme:AcmeSettings\n"

    def test_a_list_section_is_appended(self):
        edited = _manifest("key: acme\n").with_entry(*_entry(*SOURCE))

        assert edited.text == "key: acme\ncomponents:\n  - acme_interloper.acme.source:Acme\n"

    def test_a_single_section_is_appended(self):
        edited = _manifest("key: acme\n").with_entry(*_entry(*ROUTES))

        assert edited.text == "key: acme\nroutes: acme_interloper.routes:router\n"

    def test_an_empty_manifest_gets_the_section(self):
        edited = _manifest("").with_entry(*_entry(*ROUTES))

        assert edited.text == "routes: acme_interloper.routes:router\n"

    def test_a_text_without_a_final_newline_gets_one_first(self):
        edited = _manifest("key: acme").with_entry(*_entry(*ROUTES))

        assert edited.text == "key: acme\nroutes: acme_interloper.routes:router\n"

    def test_a_trailing_comment_stays_above_the_new_section(self):
        edited = _manifest("key: acme\n# the end\n").with_entry(*_entry(*ROUTES))

        assert edited.text == "key: acme\n# the end\nroutes: acme_interloper.routes:router\n"


class TestExistingSection:
    def test_a_list_grows_after_its_last_item(self):
        text = "components:\n  - a:A\n  # keep\n  - b:B\n\n# tail\nkey: acme\n"

        edited = _manifest(text).with_entry(*_entry("components", "list", "c", "c:C"))

        assert edited.text == "components:\n  - a:A\n  # keep\n  - b:B\n  - c:C\n\n# tail\nkey: acme\n"

    def test_a_list_ending_the_file_with_a_comment_grows_above_the_comment(self):
        edited = _manifest("key: acme\ncomponents:\n  - a:A\n# the end\n").with_entry(
            *_entry("components", "list", "c", "c:C")
        )

        assert edited.text == "key: acme\ncomponents:\n  - a:A\n  - c:C\n# the end\n"

    def test_a_last_item_without_a_final_newline_is_ended_first(self):
        edited = _manifest("components:\n  - a:A").with_entry(*_entry("components", "list", "c", "c:C"))

        assert edited.text == "components:\n  - a:A\n  - c:C\n"

    def test_a_map_grows_with_its_own_indentation(self):
        text = "key: acme\ncli:\n    one: a:One\n    two: a:Two\nsettings:\n    acme: a:Acme\n"

        edited = _manifest(text).with_entry(*_entry("cli", "map", "three", "a:Three"))

        assert edited.text == (
            "key: acme\ncli:\n    one: a:One\n    two: a:Two\n    three: a:Three\nsettings:\n    acme: a:Acme\n"
        )

    def test_a_list_at_column_zero_keeps_its_style(self):
        edited = _manifest("components:\n- a:A\nkey: acme\n").with_entry(*_entry("components", "list", "c", "c:C"))

        assert edited.text == "components:\n- a:A\n- c:C\nkey: acme\n"

    def test_a_section_with_no_entry_gets_its_first(self):
        edited = _manifest("key: acme\ncli:\nname: Acme\n").with_entry(*_entry("cli", "map", "sync", "a:Sync"))

        assert edited.text == "key: acme\ncli:\n  sync: a:Sync\nname: Acme\n"


class TestLineEndings:
    def test_windows_line_endings_are_kept(self):
        text = "key: acme\r\ncomponents:\r\n  - a:A\r\n"

        edited = _manifest(text).with_entry(*_entry("components", "list", "c", "c:C")).with_entry(*_entry(*SETTINGS))

        assert edited.text == (
            "key: acme\r\ncomponents:\r\n  - a:A\r\n  - c:C\r\n"
            "settings:\r\n  acme: acme_interloper.settings.acme:AcmeSettings\r\n"
        )

    def test_reading_and_writing_keep_every_byte(self, tmp_path):
        path = tmp_path / "extension.yaml"
        path.write_bytes(b"key: acme\r\n# kept\r\n")

        ManifestFile.read(path).with_entry(*_entry(*ROUTES)).write()

        assert path.read_bytes() == b"key: acme\r\n# kept\r\nroutes: acme_interloper.routes:router\r\n"


class TestRefusals:
    @pytest.mark.parametrize(
        ("text", "entry", "message"),
        [
            ("settings:\n  acme: a:A\n", SETTINGS, "already declares 'acme' under settings"),
            (
                "components:\n  - acme_interloper.acme.source:Acme\n",
                SOURCE,
                "already declares acme_interloper.acme.source:Acme under components",
            ),
            ("routes: a:router\n", ROUTES, "already declares routes"),
            ("components: acme_interloper\n", SOURCE, "components is a single import string"),
            ("settings:\n  - a:A\n", SETTINGS, "settings is a list"),
            ("components: [a:A]\n", SOURCE, "components is written on one line"),
            ("settings: {a: b}\n", SETTINGS, "settings is written on one line"),
            ("components: []\n", SOURCE, "components is written on one line"),
        ],
    )
    def test_what_cannot_be_extended_is_refused(self, text, entry, message):
        manifest = _manifest(text)

        with pytest.raises(ExtensionError, match=message):
            manifest.with_entry(*_entry(*entry))

        assert manifest.text == text

    def test_a_manifest_that_is_not_yaml_is_refused(self):
        with pytest.raises(ExtensionError, match=r"extension\.yaml is not valid YAML"):
            _manifest("key: [acme\n").with_entry(*_entry(*SOURCE))

    def test_a_list_whose_block_holds_no_list_item_is_refused(self):
        with pytest.raises(ExtensionError, match=r"components in extension\.yaml does not start with a list item"):
            _manifest("components:\n  [a:A]\n").with_entry(*_entry(*SOURCE))

    def test_a_section_no_line_opens_is_refused(self):
        with pytest.raises(ExtensionError, match=r"no line of extension\.yaml starts with 'components:'"):
            _manifest('"components":\n  - a:A\n').with_entry(*_entry(*SOURCE))

    def test_a_manifest_that_is_not_a_mapping_is_refused(self):
        with pytest.raises(ExtensionError, match=r"extension\.yaml is not a mapping"):
            _manifest("- a\n").with_entry(*_entry(*SOURCE))


class TestReparse:
    def test_an_edit_that_would_not_read_back_as_the_entry_is_refused(self):
        text = "key: acme\ncli:\n  x: a:X\ncli:\n  y: a:Y\n"
        manifest = _manifest(text)

        with pytest.raises(ExtensionError, match=r"would change more of extension\.yaml than its cli section"):
            manifest.with_entry(*_entry("cli", "map", "acme", "a:Acme"))

        assert manifest.text == text

    @pytest.mark.parametrize(
        ("text", "entry"),
        [
            ("key: acme\n", SETTINGS),
            ("key: acme\n", SOURCE),
            ("key: acme", ROUTES),
            ("components:\n  - a:A\n  # keep\n  - b:B\n\n# tail\nkey: acme\n", SOURCE),
            ("key: acme\ncli:\n    one: a:One\nsettings:\n    other: a:B\n", SETTINGS),
            ("components:\n- a:A\nkey: acme\n", SOURCE),
            ("key: acme\r\ncomponents:\r\n  - a:A\r\n", SOURCE),
            ("key: acme\ncli:\nname: Acme\n", ("cli", "map", "sync", "a:Sync")),
        ],
    )
    def test_every_edit_parses_to_the_document_plus_the_entry(self, text, entry):
        section, shape, name, path = entry
        before = _manifest(text).document
        expected = dict(before)
        if shape == "list":
            expected[section] = [*(before.get(section) or []), path]
        elif shape == "map":
            expected[section] = {**(before.get(section) or {}), name: path}
        else:
            expected[section] = path

        assert _manifest(text).with_entry(*_entry(*entry)).document == expected
