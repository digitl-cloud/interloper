"""Tests for ``interloper.extension.base``."""

from __future__ import annotations

import threading
import time
from unittest import mock

import pytest

from interloper.extension import Extension, ExtensionEntry, ExtensionFailure, Manifests
from interloper.extension import base as extension_base
from interloper.registry import Registry

ACME = """\
manifest_version: 1
key: acme
name: Acme
description: "Acme things: runners and widgets."
icon: i-lucide-box
runners:
  ray: acme_ext.runners:RayRunner
widgets:
  - acme_ext.widgets:SPINNER
gadgets: acme_ext.gadgets
"""


class TestReading:
    def test_each_section_holds_its_entries_in_declaration_order(self, install):
        install("acme_ext", ACME)
        manifests = Manifests()

        assert manifests.sections() == ("gadgets", "runners", "widgets")
        assert manifests.entries("runners") == (
            ExtensionEntry("acme_ext", "acme", "runners", "ray", "acme_ext.runners:RayRunner"),
        )
        assert manifests.entries("widgets") == (
            ExtensionEntry("acme_ext", "acme", "widgets", "acme_ext.widgets:SPINNER", "acme_ext.widgets:SPINNER"),
        )
        assert manifests.entries("gadgets") == (
            ExtensionEntry("acme_ext", "acme", "gadgets", "acme", "acme_ext.gadgets"),
        )
        assert manifests.failures == ()

    def test_sections_merge_across_manifests_in_the_order_the_packages_are_found(self, install):
        install("acme_ext", ACME)
        install("other_ext", "manifest_version: 1\nkey: other\nrunners:\n  dask: other_ext:DaskRunner\n")

        assert [entry.name for entry in Manifests().entries("runners")] == ["ray", "dask"]

    def test_a_section_no_manifest_declares_is_empty(self, install):
        install("acme_ext", ACME)
        assert Manifests().entries("launchers") == ()

    def test_the_manifest_is_read_without_importing_the_package(self, install):
        install("acme_ext", ACME, __init__="raise RuntimeError('imported')")
        assert Manifests().sections() == ("gadgets", "runners", "widgets")

    def test_the_manifests_are_read_once(self, install):
        install("acme_ext", ACME)
        manifests = Manifests()
        manifests.sections()
        with mock.patch.object(extension_base, "entry_points") as locators:
            manifests.entries("runners")
        locators.assert_not_called()

    def test_a_failure_to_read_the_locators_is_retried(self, install):
        install("acme_ext", ACME)
        manifests = Manifests()
        with (
            mock.patch.object(extension_base, "entry_points", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            manifests.sections()
        assert manifests.sections() == ("gadgets", "runners", "widgets")


class TestFailures:
    @pytest.mark.parametrize(
        ("manifest", "error"),
        [
            ("- not\n- a mapping\n", "extension.yaml is not a mapping"),
            ("manifest_version: 2\nkey: broken\n", "manifest_version is 2, expected 1"),
            ("key: broken\n", "manifest_version is None, expected 1"),
            ("manifest_version: 1\n", "key None is not a lowercase identifier"),
            ("manifest_version: 1\nkey: Broken-Key\n", "key 'Broken-Key' is not a lowercase identifier"),
            ("manifest_version: 1\nkey: broken\nname: [a, list]\n", "name is not a string"),
            ("manifest_version: 1\nkey: broken\n1: [a.b]\n", "section name 1 is not a string"),
            ("manifest_version: 1\nkey: broken\nrunners: {ray: [", "while parsing a flow node"),
        ],
        ids=["not-a-mapping", "other-version", "no-version", "no-key", "malformed-key", "name", "section", "yaml"],
    )
    def test_a_manifest_that_cannot_be_read_is_skipped_and_recorded(self, install, manifest, error):
        install("broken_ext", manifest)
        install("acme_ext", ACME)
        manifests = Manifests()

        assert manifests.sections() == ("gadgets", "runners", "widgets")
        (failure,) = manifests.failures
        assert (failure.package, failure.section, failure.entry) == ("broken_ext", None, None)
        assert error in failure.error

    def test_a_package_shipping_no_manifest_is_skipped_and_recorded(self, install):
        install("bare_ext", None)
        assert Manifests().failures == (
            ExtensionFailure("bare_ext", None, None, "package 'bare_ext' ships no extension.yaml"),
        )

    def test_a_locator_naming_no_package_is_skipped_and_recorded(self, install):
        install("acme_ext", ACME)
        with mock.patch.object(
            extension_base,
            "entry_points",
            return_value=[mock.Mock(module="missing_ext"), mock.Mock(module="acme_ext")],
        ):
            manifests = Manifests()
            assert manifests.sections() == ("gadgets", "runners", "widgets")
        assert manifests.failures == (
            ExtensionFailure("missing_ext", None, None, "'missing_ext' is not an importable package"),
        )

    def test_a_key_is_declared_once(self, install):
        install("acme_ext", ACME)
        install("copy_ext", "manifest_version: 1\nkey: acme\nrunners:\n  dask: copy_ext:DaskRunner\n")
        manifests = Manifests()

        assert [entry.name for entry in manifests.entries("runners")] == ["ray"]
        assert manifests.failures == (
            ExtensionFailure("copy_ext", None, None, "key 'acme' is already declared by package 'acme_ext'"),
        )

    def test_a_section_of_another_shape_is_skipped_and_recorded_and_the_rest_kept(self, install):
        install("acme_ext", "manifest_version: 1\nkey: acme\nrunners: 3\nwidgets: [acme_ext:SPINNER]\n")
        manifests = Manifests()

        assert manifests.sections() == ("widgets",)
        (failure,) = manifests.failures
        assert (failure.package, failure.section, failure.entry) == ("acme_ext", "runners", None)

    def test_recording_warns_with_what_was_skipped(self, caplog):
        Manifests().record(ExtensionFailure("acme_ext", "runners", "ray", "failed to load: ImportError: no ray"))
        assert "Skipping package 'acme_ext', section 'runners', entry 'ray': failed to load: ImportError: no ray" in (
            caplog.text
        )


class TestExtensions:
    def test_each_package_is_listed_as_its_manifest_says_it_is_sorted_by_package(self, install):
        install("other_ext", "manifest_version: 1\nkey: other\n")
        install("acme_ext", ACME)

        assert Manifests().extensions() == (
            Extension("acme_ext", None, "acme", "Acme", "Acme things: runners and widgets.", "i-lucide-box"),
            Extension("other_ext", None, "other"),
        )

    def test_a_package_whose_manifest_cannot_be_read_is_listed_without_a_key(self, install):
        install("bare_ext", None)
        install("taken_ext", "manifest_version: 1\nkey: bare\n")
        install("copy_ext", "manifest_version: 1\nkey: bare\n")

        assert Manifests().extensions() == (
            Extension("bare_ext", None),
            Extension("copy_ext", None),
            Extension("taken_ext", None, "bare"),
        )

    def test_the_version_is_the_distribution_naming_the_package(self, install):
        install("acme_ext", ACME)
        locator = mock.Mock(module="acme_ext", dist=mock.Mock(version="1.2.3"))
        with mock.patch.object(extension_base, "entry_points", return_value=[locator]):
            (extension,) = Manifests().extensions()

        assert extension.version == "1.2.3"


class TestUnclaimed:
    def test_a_section_no_registry_claimed_is_named_with_its_package(self, install):
        install("acme_ext", ACME)
        manifests = Manifests()
        manifests.claim("runners")
        manifests.claim("widgets")

        assert manifests.unclaimed() == (("acme_ext", "gadgets"),)


class TestThreadSafety:
    def test_concurrent_first_reads_read_the_manifests_once_and_see_them_whole(self, install):
        install("acme_ext", ACME)
        locate = extension_base.entry_points
        manifests = Manifests()
        results: dict[str, tuple[str, ...]] = {}

        def slow_locate(**kwargs):
            time.sleep(0.05)
            return locate(**kwargs)

        def read(reader: str) -> None:
            results[reader] = manifests.sections()

        with mock.patch.object(extension_base, "entry_points", side_effect=slow_locate) as locators:
            first = threading.Thread(target=read, args=("first",))
            second = threading.Thread(target=read, args=("second",))
            first.start()
            time.sleep(0.01)  # let the first reader take the lock
            second.start()
            first.join()
            second.join()

        locators.assert_called_once()
        assert results == {"first": ("gadgets", "runners", "widgets"), "second": ("gadgets", "runners", "widgets")}


class TestThroughARegistry:
    def test_a_broken_entry_is_skipped_with_a_recorded_failure_while_everything_else_loads(self, install, monkeypatch):
        install(
            "acme_ext",
            "manifest_version: 1\nkey: acme\nthings:\n"
            "  good: acme_ext.things:GOOD\n"
            "  broken: acme_ext.missing:BROKEN\n",
            things="GOOD = 'good'\n",
        )
        install(
            "other_ext",
            "manifest_version: 1\nkey: other\nthings:\n  more: other_ext:MORE\n",
            __init__="MORE = 'more'\n",
        )
        manifests = Manifests()
        monkeypatch.setattr("interloper.registry.base.MANIFESTS", manifests)
        registry: Registry[str] = Registry("things")

        assert registry.items() == (("good", "good"), ("more", "more"))
        (failure,) = manifests.failures
        assert (failure.package, failure.section, failure.entry) == ("acme_ext", "things", "broken")
        assert "No module named 'acme_ext.missing'" in failure.error
        assert manifests.unclaimed() == ()
