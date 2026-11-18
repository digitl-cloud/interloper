"""Tests for ``interloper_app.extension.base``."""

from __future__ import annotations

from interloper.extension import MANIFESTS

from interloper_app.extension import ExtensionCommand


def test_the_group_is_what_interloper_apps_manifest_declares():
    (entry,) = [entry for entry in MANIFESTS.entries("cli") if entry.name == "extension"]

    assert entry.package == "interloper_app"
    assert entry.load() is ExtensionCommand


def test_core_no_longer_declares_extensions():
    assert "extensions" not in {entry.name for entry in MANIFESTS.entries("cli")}
