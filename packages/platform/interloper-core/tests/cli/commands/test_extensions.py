"""Tests for ``interloper.cli.commands.extensions``."""

from __future__ import annotations

import argparse

import pytest

from interloper.cli.commands.extensions import ExtensionsCheckCommand, ExtensionsCommand
from interloper.extension import Manifests


@pytest.fixture
def manifests(monkeypatch: pytest.MonkeyPatch) -> Manifests:
    """Swap in manifests read from the packages the test installs.

    Args:
        monkeypatch: Restores the process's manifests afterwards.

    Returns:
        The manifests the check reads, with ``runners`` claimed as a registry
        in the process would.
    """
    manifests = Manifests()
    manifests.claim("runners")
    monkeypatch.setattr("interloper.extension.MANIFESTS", manifests)
    return manifests


class TestCheck:
    def test_the_check_is_a_subcommand_of_extensions(self):
        parser = argparse.ArgumentParser()
        ExtensionsCommand().attach(parser.add_subparsers(dest="command"), "extensions")

        assert isinstance(parser.parse_args(["extensions", "check"]).handler.__self__, ExtensionsCheckCommand)

    def test_a_clean_installation_passes(self, install, manifests, capsys):
        install("acme_ext", "manifest_version: 1\nkey: acme\nrunners:\n  ray: acme_ext:RAY\n", __init__="RAY = 1\n")

        ExtensionsCheckCommand().run(argparse.Namespace())

        assert capsys.readouterr().out == "Checked 1 entries in 1 sections: no problems.\n"

    def test_what_fails_and_what_nothing_reads_are_named_and_fail_the_check(self, install, manifests, capsys):
        install("bare_ext", None)
        install(
            "acme_ext",
            "manifest_version: 1\nkey: acme\nrunners:\n  ray: acme_ext:RAY\n  dask: acme_ext.missing:DASK\n"
            "runner:\n  spark: acme_ext:RAY\nwidgets: 3\n",
            __init__="RAY = 1\n",
        )

        with pytest.raises(SystemExit) as exit_info:
            ExtensionsCheckCommand().run(argparse.Namespace())

        assert exit_info.value.code == 1
        lines = capsys.readouterr().out.splitlines()
        assert lines[0] == "package 'bare_ext': package 'bare_ext' ships no extension.yaml"
        assert lines[1].startswith("package 'acme_ext', section 'widgets': a section holds an import string")
        assert lines[2].startswith(
            "package 'acme_ext', section 'runners', entry 'dask' (acme_ext.missing:DASK): ModuleNotFoundError"
        )
        assert lines[3] == "package 'acme_ext', section 'runner': no registry reads this section"
        assert lines[4] == "Checked 3 entries in 2 sections: 4 problem(s)."
