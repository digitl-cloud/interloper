"""Tests for ``interloper_app.extension.check``."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

import pytest
from interloper.extension import Manifests

from interloper_app.extension import ExtensionCommand
from interloper_app.extension.check import CheckCommand


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
    def test_the_check_is_a_subcommand_of_extension(self):
        parser = argparse.ArgumentParser()
        ExtensionCommand().attach(parser.add_subparsers(dest="command"), "extension")

        assert isinstance(parser.parse_args(["extension", "check"]).handler.__self__, CheckCommand)

    def test_a_clean_installation_passes(self, install, manifests, capsys):
        install("acme_ext", "manifest_version: 1\nkey: acme\nrunners:\n  ray: acme_ext:RAY\n", __init__="RAY = 1\n")

        CheckCommand().run(argparse.Namespace())

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
            CheckCommand().run(argparse.Namespace())

        assert exit_info.value.code == 1
        lines = capsys.readouterr().out.splitlines()
        assert lines[0] == "package 'bare_ext': package 'bare_ext' ships no extension.yaml"
        assert lines[1].startswith("package 'acme_ext', section 'widgets': a section holds an import string")
        assert lines[2].startswith(
            "package 'acme_ext', section 'runners', entry 'dask' (acme_ext.missing:DASK): ModuleNotFoundError"
        )
        assert lines[3] == "package 'acme_ext', section 'runner': no registry reads this section"
        assert lines[4] == "Checked 3 entries in 2 sections: 4 problem(s)."

    def test_a_section_only_the_api_reads_is_claimed(self, tmp_path):
        # A fresh interpreter: the registries in this one were constructed, and claimed, by earlier imports.
        package = tmp_path / "acme_ext"
        package.mkdir()
        (package / "__init__.py").write_text("from fastapi import APIRouter\n\nrouter = APIRouter()\n")
        (package / "extension.yaml").write_text("manifest_version: 1\nkey: acme\nroutes: acme_ext:router\n")
        metadata = tmp_path / "acme_ext-0.1.0.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text("Metadata-Version: 2.1\nName: acme-ext\nVersion: 0.1.0\n")
        (metadata / "entry_points.txt").write_text("[interloper.extensions]\nacme = acme_ext\n")
        probe = "import sys\nsys.argv[1:] = ['extension', 'check']\nfrom interloper.cli.main import main\nmain()"

        completed = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            check=False,
            cwd=tmp_path,
            env={**os.environ, "PYTHONPATH": str(tmp_path)},
        )

        assert completed.stdout.endswith(": no problems.\n"), completed.stdout
