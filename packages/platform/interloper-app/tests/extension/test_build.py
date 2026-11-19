"""Tests for ``interloper_app.extension.build``."""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from interloper_app.extension.project import Project

WHEEL = "dist/acme_interloper-0.1.0-py3-none-any.whl"
VITE_MANIFEST = "acme_interloper/static/.vite/manifest.json"


@pytest.fixture
def with_ui(extension_project, uv, extension):
    """The ``acme-interloper`` extension once ``add ui`` ran, uv recorded afresh.

    Args:
        extension_project: The extension.
        uv: Records uv's commands; cleared of the ones ``add`` ran.
        extension: Runs ``interloper extension``.

    Returns:
        The project's directory.
    """
    extension("add", "ui")
    uv.clear()
    return extension_project


def _fail_on(failing: tuple[str, ...], error: Exception, calls: list):
    def run(project, *args):
        calls.append(args)
        if args[: len(failing)] == failing:
            raise error

    return run


class TestBuild:
    def test_build_without_a_ui_runs_uv_build_only(self, extension_project, uv, npm, extension, capsys):
        extension("build")

        assert npm == []
        assert [call[0] for call in uv] == ["build"]
        assert f"Built {WHEEL}, dist/acme_interloper-0.1.0.tar.gz." in capsys.readouterr().out

    def test_build_with_a_ui_installs_checks_and_builds_it_first(self, with_ui, uv, npm, extension, capsys):
        extension("build")

        assert npm == [("install",), ("run", "typecheck"), ("run", "build")]
        assert [call[0] for call in uv] == ["build"]
        with zipfile.ZipFile(with_ui / WHEEL) as wheel:
            assert VITE_MANIFEST in wheel.namelist()
        assert WHEEL in capsys.readouterr().out

    def test_uv_build_writes_into_a_directory_of_its_own(self, extension_project, uv, npm, extension):
        extension("build")

        ((command, option, directory),) = uv
        assert (command, option) == ("build", "--out-dir")
        assert not Path(directory).is_relative_to(extension_project / "dist")

    def test_a_version_uv_normalizes_is_found(self, extension_project, uv, npm, extension, capsys):
        pyproject = extension_project / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace('version = "0.1.0"', 'version = "0.2.0-rc.1"'))

        extension("build")

        assert "Built dist/acme_interloper-0.2.0rc1-py3-none-any.whl" in capsys.readouterr().out
        assert (extension_project / "dist" / "acme_interloper-0.2.0rc1-py3-none-any.whl").is_file()

    def test_a_rebuild_replaces_the_files_of_the_same_version(self, with_ui, uv, npm, extension):
        (with_ui / "dist").mkdir()
        (with_ui / WHEEL).write_bytes(b"stale")

        extension("build")

        with zipfile.ZipFile(with_ui / WHEEL) as wheel:
            assert VITE_MANIFEST in wheel.namelist()

    def test_a_lockfile_is_installed_with_npm_ci(self, with_ui, uv, npm, extension):
        (with_ui / "ui" / "package-lock.json").write_text("{}\n")

        extension("build")

        assert npm[0] == ("ci",)


class TestRefusals:
    def test_a_wheel_without_the_bundle_is_refused_and_never_reaches_dist(
        self, with_ui, uv, extension, capsys, monkeypatch
    ):
        monkeypatch.setattr(Project, "_npm", lambda project, *args: None)

        with pytest.raises(SystemExit) as exit_info:
            extension("build")

        assert exit_info.value.code == 1
        assert f"acme_interloper-0.1.0-py3-none-any.whl lacks {VITE_MANIFEST}" in capsys.readouterr().err
        assert not (with_ui / "dist").exists()

    def test_the_wheel_checked_is_the_one_just_built(self, with_ui, uv, extension, capsys, monkeypatch):
        (with_ui / "dist").mkdir()
        with zipfile.ZipFile(with_ui / "dist" / "acme_interloper-0.0.9-py3-none-any.whl", "w") as stale:
            stale.writestr(VITE_MANIFEST, "{}")
        monkeypatch.setattr(Project, "_npm", lambda project, *args: None)

        with pytest.raises(SystemExit):
            extension("build")

        assert f"acme_interloper-0.1.0-py3-none-any.whl lacks {VITE_MANIFEST}" in capsys.readouterr().err

    def test_a_failing_npm_is_one_line(self, with_ui, uv, extension, capsys, monkeypatch):
        calls: list = []
        failure = subprocess.CalledProcessError(2, ["npm", "run", "typecheck"])
        monkeypatch.setattr(Project, "_npm", _fail_on(("run", "typecheck"), failure, calls))

        with pytest.raises(SystemExit) as exit_info:
            extension("build")

        assert exit_info.value.code == 1
        (line,) = capsys.readouterr().err.splitlines()
        assert "npm run typecheck failed in ui/" in line
        assert calls == [("install",), ("run", "typecheck")]
        assert uv == []

    def test_npm_missing_says_the_ui_needs_node(self, with_ui, uv, extension, capsys, monkeypatch):
        monkeypatch.setattr(Project, "_npm", _fail_on(("install",), FileNotFoundError("npm"), []))

        with pytest.raises(SystemExit):
            extension("build")

        assert "npm install could not run: the UI needs Node and npm on PATH" in capsys.readouterr().err
        assert uv == []

    def test_a_failing_uv_build_is_one_line(self, extension_project, extension, capsys, monkeypatch):
        failure = subprocess.CalledProcessError(2, ["uv", "build"])
        monkeypatch.setattr(Project, "_uv", _fail_on(("build",), failure, []))

        with pytest.raises(SystemExit):
            extension("build")

        (line,) = capsys.readouterr().err.splitlines()
        assert "uv build failed" in line

    def test_a_build_without_a_wheel_is_refused(self, extension_project, extension, capsys, monkeypatch):
        def sdist_only(project, *args):
            output = Path(args[args.index("--out-dir") + 1])
            (output / "acme_interloper-0.1.0.tar.gz").write_bytes(b"")

        monkeypatch.setattr(Project, "_uv", sdist_only)

        with pytest.raises(SystemExit) as exit_info:
            extension("build")

        assert exit_info.value.code == 1
        assert "uv build wrote no wheel" in capsys.readouterr().err
        assert not (extension_project / "dist").exists()

    def test_build_refuses_before_init(self, acme, uv, npm, extension, capsys):
        with pytest.raises(SystemExit):
            extension("build")

        assert "run uv run interloper extension init first" in capsys.readouterr().err
        assert (uv, npm) == ([], [])

    def test_build_refuses_a_declared_ui_without_its_folder(self, with_ui, uv, npm, extension, capsys):
        (with_ui / "ui" / "package.json").unlink()

        with pytest.raises(SystemExit):
            extension("build")

        assert "extension.yaml declares ui, but ui/package.json is missing" in capsys.readouterr().err
        assert (uv, npm) == ([], [])

    def test_windows_is_refused(self, extension_project, uv, npm, extension, capsys, monkeypatch):
        monkeypatch.setattr(sys, "platform", "win32")

        with pytest.raises(SystemExit):
            extension("build")

        assert "on Windows, run it in WSL" in capsys.readouterr().err
        assert (uv, npm) == ([], [])
