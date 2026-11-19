"""Tests for ``interloper_app.extension.project``."""

from __future__ import annotations

import subprocess
import tomllib
from importlib.metadata import version

import pytest

from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.project import Project


class TestFromDirectory:
    def test_a_library_project_is_read(self, library, tmp_path):
        project = Project.from_directory(library())

        assert project.root == tmp_path
        assert project.distribution == "acme-interloper"
        assert project.package == "acme_interloper"
        assert project.description == "Acme sources."
        assert project.package_directory == tmp_path / "src" / "acme_interloper"
        assert project.manifest_path == tmp_path / "src" / "acme_interloper" / "extension.yaml"
        assert project.default_key == "acme"
        assert not project.declares_extension

    @pytest.mark.parametrize(
        ("name", "key"),
        [("acme-interloper", "acme"), ("interloper-acme", "acme"), ("acme", "acme"), ("interloper", "interloper")],
    )
    def test_the_key_drops_the_interloper_affix(self, library, tmp_path, name, key):
        assert Project.from_directory(library(name)).default_key == key

    def test_a_module_name_set_for_uv_build_is_the_package(self, library, tmp_path):
        library()
        (tmp_path / "src" / "acme_interloper").rename(tmp_path / "src" / "acme")
        with (tmp_path / "pyproject.toml").open("a") as pyproject:
            pyproject.write('\n[tool.uv.build-backend]\nmodule-name = "acme"\n')

        assert Project.from_directory(tmp_path).package == "acme"

    def test_a_directory_without_a_project_is_refused(self, library, tmp_path):
        with pytest.raises(ExtensionError, match=r"no pyproject\.toml here: create the project with uv init --lib"):
            Project.from_directory(tmp_path)

    def test_a_pyproject_that_is_not_toml_is_refused(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text("[project\n")

        with pytest.raises(ExtensionError, match=r"pyproject\.toml is not valid TOML"):
            Project.from_directory(tmp_path)

    def test_an_app_layout_is_refused(self, library, tmp_path):
        (tmp_path / "main.py").write_text("print('hi')\n")
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "acme"\nversion = "0.1.0"\n')

        with pytest.raises(ExtensionError, match="is not a library project: create it with uv init --lib"):
            Project.from_directory(tmp_path)

    def test_a_flat_layout_is_refused(self, library, tmp_path):
        library()
        (tmp_path / "src" / "acme_interloper").rename(tmp_path / "acme_interloper")

        with pytest.raises(ExtensionError, match="is not a library project: create it with uv init --lib"):
            Project.from_directory(tmp_path)


class TestEdits:
    def test_the_entry_point_is_appended_and_declares_the_extension(self, library, tmp_path):
        project = Project.from_directory(library())
        before = (tmp_path / "pyproject.toml").read_text()

        project.add_entry_point("acme")

        after = (tmp_path / "pyproject.toml").read_text()
        assert after.startswith(before)
        assert tomllib.loads(after)["project"]["entry-points"]["interloper.extensions"] == {"acme": "acme_interloper"}
        assert project.declares_extension

    def test_an_entry_point_toml_cannot_hold_is_refused(self, library, tmp_path):
        library()
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace("dependencies = []", "dependencies = []\nentry-points = {}"))
        before = pyproject.read_text()

        with pytest.raises(ExtensionError, match=r"add it to pyproject\.toml by hand"):
            Project.from_directory(tmp_path).add_entry_point("acme")

        assert pyproject.read_text() == before

    def test_a_manifest_declares_the_extension(self, library, tmp_path):
        project = Project.from_directory(library())

        project.manifest_path.write_text("manifest_version: 1\nkey: acme\n")

        assert project.declares_extension

    def test_ignore_appends_only_missing_patterns(self, library, tmp_path):
        project = Project.from_directory(library())
        (tmp_path / ".gitignore").write_text("# venvs\n.venv")

        project.ignore("interloper.yaml", ".venv")
        project.ignore("interloper.yaml")

        assert (tmp_path / ".gitignore").read_text() == "# venvs\n.venv\ninterloper.yaml\n"

    def test_ignore_creates_the_file(self, library, tmp_path):
        Project.from_directory(library()).ignore("interloper.yaml")

        assert (tmp_path / ".gitignore").read_text() == "interloper.yaml\n"


class TestRequire:
    def test_a_missing_dependency_is_added_at_the_installed_version(self, library, tmp_path, uv):
        Project.from_directory(library()).require("interloper-api")

        assert uv == [("add", f"interloper-api>={version('interloper-api')}")]

    def test_a_dependency_the_project_names_is_not_added_again(self, library, tmp_path, uv):
        project = Project.from_directory(library(dependencies='"Interloper_API>=0.1"'))

        project.require("interloper-api")

        assert uv == []

    def test_a_failing_uv_is_a_refusal_naming_the_command(self, library, tmp_path, monkeypatch):
        def fail(self, *args):
            raise subprocess.CalledProcessError(2, ["uv", *args])

        monkeypatch.setattr(Project, "_uv", fail)

        with pytest.raises(ExtensionError, match=r"uv add interloper-api>=\S+ failed"):
            Project.from_directory(library()).require("interloper-api")

    def test_uv_runs_in_the_projects_directory_and_fails_loudly(self, library, monkeypatch):
        calls = []
        monkeypatch.setattr(subprocess, "run", lambda command, **options: calls.append((command, options)))
        project = Project.from_directory(library())

        project._uv("add", "interloper-api>=1")

        assert calls == [(["uv", "add", "interloper-api>=1"], {"cwd": project.root, "check": True})]

    def test_npm_runs_in_the_ui_folder_and_fails_loudly(self, library, monkeypatch):
        calls = []
        monkeypatch.setattr(subprocess, "run", lambda command, **options: calls.append((command, options)))
        project = Project.from_directory(library())

        project._npm("run", "build")

        assert calls == [(["npm", "run", "build"], {"cwd": project.root / "ui", "check": True})]
