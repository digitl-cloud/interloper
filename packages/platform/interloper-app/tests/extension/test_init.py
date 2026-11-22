"""Tests for ``interloper_app.extension.init``."""

from __future__ import annotations

import subprocess
import tomllib
from importlib.metadata import metadata, version

import pytest
import yaml

from interloper_app.extension.project import Project

MANIFEST = "src/acme_interloper/extension.yaml"


def _entry_points(root) -> dict[str, str]:
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    return pyproject["project"]["entry-points"]["interloper.extensions"]


class TestInit:
    def test_init_writes_the_bare_extension(self, acme, uv, extension, capsys):
        extension("init")

        assert yaml.safe_load((acme / MANIFEST).read_text()) == {
            "manifest_version": 1,
            "key": "acme",
            "name": "Acme",
            "description": "Acme sources.",
        }
        assert _entry_points(acme) == {"acme": "acme_interloper"}
        assert uv == [("add", f"interloper-core>={version('interloper-core')}")]
        assert "interloper.yaml" in (acme / ".gitignore").read_text().splitlines()
        out = capsys.readouterr().out
        assert "uv run interloper extension add <source|connection|settings|command|routes|ui> [name]" in out
        assert "uv run interloper dev" in out

    def test_the_local_configuration_points_at_a_local_database_with_a_key_of_its_own(
        self, acme, library, tmp_path, uv, extension, monkeypatch
    ):
        extension("init")
        other = library(root=tmp_path / "other")
        monkeypatch.chdir(other)
        extension("init")

        config = yaml.safe_load((acme / "interloper.yaml").read_text())
        assert config["postgres"] == {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres",
            "database": "interloper",
        }
        assert set(config) == {"postgres", "encryption"}
        assert len(config["encryption"]["key"]) >= 32
        assert (
            yaml.safe_load((other / "interloper.yaml").read_text())["encryption"]["key"] != config["encryption"]["key"]
        )

    def test_a_newer_python_floor_is_lowered_to_the_one_interloper_core_declares(self, acme, uv, extension, capsys):
        # uv init --lib declares the newest Python on the author's machine, which a deployment may not run.
        pyproject = acme / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace('">=3.11"', '">=3.15"'))

        extension("init")

        floor = metadata("interloper-core")["Requires-Python"]
        assert tomllib.loads(pyproject.read_text())["project"]["requires-python"] == floor
        assert f"requires-python is {floor} now (it was >=3.15)" in capsys.readouterr().out

    def test_the_python_floor_is_set_before_uv_add_resolves(self, acme, extension, monkeypatch):
        # Below interloper-core's floor, uv add could not resolve interloper-core at all.
        pyproject = acme / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace('">=3.11"', '">=3.10"'))
        resolved_for = []
        monkeypatch.setattr(
            Project,
            "_uv",
            lambda project, *args: resolved_for.append(project._pyproject["project"]["requires-python"]),
        )

        extension("init")

        assert resolved_for == [metadata("interloper-core")["Requires-Python"]]

    def test_a_python_floor_that_is_interloper_cores_already_is_not_mentioned(self, acme, uv, extension, capsys):
        extension("init")

        assert "requires-python" not in capsys.readouterr().out

    def test_a_description_yaml_would_misread_is_kept_as_written(self, acme, uv, extension):
        pyproject = acme / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace("Acme sources.", "Acme: sources # and more"))

        extension("init")

        assert yaml.safe_load((acme / MANIFEST).read_text())["description"] == "Acme: sources # and more"

    def test_key_and_name_options(self, acme, uv, extension):
        extension("init", "--key", "widgets", "--name", "Acme Widgets")

        manifest = yaml.safe_load((acme / MANIFEST).read_text())
        assert (manifest["key"], manifest["name"]) == ("widgets", "Acme Widgets")
        assert _entry_points(acme) == {"widgets": "acme_interloper"}

    @pytest.mark.parametrize("key", ["Bad", "1acme", "acme-widgets"])
    def test_a_key_that_is_not_snake_case_is_refused_before_anything_is_written(
        self, acme, uv, extension, tree, capsys, key
    ):
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("init", "--key", key)

        assert exit_info.value.code == 1
        assert f"the key '{key}' is not snake_case" in capsys.readouterr().err
        assert tree(acme) == before
        assert uv == []

    @pytest.mark.parametrize(("key", "package"), [("db", "interloper_db"), ("app", "interloper_app")])
    def test_a_key_another_package_holds_is_refused(self, acme, uv, extension, tree, capsys, key, package):
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("init", "--key", key)

        assert exit_info.value.code == 1
        assert f"the key '{key}' is taken by {package}" in capsys.readouterr().err
        assert tree(acme) == before
        assert uv == []

    def test_a_default_key_another_package_holds_asks_for_another(
        self, library, tmp_path, uv, extension, tree, capsys, monkeypatch
    ):
        root = library("db-interloper", root=tmp_path / "db")
        monkeypatch.chdir(root)
        before = tree(root)

        with pytest.raises(SystemExit):
            extension("init")

        assert "the key 'db' is taken by interloper_db: pick another with --key" in capsys.readouterr().err
        assert tree(root) == before


class TestRefusals:
    def test_init_refuses_an_extension_that_exists(self, acme, uv, extension, tree, capsys):
        extension("init")
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("init")

        assert exit_info.value.code == 1
        assert "acme-interloper already declares an extension" in capsys.readouterr().err
        assert tree(acme) == before

    def test_init_refuses_files_it_would_overwrite(self, acme, uv, extension, tree, capsys):
        (acme / "interloper.yaml").write_text("postgres:\n  host: db.internal\n")
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("init")

        assert exit_info.value.code == 1
        assert "interloper.yaml exists already" in capsys.readouterr().err
        assert tree(acme) == before
        assert uv == []

    def test_a_python_floor_it_cannot_rewrite_is_refused_before_anything_is_written(
        self, acme, uv, extension, tree, capsys
    ):
        pyproject = acme / "pyproject.toml"
        pyproject.write_text(pyproject.read_text().replace('">=3.11"', '""">=3.15"""'))
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("init")

        assert exit_info.value.code == 1
        floor = metadata("interloper-core")["Requires-Python"]
        assert f'set it by hand: requires-python = "{floor}"' in capsys.readouterr().err
        assert tree(acme) == before
        assert uv == []

    def test_a_failing_uv_is_one_line_naming_what_was_written(self, acme, extension, capsys, monkeypatch):
        def fail(self, *args):
            raise subprocess.CalledProcessError(1, ["uv", *args])

        monkeypatch.setattr(Project, "_uv", fail)

        with pytest.raises(SystemExit) as exit_info:
            extension("init")

        assert exit_info.value.code == 1
        (line,) = capsys.readouterr().err.splitlines()
        assert "uv add interloper-core>=" in line
        assert MANIFEST in line
        assert "interloper.yaml" in line
