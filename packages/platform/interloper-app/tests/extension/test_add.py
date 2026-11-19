"""Tests for ``interloper_app.extension.add``."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from importlib.metadata import version
from pathlib import Path

import pytest
import yaml
from interloper.extension import ExtensionEntry

from interloper_app.extension.project import Project

PACKAGE = Path("src/acme_interloper")


@pytest.fixture
def importable(extension_project, monkeypatch) -> Iterator[Path]:
    """Put the project's ``src/`` on the import path, and forget its modules afterwards.

    Args:
        extension_project: The project.
        monkeypatch: Restores ``sys.path``.

    Yields:
        The project's directory.
    """
    monkeypatch.syspath_prepend(str(extension_project / "src"))
    yield extension_project
    for module in [module for module in sys.modules if module.split(".")[0] == "acme_interloper"]:
        del sys.modules[module]


def _manifest(root: Path) -> dict:
    return yaml.safe_load((root / PACKAGE / "extension.yaml").read_text())


class TestParts:
    @pytest.mark.parametrize(
        ("part", "files", "section", "declared", "requires"),
        [
            (
                "source",
                ["acme/__init__.py", "acme/source.py"],
                "components",
                ["acme_interloper.acme.source:Acme"],
                [],
            ),
            (
                "connection",
                ["acme/__init__.py", "acme/connection.py"],
                "components",
                ["acme_interloper.acme.connection:AcmeConnection"],
                [],
            ),
            (
                "settings",
                ["settings/__init__.py", "settings/acme.py"],
                "settings",
                {"acme": "acme_interloper.settings.acme:AcmeSettings"},
                [],
            ),
            (
                "command",
                ["commands/__init__.py", "commands/acme.py"],
                "cli",
                {"acme": "acme_interloper.commands.acme:AcmeCommand"},
                [],
            ),
            ("routes", ["routes.py"], "routes", "acme_interloper.routes:router", ["interloper-api"]),
        ],
    )
    def test_each_part_writes_its_files_and_entry(
        self, extension_project, uv, extension, capsys, part, files, section, declared, requires
    ):
        extension("add", part)

        for file in files:
            assert (extension_project / PACKAGE / file).is_file()
        assert _manifest(extension_project)[section] == declared
        assert uv == [("add", f"{distribution}>={version(distribution)}") for distribution in requires]
        assert "Wrote" in capsys.readouterr().out

    def test_the_entries_import(self, importable, extension):
        for part in ("source", "connection", "settings", "command", "routes"):
            extension("add", part)

        manifest = _manifest(importable)
        loaded = {
            (entry.section, entry.name): entry.load()
            for section in ("components", "settings", "cli", "routes")
            for entry in ExtensionEntry.from_section("acme_interloper", "acme", section, manifest[section])
        }
        source = loaded[("components", "acme_interloper.acme.source:Acme")]
        connection = loaded[("components", "acme_interloper.acme.connection:AcmeConnection")]
        assert (source.key, connection.key) == ("acme", "acme_connection")
        assert loaded[("settings", "acme")].__name__ == "AcmeSettings"
        assert loaded[("cli", "acme")].__doc__
        assert [route.path for route in loaded[("routes", "acme")].routes] == ["/greeting"]

    def test_a_source_and_a_connection_share_their_folder(self, extension_project, extension):
        extension("add", "source")
        folder = (extension_project / PACKAGE / "acme" / "__init__.py").read_bytes()

        extension("add", "connection")

        assert (extension_project / PACKAGE / "acme" / "__init__.py").read_bytes() == folder
        assert _manifest(extension_project)["components"] == [
            "acme_interloper.acme.source:Acme",
            "acme_interloper.acme.connection:AcmeConnection",
        ]

    def test_what_the_author_wrote_in_the_manifest_is_kept(self, extension_project, extension):
        manifest = extension_project / PACKAGE / "extension.yaml"
        manifest.write_text(f"# Acme's manifest\n{manifest.read_text()}cli:\n  # the sync job\n  sync: a:Sync\n")
        before = manifest.read_text()

        extension("add", "command")

        assert manifest.read_text() == before.replace(
            "  sync: a:Sync\n", "  sync: a:Sync\n  acme: acme_interloper.commands.acme:AcmeCommand\n"
        )


class TestNames:
    def test_a_name_names_the_files_and_the_class(self, extension_project, extension):
        extension("add", "source", "shop")

        assert "class Shop(" in (extension_project / PACKAGE / "shop" / "source.py").read_text()
        assert _manifest(extension_project)["components"] == ["acme_interloper.shop.source:Shop"]

    def test_a_settings_section_in_the_extensions_namespace_is_accepted(self, extension_project, extension):
        extension("add", "settings", "acme_sync")

        assert _manifest(extension_project)["settings"] == {
            "acme_sync": "acme_interloper.settings.acme_sync:AcmeSyncSettings"
        }

    @pytest.mark.parametrize(
        ("argv", "message"),
        [
            (("settings", "other"), "a settings section other than 'acme' must start with acme_"),
            (("command", "app"), "interloper_app already declares 'app' under cli"),
            (("source", "1abc"), "the source name '1abc' is not snake_case"),
            (("source", "Acme"), "the source name 'Acme' is not snake_case"),
            (("source", "class"), "the source name 'class' is a Python keyword"),
            (("routes", "other"), "routes take the extension's key, acme"),
            (("source", "none"), "the name 'none' gives the class None, a Python keyword"),
            (("source", "true"), "the name 'true' gives the class True, a Python keyword"),
            (("source", "tiktok_2024"), "the name 'tiktok_2024' gives the class Tiktok2024, keyed tiktok2024"),
            (("source", "acme_"), "the name 'acme_' gives the class Acme, keyed acme"),
            (("connection", "a_b"), "the name 'a_b' gives the class ABConnection, keyed ab_connection"),
            (("source", "static"), "the name 'static' is where the extension's UI lives"),
            (("connection", "ui"), "the name 'ui' is where the extension's UI lives"),
            (("ui", "other"), "ui take the extension's key, acme"),
        ],
    )
    def test_a_name_that_would_break_the_catalog_or_the_cli_is_refused(
        self, extension_project, uv, extension, tree, capsys, argv, message
    ):
        before = tree(extension_project)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", *argv)

        assert exit_info.value.code == 1
        assert message in capsys.readouterr().err
        assert tree(extension_project) == before
        assert uv == []

    def test_a_folder_that_would_shadow_a_module_is_refused(self, extension_project, extension, tree, capsys):
        (extension_project / PACKAGE / "shop.py").write_text("SHOP = 1\n")
        before = tree(extension_project)

        with pytest.raises(SystemExit):
            extension("add", "source", "shop")

        assert "src/acme_interloper/shop.py exists" in capsys.readouterr().err
        assert tree(extension_project) == before


UI_FILES = [
    "ui/package.json",
    "ui/vite.config.ts",
    "ui/tsconfig.json",
    "ui/src/index.ts",
    "ui/src/main.css",
    "ui/src/pages/Greeting.vue",
]


class TestUI:
    def test_add_ui_writes_the_ui_and_its_routes(self, extension_project, uv, extension, capsys):
        extension("add", "ui")

        for file in [PACKAGE / "ui.py", PACKAGE / "routes.py", *UI_FILES]:
            assert (extension_project / file).is_file(), file
        manifest = (extension_project / PACKAGE / "extension.yaml").read_text()
        assert manifest.endswith("routes: acme_interloper.routes:router\nui: acme_interloper.ui:UI\n")
        ignored = (extension_project / ".gitignore").read_text().splitlines()
        assert {"src/acme_interloper/static/", "ui/node_modules/"} <= set(ignored)
        assert uv == [("add", f"interloper-api>={version('interloper-api')}")]
        assert "outDir: '../src/acme_interloper/static'" in (extension_project / "ui/vite.config.ts").read_text()
        out = capsys.readouterr().out
        assert "routes declares acme_interloper.routes:router" in out
        assert "ui declares acme_interloper.ui:UI" in out
        assert "Build it with uv run interloper extension build" in out

    def test_add_ui_keeps_routes_the_extension_has(self, extension_project, extension):
        extension("add", "routes")
        routes = (extension_project / PACKAGE / "routes.py").read_bytes()

        extension("add", "ui")

        assert (extension_project / PACKAGE / "routes.py").read_bytes() == routes
        assert _manifest(extension_project)["routes"] == "acme_interloper.routes:router"
        assert _manifest(extension_project)["ui"] == "acme_interloper.ui:UI"

    def test_add_ui_refuses_a_ui_folder_that_exists(self, extension_project, uv, extension, tree, capsys):
        (extension_project / "ui").mkdir()
        (extension_project / "ui" / "package.json").write_text("{}\n")
        before = tree(extension_project)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "ui")

        assert exit_info.value.code == 1
        assert "ui/package.json exists already" in capsys.readouterr().err
        assert tree(extension_project) == before
        assert uv == []

    def test_add_ui_refuses_a_static_folder_its_build_would_empty(self, extension_project, uv, extension, tree, capsys):
        (extension_project / PACKAGE / "static").mkdir()
        (extension_project / PACKAGE / "static" / "logo.svg").write_text("<svg/>\n")
        before = tree(extension_project)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "ui")

        assert exit_info.value.code == 1
        assert "src/acme_interloper/static exists, and every build of the UI empties it" in capsys.readouterr().err
        assert tree(extension_project) == before
        assert uv == []

    def test_add_ui_twice_is_refused(self, extension_project, extension, capsys):
        extension("add", "ui")

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "ui")

        assert exit_info.value.code == 1
        assert "extension.yaml already declares ui" in capsys.readouterr().err


class TestRefusals:
    def test_add_refuses_before_init(self, acme, uv, extension, tree, capsys):
        before = tree(acme)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "source")

        assert exit_info.value.code == 1
        assert "run uv run interloper extension init first" in capsys.readouterr().err
        assert tree(acme) == before

    def test_add_refuses_an_extension_whose_manifest_is_missing(self, extension_project, extension, tree, capsys):
        (extension_project / PACKAGE / "extension.yaml").unlink()
        before = tree(extension_project)

        with pytest.raises(SystemExit):
            extension("add", "source")

        assert "src/acme_interloper/extension.yaml is missing" in capsys.readouterr().err
        assert tree(extension_project) == before

    def test_add_refuses_a_manifest_without_a_key(self, extension_project, extension, tree, capsys):
        (extension_project / PACKAGE / "extension.yaml").write_text("manifest_version: 1\nname: Acme\n")
        before = tree(extension_project)

        with pytest.raises(SystemExit):
            extension("add", "source")

        assert "src/acme_interloper/extension.yaml names no key" in capsys.readouterr().err
        assert tree(extension_project) == before

    def test_add_changes_nothing_when_a_file_exists(self, extension_project, uv, extension, tree, capsys):
        (extension_project / PACKAGE / "acme").mkdir()
        (extension_project / PACKAGE / "acme" / "source.py").write_text("# mine\n")
        before = tree(extension_project)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "source")

        assert exit_info.value.code == 1
        assert "src/acme_interloper/acme/source.py exists already" in capsys.readouterr().err
        assert tree(extension_project) == before

    def test_add_refuses_a_part_the_manifest_declares(self, extension_project, extension, tree, capsys):
        extension("add", "routes")
        (extension_project / PACKAGE / "routes.py").unlink()
        before = tree(extension_project)

        with pytest.raises(SystemExit):
            extension("add", "routes")

        assert "extension.yaml already declares routes" in capsys.readouterr().err
        assert tree(extension_project) == before

    def test_a_failing_uv_is_one_line_naming_what_was_written(self, extension_project, extension, capsys, monkeypatch):
        def fail(self, *args):
            raise subprocess.CalledProcessError(1, ["uv", *args])

        monkeypatch.setattr(Project, "_uv", fail)

        with pytest.raises(SystemExit) as exit_info:
            extension("add", "routes")

        assert exit_info.value.code == 1
        (line,) = capsys.readouterr().err.splitlines()
        assert "uv add interloper-api>=" in line
        assert "src/acme_interloper/routes.py" in line
        assert "src/acme_interloper/extension.yaml" in line
