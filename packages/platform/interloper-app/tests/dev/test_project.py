"""Tests for ``interloper_app.dev.project``."""

from __future__ import annotations

import email
import fnmatch
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from watchfiles import Change

from interloper_app.dev import project as project_module
from interloper_app.dev.project import DevProject, HeldExtension


def _pyproject(directory: Path, text: str) -> Path:
    """Write a ``pyproject.toml``.

    Args:
        directory: Where it goes, created when missing.
        text: Its content.

    Returns:
        The directory.
    """
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(text)
    return directory


def _distribution(url: str, *, editable: bool = True) -> SimpleNamespace:
    """Stand in for an installed distribution that records where it was installed from.

    Args:
        url: The ``direct_url.json`` URL.
        editable: Whether it was installed editable.

    Returns:
        An object answering ``read_text("direct_url.json")``.
    """
    recorded = json.dumps({"url": url, "dir_info": {"editable": editable}})
    return SimpleNamespace(read_text=lambda name: recorded if name == "direct_url.json" else None)


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> list[SimpleNamespace]:
    """The installed distributions the project reads, none until a test adds some.

    Args:
        monkeypatch: Points ``importlib.metadata.distributions`` at the list.

    Returns:
        The list to add distributions to.
    """
    distributions: list[SimpleNamespace] = []
    monkeypatch.setattr(project_module.importlib.metadata, "distributions", lambda: distributions)
    return distributions


class TestDiscover:
    def test_a_workspace_member_belongs_to_the_workspace(self, tmp_path: Path, installed):
        _pyproject(tmp_path, '[project]\nname = "interloper"\n[tool.uv.workspace]\nmembers = ["packages/*"]\n')
        member = _pyproject(tmp_path / "packages" / "acme", '[project]\nname = "acme"\n')
        (tmp_path / "dev").mkdir()

        for directory in (member, tmp_path / "dev"):
            project = DevProject.discover(directory)
            assert (project.root, project.name) == (tmp_path.resolve(), "interloper")
        assert project.config == (tmp_path / "dev" / "interloper.yaml").resolve()

    @pytest.mark.parametrize(
        "workspace", ['members = ["packages/*"]', 'members = ["*", "examples/*"]\nexclude = ["examples/*"]']
    )
    def test_a_project_the_workspace_does_not_hold_is_its_own_root(self, tmp_path: Path, installed, workspace: str):
        _pyproject(tmp_path, f'[project]\nname = "interloper"\n[tool.uv.workspace]\n{workspace}\n')
        example = _pyproject(tmp_path / "examples" / "extension", '[project]\nname = "example"\n')

        project = DevProject.discover(example)

        assert (project.root, project.name) == (example.resolve(), "example")

    def test_a_uv_project_is_its_own_root(self, tmp_path: Path, installed):
        weather = _pyproject(tmp_path / "weather", '[project]\nname = "weather-pipelines"\n')
        (weather / "src").mkdir()

        project = DevProject.discover(weather / "src")

        assert (project.root, project.name) == (weather.resolve(), "weather-pipelines")

    def test_outside_any_project_the_directory_is_the_root(self, tmp_path: Path, installed):
        project = DevProject.discover(tmp_path)

        assert (project.root, project.name) == (tmp_path.resolve(), tmp_path.name)

    def test_it_holds_the_distributions_installed_editable_from_under_it(self, tmp_path: Path, installed):
        root = _pyproject(tmp_path / "acme", '[project]\nname = "acme"\n')
        installed.extend(
            [
                _distribution(root.as_uri()),
                _distribution((root / "examples" / "extension").as_uri()),
                _distribution((tmp_path / "elsewhere").as_uri()),
                _distribution((root / "vendored").as_uri(), editable=False),
                SimpleNamespace(read_text=lambda name: None),
            ]
        )

        project = DevProject.discover(root)

        assert project.held == (root.resolve(), (root / "examples" / "extension").resolve())


class TestExtensions:
    def test_only_the_extensions_shipped_from_a_held_directory(self, tmp_path: Path, monkeypatch, installed):
        root = _pyproject(tmp_path / "acme", '[project]\nname = "acme"\n')
        installed.append(_distribution(root.as_uri()))
        locations = {"acme_interloper": root / "src" / "acme_interloper", "interloper_app": tmp_path / "site"}
        monkeypatch.setattr(DevProject, "_location", staticmethod(lambda package: locations.get(package)))
        manifests = mock.Mock()
        manifests.extensions.return_value = [
            SimpleNamespace(key="acme", package="acme_interloper"),
            SimpleNamespace(key="app", package="interloper_app"),
            SimpleNamespace(key=None, package="broken"),
        ]
        manifests.entries.return_value = [SimpleNamespace(package="acme_interloper")]
        monkeypatch.setattr(project_module, "MANIFESTS", manifests)

        held = DevProject.discover(root).extensions()

        assert held == [HeldExtension("acme", "acme_interloper", root.resolve(), declares_ui=True)]
        manifests.entries.assert_called_once_with("ui")

    def test_a_nested_extension_belongs_to_its_own_directory(self, tmp_path: Path, monkeypatch, installed):
        root = _pyproject(tmp_path / "acme", '[project]\nname = "acme"\n')
        nested = root / "examples" / "extension"
        installed.extend([_distribution(root.as_uri()), _distribution(nested.as_uri())])
        monkeypatch.setattr(DevProject, "_location", staticmethod(lambda package: nested / "src" / package))
        manifests = mock.Mock()
        manifests.extensions.return_value = [SimpleNamespace(key="example", package="interloper_example")]
        manifests.entries.return_value = []
        monkeypatch.setattr(project_module, "MANIFESTS", manifests)

        (held,) = DevProject.discover(root).extensions()

        assert held.directory == nested.resolve()

    def test_the_ui_is_served_once_it_has_a_package_json(self, tmp_path: Path):
        extension = HeldExtension("acme", "acme_interloper", tmp_path, declares_ui=True)
        assert extension.ui_directory is None

        (tmp_path / "ui").mkdir()
        (tmp_path / "ui" / "package.json").write_text("{}")

        assert extension.ui_directory == tmp_path / "ui"
        assert HeldExtension("acme", "acme_interloper", tmp_path, declares_ui=False).ui_directory is None


class TestWatches:
    @pytest.fixture
    def project(self, tmp_path: Path) -> DevProject:
        return DevProject(tmp_path, "acme", tmp_path / "interloper.yaml", held=())

    @pytest.mark.parametrize(
        ("path", "restarts"),
        [
            ("src/acme_interloper/routes.py", True),
            ("src/acme_interloper/extension.yaml", True),
            ("interloper.yaml", True),
            ("ui/src/pages/Greeting.vue", False),
            ("docs/interloper.yaml", False),
            ("README.md", False),
            (".venv/lib/python3.13/site-packages/vue.py", False),
            ("ui/node_modules/thing/index.py", False),
            ("src/acme_interloper/__pycache__/routes.cpython-313.pyc", False),
        ],
    )
    def test_python_files_manifests_and_the_configuration_restart(self, project: DevProject, path: str, restarts: bool):
        assert project.watches(Change.modified, str(project.root / path)) is restarts

    @pytest.mark.parametrize("marker", [".git", "pyvenv.cfg"])
    def test_another_checkout_or_a_venv_under_the_root_is_not_watched(self, project: DevProject, marker: str):
        apart = project.root / ".claude" / "worktrees" / "feat"
        apart.mkdir(parents=True)
        (apart / marker).write_text("")

        assert project.watches(Change.modified, str(apart / "src" / "routes.py")) is False
        (project.root / ".git").mkdir()
        assert project.watches(Change.modified, str(project.root / "src" / "routes.py")) is True

    def test_a_pnpm_workspace_installs_with_pnpm(self, project: DevProject):
        assert project.pnpm_workspace is False
        (project.root / "pnpm-workspace.yaml").write_text("packages: []\n")
        assert project.pnpm_workspace is True


class TestLocation:
    def test_a_package_is_found_without_being_imported(self):
        assert DevProject._location("email") == Path(email.__file__).parent.resolve()

    def test_a_module_is_found_by_its_file(self):
        assert DevProject._location("fnmatch") == Path(fnmatch.__file__).resolve()

    def test_a_missing_package_is_nowhere(self):
        assert DevProject._location("no_such_package_anywhere") is None
