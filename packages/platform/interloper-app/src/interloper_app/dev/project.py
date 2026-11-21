"""The project ``interloper dev`` serves: its root, what it holds, and the edits that restart the platform.

What the project holds is served from source and reloads; everything else
runs as installed. It holds the Python files under its root, and every
distribution installed editable from a directory under it, as its
``direct_url.json`` records: uv installs the project itself editable, and
every member of a workspace. Nothing is decided from folder names.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import url2pathname

from interloper.extension import MANIFESTS
from watchfiles import Change, DefaultFilter

CONFIG = "interloper.yaml"
MANIFEST = "extension.yaml"


@dataclass(frozen=True)
class HeldExtension:
    """An extension the project holds: a manifest shipped by one of its editable distributions.

    Attributes:
        key: The manifest's key.
        package: The import package shipping the manifest.
        directory: The directory the distribution is installed from, its
            ``pyproject.toml`` beside its ``ui/``.
        declares_ui: Whether the manifest declares a ``ui`` entry.
    """

    key: str
    package: str
    directory: Path
    declares_ui: bool

    @property
    def ui_directory(self) -> Path | None:
        """The UI's source, served by its Vite dev server.

        Returns:
            ``<directory>/ui`` when the manifest declares a UI and that folder
            holds a ``package.json``, else ``None``.
        """
        ui = self.directory / "ui"
        return ui if self.declares_ui and (ui / "package.json").is_file() else None


class DevProject:
    """The project in a directory, as ``interloper dev`` serves it.

    Attributes:
        root: The uv workspace root or uv project containing the directory,
            the directory itself outside any.
        name: The project's ``[project].name``, the root's folder name when
            it declares none; it names the dev organisation.
        config: The ``interloper.yaml`` every command reads, in the
            directory ``dev`` runs from.
        held: The directories of the distributions installed editable from
            under the root.
        files: The spec files ``dev`` applies, absolute; watched wherever
            they are.
    """

    def __init__(self, root: Path, name: str, config: Path, held: tuple[Path, ...], files: Sequence[Path] = ()) -> None:
        """Describe a project.

        Args:
            root: Its root.
            name: Its name.
            config: The active ``interloper.yaml``, existing or not.
            held: Its editable distributions' directories.
            files: The spec files ``dev`` applies, absolute.
        """
        self.root = root
        self.name = name
        self.config = config
        self.held = held
        self.files = tuple(files)
        self._default_filter = DefaultFilter()
        self._apart: dict[Path, bool] = {}

    @classmethod
    def discover(cls, directory: Path, files: Sequence[Path] = ()) -> DevProject:
        """Find the project a directory belongs to and what it holds.

        Args:
            directory: The directory ``dev`` runs from.
            files: The spec files ``dev`` applies, absolute.

        Returns:
            The project.
        """
        directory = directory.resolve()
        declared: dict[Path, dict[str, Any]] = {}
        for path in (directory, *directory.parents):
            if (pyproject := cls._read_toml(path / "pyproject.toml")) is not None:
                declared[path] = pyproject
        nearest = next(iter(declared), directory)
        root = next(
            (path for path, pyproject in declared.items() if cls._workspace_holds(path, pyproject, nearest)), nearest
        )
        name = declared.get(root, {}).get("project", {}).get("name") or root.name
        return cls(root, name, directory / CONFIG, cls._editable_under(root), files)

    @property
    def watched(self) -> list[Path]:
        """The directories whose edits restart the platform: the root, and those of the spec files outside it.

        Returns:
            Each directory once.
        """
        outside = [file.parent for file in self.files if not file.is_relative_to(self.root)]
        return list(dict.fromkeys([self.root, *outside]))

    @property
    def pnpm_workspace(self) -> bool:
        """Whether the root holds a pnpm workspace, whose UIs install with pnpm rather than npm.

        Returns:
            True when ``pnpm-workspace.yaml`` sits at the root.
        """
        return (self.root / "pnpm-workspace.yaml").is_file()

    def extensions(self) -> list[HeldExtension]:
        """List the extensions the project holds, read from the installed manifests without importing them.

        Returns:
            Each installed extension whose package lives under a held
            directory, in the manifests' order.
        """
        with_ui = {entry.package for entry in MANIFESTS.entries("ui")}
        held: list[HeldExtension] = []
        for extension in MANIFESTS.extensions():
            location = self._location(extension.package)
            holding = [path for path in self.held if location and location.is_relative_to(path)]
            directory = max(holding, key=lambda path: len(path.parts), default=None)
            if extension.key is not None and directory is not None:
                held.append(HeldExtension(extension.key, extension.package, directory, extension.package in with_ui))
        return held

    def watches(self, change: Change, path: str) -> bool:
        """Say whether an edit restarts the platform: a Python file, a manifest, or the active configuration.

        The usual noise is ignored (``.venv``, ``node_modules``, caches,
        version control), as it is by every reloader built on watchfiles, and
        so is everything inside another checkout or virtual environment under
        the root (a directory holding its own ``.git`` or ``pyvenv.cfg``): a
        worktree or a venv beside the code is not the project.

        Args:
            change: What happened to the file.
            path: The file's path.

        Returns:
            True when the platform must restart.
        """
        if not self._default_filter(change, path):
            return False
        edited = Path(path)
        if edited in self.files:
            return True
        if edited.name.startswith("."):
            return False
        if not (edited.suffix == ".py" or edited.name == MANIFEST or edited == self.config):
            return False
        return not any(self._kept_apart(parent) for parent in edited.parents if parent.is_relative_to(self.root))

    # -- Internals -------------------------------------------------------------

    def _kept_apart(self, directory: Path) -> bool:
        """Say whether a directory under the root is another checkout or a virtual environment.

        Args:
            directory: The directory, the root or one under it.

        Returns:
            True when it holds its own ``.git`` or ``pyvenv.cfg``; the root
            itself never is.
        """
        if directory == self.root:
            return False
        if directory not in self._apart:
            self._apart[directory] = (directory / ".git").exists() or (directory / "pyvenv.cfg").exists()
        return self._apart[directory]

    @staticmethod
    def _workspace_holds(path: Path, pyproject: dict[str, Any], project: Path) -> bool:
        """Say whether a ``pyproject.toml`` declares a uv workspace the project belongs to.

        Args:
            path: The directory of the ``pyproject.toml``.
            pyproject: Its content.
            project: The nearest project to the directory ``dev`` runs from.

        Returns:
            True when it declares a workspace and the project is its root, or
            one of its members and none of its exclusions.
        """
        workspace = pyproject.get("tool", {}).get("uv", {}).get("workspace")
        if workspace is None:
            return False
        if project == path:
            return True
        relative = project.relative_to(path).as_posix()
        members = workspace.get("members", [])
        excluded = workspace.get("exclude", [])
        if any(fnmatch(relative, glob) for glob in excluded):
            return False
        return any(fnmatch(relative, glob) for glob in members)

    @staticmethod
    def _read_toml(path: Path) -> dict[str, Any] | None:
        """Read a ``pyproject.toml``.

        Args:
            path: Its path.

        Returns:
            Its content, ``None`` when there is no such file.
        """
        if not path.is_file():
            return None
        with path.open("rb") as file:
            return tomllib.load(file)

    @staticmethod
    def _editable_under(root: Path) -> tuple[Path, ...]:
        """Find the distributions installed editable from a directory under the root.

        Args:
            root: The project's root.

        Returns:
            Their directories, each once, sorted.
        """
        directories: set[Path] = set()
        for distribution in importlib.metadata.distributions():
            recorded = distribution.read_text("direct_url.json")
            if not recorded:
                continue
            direct_url = json.loads(recorded)
            url = urlsplit(direct_url.get("url", ""))
            if url.scheme != "file" or not direct_url.get("dir_info", {}).get("editable"):
                continue
            directory = Path(url2pathname(url.path)).resolve()
            if directory.is_relative_to(root):
                directories.add(directory)
        return tuple(sorted(directories))

    @staticmethod
    def _location(package: str) -> Path | None:
        """Find where an import package lives, without importing it.

        Args:
            package: The top-level import package.

        Returns:
            Its directory, or its module file; ``None`` when it cannot be found.
        """
        spec = importlib.util.find_spec(package)
        if spec is None:
            return None
        if spec.submodule_search_locations:
            return Path(next(iter(spec.submodule_search_locations))).resolve()
        return Path(spec.origin).resolve() if spec.origin else None
