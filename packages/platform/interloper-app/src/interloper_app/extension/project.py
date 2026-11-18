"""The uv project an author runs ``interloper extension`` in, and the edits made to it.

An extension is a library: ``uv init --lib`` gives the ``src/`` package and
the build system its wheel needs, so a project without them is refused with
that command rather than guessed at. Dependencies go through ``uv add``, which
keeps ``pyproject.toml``, the lockfile and the environment in step.
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

from interloper.extension import MANIFEST_FILE

from interloper_app.extension.errors import ExtensionError

ENTRY_POINT_GROUP = "interloper.extensions"
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


@dataclass(frozen=True)
class Project:
    """A library project made with ``uv init --lib``.

    Attributes:
        root: The directory holding ``pyproject.toml``.
        distribution: The project's name, ``[project].name``.
        package: The import package under ``src/``.
        description: ``[project].description``, empty when the project gives
            none.
    """

    root: Path
    distribution: str
    package: str
    description: str = ""

    @classmethod
    def from_directory(cls, directory: Path) -> Project:
        """Read the project in a directory.

        The import package is ``[tool.uv.build-backend].module-name`` when the
        project sets it, the distribution's name in snake case otherwise, as
        ``uv_build`` decides.

        Args:
            directory: The directory to read, the current one for a command.

        Returns:
            The project.

        Raises:
            ExtensionError: If the directory holds no ``pyproject.toml`` (naming
                the project's root when a parent holds one), or a project
                without a name, a ``[build-system]`` or a
                ``src/<package>/__init__.py``.
        """
        path = directory / "pyproject.toml"
        if not path.is_file():
            root = next((parent for parent in directory.parents if (parent / "pyproject.toml").is_file()), None)
            if root is not None:
                raise ExtensionError(f"no pyproject.toml here: run it from the project's root, {root}")
            raise ExtensionError("no pyproject.toml here: create the project with uv init --lib")
        pyproject = cls._read_toml(path)
        metadata = pyproject.get("project", {})
        distribution = metadata.get("name")
        module = pyproject.get("tool", {}).get("uv", {}).get("build-backend", {}).get("module-name")
        package = module if isinstance(module, str) else re.sub(r"[-.]", "_", str(distribution)).lower()
        if (
            not isinstance(distribution, str)
            or "build-system" not in pyproject
            or not (directory / "src" / package / "__init__.py").is_file()
        ):
            raise ExtensionError(
                f"{directory.name} is not a library project: create it with uv init --lib "
                f"(it needs a [project] name, a [build-system] and src/{package}/__init__.py)"
            )
        description = metadata.get("description")
        return cls(directory, distribution, package, description if isinstance(description, str) else "")

    @property
    def package_directory(self) -> Path:
        """The import package's directory, ``src/<package>``.

        Returns:
            The directory.
        """
        return self.root / "src" / self.package

    @property
    def manifest_path(self) -> Path:
        """Where the extension's manifest lives, at the root of its import package.

        Returns:
            The path of ``extension.yaml``.
        """
        return self.package_directory / MANIFEST_FILE

    @property
    def declares_extension(self) -> bool:
        """Whether the project is an extension already.

        Returns:
            True when ``pyproject.toml`` names an ``interloper.extensions``
            entry point or the manifest exists.
        """
        entry_points = self._pyproject.get("project", {}).get("entry-points", {})
        return ENTRY_POINT_GROUP in entry_points or self.manifest_path.exists()

    @property
    def default_key(self) -> str:
        """The extension's key when the author names none.

        Returns:
            The import package's name without an ``interloper_`` prefix or an
            ``_interloper`` suffix; the whole name when that leaves nothing.
        """
        return self.package.removeprefix("interloper_").removesuffix("_interloper") or self.package

    def relative(self, paths: Iterable[Path]) -> str:
        """Name files as the author sees them.

        Args:
            paths: Files inside the project.

        Returns:
            Their paths relative to the project's root, comma-separated.
        """
        return ", ".join(str(path.relative_to(self.root)) for path in paths)

    def check_free(self, paths: Iterable[Path]) -> None:
        """Refuse files that exist, and a folder or module that would share an import name with one.

        Args:
            paths: The files about to be written inside the import package.

        Raises:
            ExtensionError: If a file exists, or the module beside a new
                folder (``shop.py`` for ``shop/``) or the folder beside a new
                module (``routes/`` for ``routes.py``) does.
        """
        paths = list(paths)
        existing = [path for path in paths if path.exists()]
        if existing:
            raise ExtensionError(f"{self.relative(existing)} exists already; move it away or pick another name")
        for path in paths:
            in_folder = path.parent != self.package_directory
            clash = path.parent.with_suffix(".py") if in_folder else path.with_suffix("")
            if clash.exists():
                raise ExtensionError(
                    f"{self.relative([clash])} exists, and would share its import name with "
                    f"{self.relative([path])}; pick another name"
                )

    def add_entry_point(self, key: str) -> None:
        """Name the import package under the ``interloper.extensions`` entry point.

        The entry point is appended as a table of its own, which TOML allows
        anywhere after ``[project]``, so nothing the author wrote moves.

        Args:
            key: The entry point's name, the extension's key.

        Raises:
            ExtensionError: If the file would not read back with the entry
                point, as when ``[project]`` holds ``entry-points`` as an
                inline table.
        """
        path = self.root / "pyproject.toml"
        text = self._appending(path, ["", f'[project.entry-points."{ENTRY_POINT_GROUP}"]', f'{key} = "{self.package}"'])
        try:
            declared = tomllib.loads(text)["project"]["entry-points"][ENTRY_POINT_GROUP]
        except (tomllib.TOMLDecodeError, KeyError, TypeError):
            declared = None
        if declared != {key: self.package}:
            raise ExtensionError(
                f"the entry point cannot be appended to pyproject.toml; add it to pyproject.toml by hand: "
                f'[project.entry-points."{ENTRY_POINT_GROUP}"] {key} = "{self.package}"'
            )
        path.write_bytes(text.encode("utf-8"))

    def ignore(self, *patterns: str) -> None:
        """Add patterns to ``.gitignore``, creating it when missing.

        Args:
            *patterns: The patterns; those the file lists already are skipped.
        """
        path = self.root / ".gitignore"
        listed = set(path.read_text().splitlines()) if path.exists() else set()
        missing = [pattern for pattern in dict.fromkeys(patterns) if pattern not in listed]
        if missing:
            path.write_bytes(self._appending(path, missing).encode("utf-8"))

    def require(self, distribution: str) -> None:
        """Add a dependency through ``uv add`` unless the project names it already.

        Args:
            distribution: The distribution, required at least at the version
                installed in this environment.

        Raises:
            ExtensionError: If ``uv add`` fails or cannot run.
        """
        dependencies = self._pyproject.get("project", {}).get("dependencies", [])
        if any(self._canonical(dependency) == self._canonical(distribution) for dependency in dependencies):
            return
        requirement = f"{distribution}>={version(distribution).split('+')[0]}"
        try:
            self._uv("add", requirement)
        except (subprocess.CalledProcessError, OSError) as error:
            raise ExtensionError(f"uv add {requirement} failed; run it yourself to finish") from error

    # -- Internals -------------------------------------------------------------

    @property
    def _pyproject(self) -> dict[str, Any]:
        """``pyproject.toml`` as it is now on disk.

        Returns:
            The parsed file.
        """
        return self._read_toml(self.root / "pyproject.toml")

    def _uv(self, *args: str) -> None:
        """Run uv in the project's directory, its output going to the terminal.

        Args:
            *args: uv's arguments.
        """
        subprocess.run(["uv", *args], cwd=self.root, check=True)

    @staticmethod
    def _read_toml(path: Path) -> dict[str, Any]:
        """Parse a TOML file.

        Args:
            path: The file.

        Returns:
            The parsed document.

        Raises:
            ExtensionError: If the file is not valid TOML.
        """
        try:
            return tomllib.loads(path.read_text())
        except tomllib.TOMLDecodeError as error:
            raise ExtensionError(f"{path.name} is not valid TOML: {error}") from error

    @staticmethod
    def _appending(path: Path, lines: Sequence[str]) -> str:
        """A file's text with lines appended, in the file's own line endings.

        Args:
            path: The file; a missing one reads as empty.
            lines: The lines to append, without endings.

        Returns:
            The new text, the file's last line ended first when it was not.
        """
        text = path.read_bytes().decode("utf-8") if path.exists() else ""
        newline = "\r\n" if "\r\n" in text else "\n"
        if text and not text.endswith("\n"):
            text = f"{text}{newline}"
        return text + "".join(f"{line}{newline}" for line in lines)

    @staticmethod
    def _canonical(requirement: str) -> str:
        """The normalized distribution name a requirement starts with.

        Args:
            requirement: A requirement (``Interloper_API>=0.1``) or a bare name.

        Returns:
            The name, lowercased with runs of ``-``, ``_`` and ``.`` as one ``-``.
        """
        match = _REQUIREMENT_NAME.match(requirement.strip())
        return re.sub(r"[-_.]+", "-", match.group(0)).lower() if match else ""
