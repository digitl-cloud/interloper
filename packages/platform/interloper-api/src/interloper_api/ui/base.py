"""What a package names in its manifest's ``ui`` section: the prebuilt UI it ships, or a dev server in its place.

An extension's ``ui`` entry builds its `UIBundle` when it is imported, and
``interloper_api`` re-exports both classes, so this module imports nothing
of the API's runtime: neither loading the entry nor reading one of the API's
settings sections loads the app. The registry reading the section, and the
routes serving a bundle's files, are in `interloper_api.routes.extensions`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

VITE_MANIFEST = ".vite/manifest.json"


class UIBundle:
    """A prebuilt UI an installed package ships: the output directory of its build, whatever built it.

    The host imports the entry, an ES module, and adds the stylesheets beside
    it. Chunks the entry loads must be addressed relative to it (Vite's
    ``base: './'``): the bundle is served under a path of its own. A build
    whose output names its entry and stylesheets for it, a Vite build, is
    declared with `from_vite`.

    The bundle is read when it is declared: a package installed without its
    build fails to load, and the API reports it.

    Attributes:
        directory: The build's output directory.
        entry: The entry module, relative to the directory.
        css: The stylesheets the entry needs, relative to the directory.
        digest: The hash of every file of the directory, their paths and
            contents, which the files are served under.
    """

    def __init__(self, directory: Path, entry: str = "index.js", css: Sequence[str] = ()) -> None:
        """Declare a build's output.

        Args:
            directory: The build's output directory, inside the package that
                ships it (``Path(__file__).parent / "dist"``).
            entry: The entry module, relative to the directory.
            css: The stylesheets the entry needs, relative to the directory;
                none when the entry adds its own.

        Raises:
            FileNotFoundError: If the entry or a stylesheet is not a file of the
                directory.
        """
        self.directory = Path(directory)
        self.entry = entry
        self.css = tuple(css)
        for path in (entry, *self.css):
            if not (self.directory / path).is_file():
                raise FileNotFoundError(f"{self.directory} holds no {path}")
        self.digest = self._digest()

    @classmethod
    def from_vite(cls, directory: Path) -> UIBundle:
        """Declare the output of a Vite build, its entry and stylesheets read from the build's Vite manifest.

        The build writes the manifest (``build.manifest: true``), so the entry
        may carry a content hash in its name and nothing in the declaration
        changes with a rebuild.

        Args:
            directory: The build's output directory, inside the package that
                ships it.

        Returns:
            The bundle, its stylesheets those of the entry and of every chunk
            it imports.

        Raises:
            ValueError: If the manifest names no entry chunk, or several.
        """
        directory = Path(directory)
        chunks: dict[str, dict[str, Any]] = json.loads((directory / VITE_MANIFEST).read_text())
        entries = [key for key, chunk in chunks.items() if chunk.get("isEntry")]
        if len(entries) != 1:
            raise ValueError(f"{directory / VITE_MANIFEST} names {len(entries)} entry chunks, expected one")
        return cls(directory, entry=chunks[entries[0]]["file"], css=cls._vite_stylesheets(chunks, entries[0]))

    def _digest(self) -> str:
        """Hash every file of the directory, its path and its content, in path order.

        Returns:
            The first 16 hex digits of the SHA-256.
        """
        digest = hashlib.sha256()
        for path in sorted(path for path in self.directory.rglob("*") if path.is_file()):
            digest.update(path.relative_to(self.directory).as_posix().encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
        return digest.hexdigest()[:16]

    @staticmethod
    def _vite_stylesheets(chunks: dict[str, dict[str, Any]], entry: str) -> tuple[str, ...]:
        """Collect the stylesheets of a chunk and of every chunk it imports, each once.

        Args:
            chunks: The Vite manifest's chunks, by source path.
            entry: The source path of the chunk to start from.

        Returns:
            The stylesheets, the entry's own first.
        """
        stylesheets: dict[str, None] = {}
        visited: set[str] = set()
        pending = [entry]
        while pending:
            key = pending.pop(0)
            if key in visited:
                continue
            visited.add(key)
            stylesheets.update(dict.fromkeys(chunks[key].get("css", ())))
            pending.extend(chunks[key].get("imports", ()))
        return tuple(stylesheets)


class DevServerUI:
    """A UI served by its own dev server instead of a build: what ``interloper dev`` registers for a held UI.

    It takes the place of the manifest's entry through
    ``UI_BUNDLES.override``, inside the process that serves the project, so
    the entry is never imported and needs no build. The dev server adds the
    UI's styles itself, and no file is served for it.

    Attributes:
        entry: The URL of the entry module on the dev server.
    """

    def __init__(self, entry: str) -> None:
        """Name the dev server's entry module.

        Args:
            entry: Its absolute URL (``http://127.0.0.1:5173/src/index.ts``).
        """
        self.entry = entry
