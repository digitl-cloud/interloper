"""Tests for ``interloper_api.ui.base``."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from interloper_api.ui import UIBundle


def _files(directory: Path, **files: str) -> Path:
    """Write a build of any bundler: files by path relative to the directory.

    Args:
        directory: The build's output directory, created.
        **files: Each file's content, by its path with ``__`` for ``/``.

    Returns:
        The directory.
    """
    for name, content in files.items():
        path = directory / name.replace("__", "/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return directory


class TestUIBundle:
    def test_a_build_is_declared_by_its_entry_and_stylesheets(self, tmp_path: Path):
        directory = _files(tmp_path / "dist", **{"main.js": "export {}", "styles__main.css": "a {}"})

        bundle = UIBundle(directory, entry="main.js", css=["styles/main.css"])

        assert (bundle.entry, bundle.css) == ("main.js", ("styles/main.css",))

    def test_the_entry_is_index_js_and_no_stylesheet_unless_named(self, tmp_path: Path):
        bundle = UIBundle(_files(tmp_path / "dist", **{"index.js": "export {}"}))

        assert (bundle.entry, bundle.css) == ("index.js", ())

    @pytest.mark.parametrize(("entry", "css"), [("main.js", ()), ("index.js", ("index.css",))])
    def test_a_named_file_the_directory_does_not_hold_is_refused(self, tmp_path: Path, entry: str, css: tuple):
        directory = _files(tmp_path / "dist", **{"index.js": "export {}"})

        with pytest.raises(FileNotFoundError, match="holds no"):
            UIBundle(directory, entry=entry, css=css)

    def test_the_digest_follows_every_file_its_path_and_content(self, tmp_path: Path):
        def digest(name: str, **files: str) -> str:
            return UIBundle(_files(tmp_path / name, **{"index.js": "export {}", **files})).digest

        base = digest("base", **{"chunk.js": "1"})

        assert digest("same", **{"chunk.js": "1"}) == base
        assert digest("edited", **{"chunk.js": "2"}) != base
        assert digest("renamed", **{"part.js": "1"}) != base
        assert digest("added", **{"chunk.js": "1", "logo.svg": "<svg/>"}) != base

    def test_declaring_one_loads_none_of_the_api(self):
        loaded = subprocess.run(
            [sys.executable, "-c", "import sys\nfrom interloper_api import UIBundle\nprint(*sys.modules)"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()

        assert {"fastapi", "sqlalchemy", "interloper_api.app"}.isdisjoint(loaded)


class TestFromVite:
    def test_the_entry_and_its_stylesheets_come_from_the_vite_manifest(
        self, tmp_path: Path, vite_build: Callable[..., Path]
    ):
        bundle = UIBundle.from_vite(vite_build(tmp_path / "static"))

        assert bundle.entry == "assets/index-3f9a1c.js"
        assert bundle.css == ("assets/index-b72e04.css", "assets/shared-0c11d2.css")

    def test_a_directory_without_a_build_cannot_be_declared(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            UIBundle.from_vite(tmp_path)

    @pytest.mark.parametrize("entries", [0, 2])
    def test_a_build_names_exactly_one_entry(self, tmp_path: Path, vite_build: Callable[..., Path], entries: int):
        chunks = {f"src/page{i}.ts": {"file": f"assets/page{i}-0a0a0a.js", "isEntry": i < entries} for i in range(2)}

        with pytest.raises(ValueError, match=f"names {entries} entry chunks, expected one"):
            UIBundle.from_vite(vite_build(tmp_path / "static", chunks))
