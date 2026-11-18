"""Fixtures for the ``interloper extension`` commands: a library project, uv recorded, the CLI run."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path

import pytest

from interloper_app.extension import ExtensionCommand
from interloper_app.extension.project import Project

PYPROJECT = """\
[project]
name = "{name}"
version = "0.1.0"
description = "Acme sources."
requires-python = ">=3.11"
dependencies = [{dependencies}]

[build-system]
requires = ["uv_build>=0.13.1,<0.14.0"]
build-backend = "uv_build"
"""


@pytest.fixture
def library(tmp_path: Path) -> Callable[..., Path]:
    """Write the project ``uv init --lib <name>`` writes.

    Args:
        tmp_path: Where the projects are written, the first one at its root.

    Returns:
        A function taking the distribution's name, the inside of
        ``[project].dependencies`` and the project's directory (``tmp_path``
        by default), returning the directory.
    """

    def write(name: str = "acme-interloper", dependencies: str = "", root: Path = tmp_path) -> Path:
        package = root / "src" / name.replace("-", "_")
        package.mkdir(parents=True)
        (package / "__init__.py").write_text('def hello() -> str:\n    return "Hello"\n')
        (root / "pyproject.toml").write_text(PYPROJECT.format(name=name, dependencies=dependencies))
        return root

    return write


@pytest.fixture
def acme(library: Callable[..., Path], monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fresh ``acme-interloper`` library project, made the working directory.

    Args:
        library: Writes the project.
        monkeypatch: Restores the working directory afterwards.

    Returns:
        The project's directory.
    """
    root = library()
    monkeypatch.chdir(root)
    return root


@pytest.fixture
def uv(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record the uv commands a project runs instead of running them.

    ``uv add`` is played as far as ``pyproject.toml``: the requirement joins
    ``[project].dependencies``, so a later ``require`` sees it as uv's would.

    Args:
        monkeypatch: Restores ``Project._uv`` afterwards.

    Returns:
        The argument tuples, in the order they were run.
    """
    calls: list[tuple[str, ...]] = []

    def run(project: Project, *args: str) -> None:
        calls.append(args)
        if args[0] == "add":
            pyproject = project.root / "pyproject.toml"
            pyproject.write_text(pyproject.read_text().replace("dependencies = [", f'dependencies = ["{args[1]}", ', 1))

    monkeypatch.setattr(Project, "_uv", run)
    return calls


@pytest.fixture
def extension() -> Callable[..., None]:
    """Run ``interloper extension`` through the parser the CLI builds.

    Returns:
        A function taking the arguments after ``interloper extension``.
    """

    def run(*argv: str) -> None:
        parser = argparse.ArgumentParser(prog="interloper")
        ExtensionCommand().attach(parser.add_subparsers(dest="command"), "extension")
        args = parser.parse_args(["extension", *argv])
        args.handler(args)

    return run


@pytest.fixture
def tree() -> Callable[[Path], dict[str, bytes]]:
    """Read every file under a directory, to compare before and after a refusal.

    Returns:
        A function mapping each file's path, relative to the directory, to
        its bytes.
    """

    def read(root: Path) -> dict[str, bytes]:
        return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}

    return read
