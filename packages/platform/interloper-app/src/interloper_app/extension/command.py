"""What the commands acting on the extension in the current directory share."""

from __future__ import annotations

import argparse
import keyword
import re
import sys
from pathlib import Path

from interloper.cli import Command

from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.project import Project

SNAKE_CASE = re.compile(r"[a-z][a-z0-9_]*")


class ProjectCommand(Command):
    """A command acting on the uv project in the current directory.

    A refusal is one line on stderr naming the command, and exit status 1;
    `execute` raises `ExtensionError` for each.
    """

    def run(self, args: argparse.Namespace) -> None:
        """Read the project in the current directory and act on it.

        Args:
            args: The parsed command line.
        """
        if sys.platform == "win32":
            self._parser.exit(1, f"{self._parser.prog} runs on macOS and Linux; on Windows, run it in WSL\n")
        try:
            self.execute(Project.from_directory(Path.cwd()), args)
        except ExtensionError as error:
            self._parser.exit(1, f"{self._parser.prog}: {error}\n")

    def execute(self, project: Project, args: argparse.Namespace) -> None:
        """Act on the project.

        Args:
            project: The project in the current directory.
            args: The parsed command line.

        Raises:
            NotImplementedError: Always; each command implements it.
        """
        raise NotImplementedError

    @staticmethod
    def check_name(value: str, what: str) -> str:
        """Refuse a name the manifest, the catalog or the CLI would not take.

        Args:
            value: The name.
            what: What the name is, as the refusal names it (``the key``).

        Returns:
            The name, unchanged.

        Raises:
            ExtensionError: If the name is not snake_case, or is a Python
                keyword, which no import string can hold.
        """
        if not SNAKE_CASE.fullmatch(value):
            raise ExtensionError(
                f"{what} '{value}' is not snake_case: lowercase letters, digits and underscores, starting with a letter"
            )
        if keyword.iskeyword(value):
            raise ExtensionError(f"{what} '{value}' is a Python keyword, which no import string can hold")
        return value
