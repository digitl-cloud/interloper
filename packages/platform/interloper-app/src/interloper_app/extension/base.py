"""``interloper extension``: the commands an extension's author runs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from interloper.cli import Command

from interloper_app.extension.add import AddCommand
from interloper_app.extension.check import CheckCommand
from interloper_app.extension.init import InitCommand


class ExtensionCommand(Command):
    """Create, grow and check interloper extensions."""

    commands: ClassVar[Mapping[str, type[Command]]] = {"init": InitCommand, "add": AddCommand, "check": CheckCommand}
