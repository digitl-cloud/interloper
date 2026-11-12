"""The ``interloper`` command type: what a manifest's ``cli`` section names.

```yaml
cli:
  backup: acme_interloper.cli:BackupCommand
```

The entry's name is the command's name: the CLI adds the command's parser
under it, so the class never names itself. A command nested in a group is
named by the group's `Command.commands` the same way.
"""

from __future__ import annotations

import argparse
import inspect
from collections.abc import Mapping
from typing import ClassVar


class Command:
    """One ``interloper`` command.

    A subclass is the command, and its docstring is its copy: the first line
    is the help the command list shows, the whole docstring the description
    its own help shows, so it is written for the person at the terminal and
    carries no ``Args:`` or ``Returns:`` sections. `add_arguments` declares
    the command's arguments and `run` executes it, with its heavy imports kept
    inside so that building the parser stays cheap.

    A group lists its commands in `commands` and runs none itself: named
    alone, it prints its help.
    """

    commands: ClassVar[Mapping[str, type[Command]]] = {}

    _parser: argparse.ArgumentParser

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the command's arguments; a command takes none unless it overrides this.

        Args:
            parser: The command's own parser.
        """

    def run(self, args: argparse.Namespace) -> None:
        """Execute the command; unless overridden, print its help, as a group named alone does.

        Args:
            args: The parsed command line.
        """
        self._parser.print_help()

    def attach(self, subparsers: argparse._SubParsersAction, name: str) -> None:
        """Add the command to a parser under ``name``, with its arguments and its own commands.

        Args:
            subparsers: The subcommands of the parser the command joins.
            name: The command's name, from the manifest entry or the group that names it.
        """
        documentation = inspect.cleandoc(type(self).__doc__ or "")
        self._parser = subparsers.add_parser(
            name, help=documentation.partition("\n")[0] or None, description=documentation or None
        )
        self.add_arguments(self._parser)
        self._parser.set_defaults(handler=self.run)
        if self.commands:
            children = self._parser.add_subparsers()
            for child_name, child in self.commands.items():
                child().attach(children, child_name)
