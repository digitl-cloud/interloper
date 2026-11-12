"""Tests for ``interloper.cli.command``."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import ClassVar

import pytest

from interloper.cli import Command


class BackupCommand(Command):
    """Back up the database.

    Writes a dump of every table to the given directory.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("directory", help="Where the dump is written")

    def run(self, args: argparse.Namespace) -> None:
        print(f"backed up to {args.directory}")


class RestoreCommand(Command):
    """Restore the database from a dump."""


class StorageCommand(Command):
    """Storage operations."""

    commands: ClassVar[Mapping[str, type[Command]]] = {"backup": BackupCommand, "restore": RestoreCommand}


def _parser(command: Command, name: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="interloper")
    command.attach(parser.add_subparsers(dest="command"), name)
    return parser


def _subcommands(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    return next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))


class TestAttach:
    def test_the_command_is_added_under_the_name_it_is_given(self):
        parser = _parser(BackupCommand(), "dump")

        assert list(_subcommands(parser).choices) == ["dump"]

    def test_the_docstring_is_the_copy(self):
        subcommands = _subcommands(_parser(BackupCommand(), "backup"))

        (choice,) = subcommands._choices_actions
        assert choice.help == "Back up the database."
        assert subcommands.choices["backup"].description == (
            "Back up the database.\n\nWrites a dump of every table to the given directory."
        )

    def test_a_command_without_a_docstring_has_no_copy(self):
        class Bare(Command):
            pass

        subcommands = _subcommands(_parser(Bare(), "bare"))

        assert subcommands._choices_actions[0].help is None
        assert subcommands.choices["bare"].description is None

    def test_the_arguments_are_declared_and_run_handles_the_command(self, capsys):
        args = _parser(BackupCommand(), "backup").parse_args(["backup", "/tmp/dump"])

        args.handler(args)

        assert capsys.readouterr().out == "backed up to /tmp/dump\n"


class TestGroups:
    def test_a_group_holds_its_commands_under_their_names(self):
        group = _subcommands(_parser(StorageCommand(), "storage")).choices["storage"]

        assert list(_subcommands(group).choices) == ["backup", "restore"]

    def test_the_named_command_handles_the_command_line(self, capsys):
        args = _parser(StorageCommand(), "storage").parse_args(["storage", "backup", "/tmp/dump"])

        args.handler(args)

        assert capsys.readouterr().out == "backed up to /tmp/dump\n"

    @pytest.mark.parametrize("argv", [["storage"], ["storage", "restore"]])
    def test_a_command_that_runs_nothing_prints_its_help(self, argv, capsys):
        args = _parser(StorageCommand(), "storage").parse_args(argv)

        args.handler(args)

        assert capsys.readouterr().out.startswith(f"usage: interloper {' '.join(argv)}")
