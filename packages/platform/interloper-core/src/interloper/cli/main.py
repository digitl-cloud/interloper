"""Interloper CLI entry point: every command is a `Command` a package manifest's ``cli`` section names."""

from __future__ import annotations

import argparse
import sys

from interloper.cli.command import Command
from interloper.registry import Registry
from interloper.settings import Settings, TelemetrySettings

COMMANDS: Registry[type[Command]] = Registry("cli")


def _load_dotenv() -> None:
    """Load .env file if python-dotenv is available.

    Resolve ``.env`` from the current working directory rather than the
    package location, so it is found relative to where the CLI is invoked
    (e.g. a manifests repo) regardless of how interloper is installed —
    an editable/local checkout otherwise anchors the search to its own tree.
    """
    try:
        from dotenv import find_dotenv, load_dotenv

        load_dotenv(find_dotenv(usecwd=True))
    except ImportError:
        pass


def build_parser(command: str | None = None) -> argparse.ArgumentParser:
    """Assemble the ``interloper`` parser: the named command in full, or every installed one by name alone.

    Building one command imports one package, so ``interloper run`` neither
    pays for nor breaks on an installed api or app. The list imports none: it
    names the commands the manifests declare, and each command's own help
    says what it does. The documentation site renders the CLI reference from
    each command's parser, so every flag's ``help`` and every command's
    ``description`` is user-facing copy.

    Args:
        command: The command the process runs; ``None``, or a name no
            installed package registers, lists every command (for the usage
            and its error).

    Returns:
        The root parser.
    """
    parser = argparse.ArgumentParser(
        prog="interloper",
        description="Interloper CLI",
        epilog="Run 'interloper <command> --help' for what a command does and the arguments it takes.",
    )
    subparsers = parser.add_subparsers(dest="command", title="commands", metavar="<command>")
    if command and (named := COMMANDS.get(command)):
        named().attach(subparsers, command)
    else:
        for name in COMMANDS.names():
            # A help, even empty, lists the name on its own line; without one, only the usage error names it.
            subparsers.add_parser(name, help="")
    return parser


def main() -> None:
    """Run one command: load ``.env``, start telemetry, call the handler, then shut down and forget its settings."""
    _load_dotenv()

    parser = build_parser(next((argument for argument in sys.argv[1:] if not argument.startswith("-")), None))
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    from interloper.telemetry import init_telemetry, shutdown_telemetry

    init_telemetry(TelemetrySettings.get())
    try:
        args.handler(args)
    finally:
        shutdown_telemetry()
        Settings.clear_active()


if __name__ == "__main__":
    main()
