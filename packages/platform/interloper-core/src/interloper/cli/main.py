"""Interloper CLI entry point: every command is contributed through the ``interloper.cli`` group."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from interloper.registry import Registry
from interloper.settings import AppSettings

COMMANDS: Registry[Callable[[argparse._SubParsersAction], None]] = Registry("interloper.cli")


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
    """Assemble the ``interloper`` parser: the named command alone, or every installed one sorted by name.

    Building one command imports one package, so ``interloper run`` neither
    pays for nor breaks on an installed api or app. The documentation site
    renders the CLI reference from the full parser, so every flag's ``help``
    and every command's ``description`` is user-facing copy.

    Args:
        command: The command the process runs; ``None``, or a name no
            package registers, builds every command (for the usage and its
            error).

    Returns:
        The root parser.
    """
    parser = argparse.ArgumentParser(prog="interloper", description="Interloper CLI")
    subparsers = parser.add_subparsers(dest="command")
    register = COMMANDS.get(command) if command else None
    for _name, register_command in [(command, register)] if register else COMMANDS.items():
        register_command(subparsers)
    return parser


def main() -> None:
    """Run one command: load ``.env``, activate settings and telemetry, call the handler, shut down."""
    _load_dotenv()

    parser = build_parser(next((argument for argument in sys.argv[1:] if not argument.startswith("-")), None))
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    AppSettings.activate(AppSettings.get())

    from interloper.telemetry import init_telemetry, shutdown_telemetry

    init_telemetry(AppSettings.get().otel)
    try:
        args.handler(args)
    finally:
        shutdown_telemetry()
        AppSettings.clear_active()


if __name__ == "__main__":
    main()
