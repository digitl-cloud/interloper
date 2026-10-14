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


def build_parser() -> argparse.ArgumentParser:
    """Assemble the ``interloper`` parser with every installed command, sorted by name.

    The documentation site renders the CLI reference from this parser, so
    every flag's ``help`` and every command's ``description`` is user-facing
    copy.

    Returns:
        The root parser.
    """
    parser = argparse.ArgumentParser(prog="interloper", description="Interloper CLI")
    subparsers = parser.add_subparsers(dest="command")
    for _name, register in sorted(COMMANDS.items()):
        register(subparsers)
    return parser


def main() -> None:
    """Run one command: load ``.env``, activate settings and telemetry, call the handler, shut down."""
    _load_dotenv()

    parser = build_parser()
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
