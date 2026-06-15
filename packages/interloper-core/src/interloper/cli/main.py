"""Interloper CLI entry point."""

from __future__ import annotations

import argparse
import importlib
import sys

from interloper.cli.runtime import apply_cli_overrides
from interloper.settings import AppSettings


def _has_package(name: str) -> bool:
    """Check if a package is importable.

    Returns:
        True if the package can be imported.
    """
    try:
        importlib.import_module(name)
        return True
    except ImportError:
        return False


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


def _enforce_requires(args: argparse.Namespace) -> None:
    """Validate command requirements declared by the parser.

    Raises:
        SystemExit: If required packages are not available.
    """
    missing: list[str] = []

    required_packages: list[str] = list(getattr(args, "requires", []))
    for package in required_packages:
        if not _has_package(package):
            missing.append(package)

    required_any_groups: list[list[str]] = list(getattr(args, "requires_any", []))
    for group in required_any_groups:
        if not any(_has_package(package) for package in group):
            options = ", ".join(group)
            raise SystemExit(f"Error: command '{args.command}' requires at least one of: {options}")

    required_when: dict[str, list[str]] = dict(getattr(args, "requires_when", {}))
    for flag, packages in required_when.items():
        if getattr(args, flag, False) is True:
            for package in packages:
                if not _has_package(package):
                    missing.append(package)

    if missing:
        formatted = ", ".join(sorted(set(missing)))
        raise SystemExit(f"Error: command '{args.command}' requires package(s): {formatted}")


def main() -> None:
    """CLI entry point for interloper."""
    _load_dotenv()

    parser = argparse.ArgumentParser(prog="interloper", description="Interloper CLI")
    subparsers = parser.add_subparsers(dest="command")

    from interloper.cli.commands.agent import register as register_agent
    from interloper.cli.commands.app import register as register_app
    from interloper.cli.commands.db import register as register_db
    from interloper.cli.commands.launch import register as register_launch
    from interloper.cli.commands.run import register as register_run

    register_run(subparsers)
    register_db(subparsers)
    register_app(subparsers)
    register_launch(subparsers)
    register_agent(subparsers)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    _enforce_requires(args)

    settings = AppSettings.get()
    if args.command == "app":
        settings = apply_cli_overrides(args, settings)
    AppSettings.activate(settings)

    try:
        if hasattr(args, "func"):
            args.func(args)
        else:
            parser.parse_args([args.command, "--help"])
    finally:
        AppSettings.clear_active()


if __name__ == "__main__":
    main()
