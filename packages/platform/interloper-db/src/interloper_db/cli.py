"""``interloper db``: bootstrap, reset and migrate the platform database."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from typing import ClassVar

from interloper.cli import Command
from interloper.settings import AppSettings

from interloper_db.engine import init_engine
from interloper_db.provision import bootstrap, downgrade, drop_database, upgrade


class DatabaseInitCommand(Command):
    """Ensure the database exists, create its tables and migrate it to head (idempotent)."""

    def run(self, args: argparse.Namespace) -> None:
        """Bootstrap the database: create it if missing, create its tables and migrate it to head.

        Idempotent and concurrent-safe.

        Args:
            args: The parsed command line; this command takes no arguments.
        """
        postgres = AppSettings.get().postgres
        print(f"Bootstrapping the database at {postgres.location}...")
        bootstrap(postgres.dsn)
        print("Done.")


class DatabaseResetCommand(Command):
    """Drop and recreate all tables."""

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the flag that skips the confirmation prompt.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    def run(self, args: argparse.Namespace) -> None:
        """Drop the database entirely and bootstrap it from scratch.

        Args:
            args: The parsed command line, carrying ``yes`` to skip the confirmation prompt.
        """
        postgres = AppSettings.get().postgres

        if not args.yes:
            answer = input(f"This will DROP and recreate the database at {postgres.location}. Continue? [y/N] ")
            if answer.lower() not in ("y", "yes"):
                print("Aborted.")
                sys.exit(0)

        print("Dropping database...")
        drop_database(postgres.dsn)
        print(f"Bootstrapping the database at {postgres.location}...")
        bootstrap(postgres.dsn)
        print("Done.")


class DatabaseUpgradeCommand(Command):
    """Run Alembic migrations to head (or a specific revision)."""

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the revision to migrate to.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("revision", nargs="?", default="head", help="Target revision (default: head)")

    def run(self, args: argparse.Namespace) -> None:
        """Run Alembic migrations.

        Args:
            args: The parsed command line, carrying ``revision`` (the target, default ``head``).
        """
        init_engine(AppSettings.get().postgres.dsn)
        print(f"Running migrations to {args.revision}...")
        upgrade(revision=args.revision)
        print("Done.")


class DatabaseDowngradeCommand(Command):
    """Downgrade Alembic migrations."""

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the revision to downgrade to.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("revision", nargs="?", default="-1", help="Target revision (default: -1)")

    def run(self, args: argparse.Namespace) -> None:
        """Downgrade Alembic migrations.

        Args:
            args: The parsed command line, carrying ``revision`` (the target, default ``-1``).
        """
        init_engine(AppSettings.get().postgres.dsn)
        print(f"Downgrading to {args.revision}...")
        downgrade(revision=args.revision)
        print("Done.")


class DatabaseCommand(Command):
    """Database operations."""

    commands: ClassVar[Mapping[str, type[Command]]] = {
        "init": DatabaseInitCommand,
        "reset": DatabaseResetCommand,
        "upgrade": DatabaseUpgradeCommand,
        "downgrade": DatabaseDowngradeCommand,
    }
