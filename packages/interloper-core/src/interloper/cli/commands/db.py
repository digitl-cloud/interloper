"""Database CLI commands. Available when ``interloper-db`` is installed."""

from __future__ import annotations

import argparse
import sys


def register(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register the ``db`` command group.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    db_parser = subparsers.add_parser("db", help="Database operations")
    db_sub = db_parser.add_subparsers(dest="db_command")
    db_parser.set_defaults(requires=["interloper_db"])

    # db init
    init_parser = db_sub.add_parser(
        "init",
        help="Ensure database exists, create tables, and migrate to head (idempotent)",
    )
    init_parser.set_defaults(func=_cmd_init, requires=["interloper_db"])

    # db reset
    reset_parser = db_sub.add_parser("reset", help="Drop and recreate all tables")
    reset_parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    reset_parser.set_defaults(func=_cmd_reset, requires=["interloper_db"])

    # db upgrade
    upgrade_parser = db_sub.add_parser("upgrade", help="Run Alembic migrations to head (or a specific revision)")
    upgrade_parser.add_argument("revision", nargs="?", default="head", help="Target revision (default: head)")
    upgrade_parser.set_defaults(func=_cmd_upgrade, requires=["interloper_db"])

    # db downgrade
    downgrade_parser = db_sub.add_parser("downgrade", help="Downgrade Alembic migrations")
    downgrade_parser.add_argument("revision", nargs="?", default="-1", help="Target revision (default: -1)")
    downgrade_parser.set_defaults(func=_cmd_downgrade, requires=["interloper_db"])

    db_parser.set_defaults(func=lambda args: db_parser.print_help(), requires=["interloper_db"])


def _cmd_init(args: argparse.Namespace) -> None:
    """Ensure database, create tables, and migrate. Idempotent and concurrent-safe."""
    from interloper_db import create_all, ensure_database, init_engine

    from interloper.settings import AppSettings

    settings = AppSettings.get()
    dsn = settings.postgres.dsn

    print(f"Ensuring database at {dsn}...")
    ensure_database(dsn)
    engine = init_engine(dsn)
    print("Creating tables and running migrations...")
    create_all(engine)
    print("Done.")


def _cmd_reset(args: argparse.Namespace) -> None:
    """Drop the database entirely and recreate it from scratch."""
    from interloper_db import create_all, ensure_database, init_engine
    from interloper_db.provision import drop_database

    from interloper.settings import AppSettings

    settings = AppSettings.get()
    dsn = settings.postgres.dsn

    if not args.yes:
        answer = input(f"This will DROP and recreate the database at {dsn}. Continue? [y/N] ")
        if answer.lower() not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    print("Dropping database...")
    drop_database(dsn)
    print("Creating database...")
    ensure_database(dsn)
    engine = init_engine(dsn)
    print("Creating tables and running migrations...")
    create_all(engine)
    print("Done.")


def _cmd_upgrade(args: argparse.Namespace) -> None:
    """Run Alembic migrations."""
    from interloper_db import init_engine, upgrade

    from interloper.settings import AppSettings

    settings = AppSettings.get()
    dsn = settings.postgres.dsn

    init_engine(dsn)
    print(f"Running migrations to {args.revision}...")
    upgrade(revision=args.revision)
    print("Done.")


def _cmd_downgrade(args: argparse.Namespace) -> None:
    """Downgrade Alembic migrations."""
    from interloper_db import downgrade, init_engine

    from interloper.settings import AppSettings

    settings = AppSettings.get()
    dsn = settings.postgres.dsn

    init_engine(dsn)
    print(f"Downgrading to {args.revision}...")
    downgrade(revision=args.revision)
    print("Done.")
