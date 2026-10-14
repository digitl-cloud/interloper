"""``interloper db``: bootstrap, reset and migrate the platform database."""

from __future__ import annotations

import argparse
import sys

from interloper.catalog import Catalog
from interloper.settings import AppSettings

from interloper_db.engine import init_engine
from interloper_db.provision import bootstrap, downgrade, drop_database, upgrade
from interloper_db.store import Store


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``db`` command group.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    db_parser = subparsers.add_parser("db", help="Database operations", description="Database operations.")
    db_subparsers = db_parser.add_subparsers(dest="db_command")

    init_parser = db_subparsers.add_parser(
        "init",
        help="Ensure database, create tables, migrate to head, promote configured super-admins (idempotent)",
    )
    init_parser.set_defaults(handler=_cmd_init)

    reset_parser = db_subparsers.add_parser("reset", help="Drop and recreate all tables")
    reset_parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    reset_parser.set_defaults(handler=_cmd_reset)

    upgrade_parser = db_subparsers.add_parser("upgrade", help="Run Alembic migrations to head (or a specific revision)")
    upgrade_parser.add_argument("revision", nargs="?", default="head", help="Target revision (default: head)")
    upgrade_parser.set_defaults(handler=_cmd_upgrade)

    downgrade_parser = db_subparsers.add_parser("downgrade", help="Downgrade Alembic migrations")
    downgrade_parser.add_argument("revision", nargs="?", default="-1", help="Target revision (default: -1)")
    downgrade_parser.set_defaults(handler=_cmd_downgrade)

    db_parser.set_defaults(handler=lambda args: db_parser.print_help())


def _cmd_init(args: argparse.Namespace) -> None:
    """Bootstrap the database and promote the configured super-admins.

    Idempotent and concurrent-safe. ``auth.super_admin_emails`` is applied here
    as well as at login, so a deployment shipping a new list promotes the
    profiles that already exist without waiting for them to sign in again.

    Args:
        args: Parsed CLI arguments; this subcommand takes none of its own.
    """
    settings = AppSettings.get()
    print(f"Bootstrapping the database at {settings.postgres.location}...")
    engine = bootstrap(settings.postgres.dsn)
    store = Store(catalog=Catalog(components={}), engine=engine)
    for profile in store.profiles.promote_super_admins(settings.auth.super_admin_emails):
        print(f"Promoted {profile.email} to super-admin.")
    print("Done.")


def _cmd_reset(args: argparse.Namespace) -> None:
    """Drop the database entirely and bootstrap it from scratch.

    Args:
        args: Parsed CLI arguments, carrying ``yes`` to skip the confirmation prompt.
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


def _cmd_upgrade(args: argparse.Namespace) -> None:
    """Run Alembic migrations.

    Args:
        args: Parsed CLI arguments, carrying ``revision`` (the target, default ``head``).
    """
    init_engine(AppSettings.get().postgres.dsn)
    print(f"Running migrations to {args.revision}...")
    upgrade(revision=args.revision)
    print("Done.")


def _cmd_downgrade(args: argparse.Namespace) -> None:
    """Downgrade Alembic migrations.

    Args:
        args: Parsed CLI arguments, carrying ``revision`` (the target, default ``-1``).
    """
    init_engine(AppSettings.get().postgres.dsn)
    print(f"Downgrading to {args.revision}...")
    downgrade(revision=args.revision)
    print("Done.")
