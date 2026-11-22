"""Database provisioning: tables via SQLModel, everything else via Alembic."""

from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING

from sqlalchemy import Engine

if TYPE_CHECKING:
    from alembic.config import Config

import interloper_db.models as _models  # noqa: F401 — ensure all models are registered
from interloper_db.engine import get_engine, init_engine

FOLDED_HEAD = "026"
"""The head of the migrations folded into 0001 to 0003, which a database at it already matches."""
FOLDED_INTO = "0003"
"""The revision a database at :data:`FOLDED_HEAD` is stamped at, the folded chain's last."""


def upgrade(engine: Engine | None = None, revision: str = "head") -> None:
    """Run Alembic migrations up to the given revision.

    A database still at the head of the folded migrations is stamped at the
    revision that folded them first, its schema already theirs, so it runs
    only what came after.

    Args:
        engine: The engine the migrations run against. Defaults to the global engine.
        revision: Target revision (default ``"head"``).
    """
    from alembic import command
    from alembic.runtime.migration import MigrationContext

    engine = engine or get_engine()
    config = _alembic_config(engine)
    with engine.connect() as connection:
        current = MigrationContext.configure(connection).get_current_revision()
    if current == FOLDED_HEAD:
        command.stamp(config, FOLDED_INTO, purge=True)
    command.upgrade(config, revision)


def downgrade(engine: Engine | None = None, revision: str = "-1") -> None:
    """Run Alembic downgrade.

    Args:
        engine: The engine the migrations run against. Defaults to the global engine.
        revision: Target revision (default: one step back).
    """
    from alembic import command

    command.downgrade(_alembic_config(engine or get_engine()), revision)


def create_all(engine: Engine | None = None) -> None:
    """Create all tables and run Alembic migrations to head.

    1. ``SQLModel.metadata.create_all()`` creates tables idempotently.
    2. ``alembic upgrade head`` applies any pending migrations (views,
       indexes, functions, triggers). All migrations use idempotent
       DDL (``CREATE OR REPLACE``, ``IF NOT EXISTS``), so this is safe
       to call on both fresh and existing databases.

    Safe to call multiple times — and concurrent calls from sibling
    processes are serialized by a Postgres advisory lock, since
    ``CREATE TABLE IF NOT EXISTS`` is not race-safe in Postgres
    (concurrent callers can both pass the existence check and then
    collide on ``pg_type_typname_nsp_index``).

    Args:
        engine: SQLAlchemy engine. Defaults to the global engine.
    """
    from sqlalchemy import text
    from sqlmodel import SQLModel

    eng = engine or get_engine()
    # Stable arbitrary bigint — only this provisioning routine uses it.
    PROVISION_LOCK_KEY = 0x1A7E_5C8E_A7E1_5C8E
    # A migration's CREATE INDEX CONCURRENTLY waits out every older transaction, so the lock is held outside one
    # and polled for: a sibling blocked in pg_advisory_lock would hold a snapshot the holder's build waits out.
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as lock_connection:
        while not lock_connection.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": PROVISION_LOCK_KEY}).scalar():
            sleep(1)
        try:
            SQLModel.metadata.create_all(eng)
            upgrade(engine=eng)
        finally:
            lock_connection.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": PROVISION_LOCK_KEY})


def ensure_database(dsn: str) -> None:
    """Create the PostgreSQL database if it doesn't exist.

    Connects to the default ``postgres`` maintenance database to issue
    ``CREATE DATABASE``. Silently succeeds if the database already exists.

    Args:
        dsn: The target database connection string.
    """
    from urllib.parse import urlparse, urlunparse

    from sqlalchemy import create_engine, text

    parsed = urlparse(dsn)
    db_name = parsed.path.lstrip("/")
    if not db_name:
        return

    maintenance_dsn = urlunparse(parsed._replace(path="/postgres"))
    engine = create_engine(maintenance_dsn, isolation_level="AUTOCOMMIT")
    with engine.connect() as connection:
        exists = connection.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": db_name},
        ).fetchone()
        if not exists:
            connection.execute(text(f'CREATE DATABASE "{db_name}"'))
    engine.dispose()


def bootstrap(dsn: str, *, statement_timeout: float | None = None, create_tables: bool = True) -> Engine:
    """Bring a database up: ensure it exists, bind the process engine, create the tables.

    ``create_all`` is idempotent and serialized by an advisory lock, so
    concurrent bootstraps are safe; production deploys run ``interloper db
    init`` once and start the serving commands with ``create_tables=False``.

    Args:
        dsn: The PostgreSQL connection string.
        statement_timeout: Seconds after which the server cancels a statement; ``None`` keeps the server's setting.
        create_tables: Whether to create the tables and run the migrations.

    Returns:
        The bound engine.
    """
    ensure_database(dsn)
    engine = init_engine(dsn, statement_timeout=statement_timeout)
    if create_tables:
        create_all(engine)
    return engine


def drop_database(dsn: str) -> None:
    """Drop the PostgreSQL database.

    Terminates active connections and issues ``DROP DATABASE``.
    Silently succeeds if the database doesn't exist.

    Args:
        dsn: The target database connection string.
    """
    from urllib.parse import urlparse, urlunparse

    from sqlalchemy import create_engine, text

    parsed = urlparse(dsn)
    db_name = parsed.path.lstrip("/")
    if not db_name:
        return

    maintenance_dsn = urlunparse(parsed._replace(path="/postgres"))
    engine = create_engine(maintenance_dsn, isolation_level="AUTOCOMMIT")
    with engine.connect() as connection:
        connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": db_name},
        )
        connection.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
    engine.dispose()


def _alembic_config(engine: Engine) -> Config:
    """Build an Alembic Config pointing at this package's migrations, run against *engine*.

    The ``[tool.alembic]`` section in ``pyproject.toml`` is used by the
    Alembic CLI when run from the package directory. This function is
    the programmatic equivalent — it resolves the migrations path from
    the package layout so it works regardless of the working directory.
    The engine travels in the config's ``attributes``, where ``env.py``
    reads it.

    Args:
        engine: The engine the migrations run against.

    Returns:
        Configured ``alembic.config.Config``.
    """
    from pathlib import Path

    from alembic.config import Config

    migrations_dir = Path(__file__).resolve().parent / "migrations"
    alembic_config = Config()
    alembic_config.set_main_option("script_location", str(migrations_dir))
    alembic_config.attributes["engine"] = engine
    return alembic_config
