"""Database provisioning: tables via SQLModel, everything else via Alembic."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Engine

if TYPE_CHECKING:
    from alembic.config import Config

import interloper_db.models as _models  # noqa: F401 — ensure all models are registered
from interloper_db.engine import get_engine


def upgrade(engine: Engine | None = None, revision: str = "head") -> None:
    """Run Alembic migrations up to the given revision.

    Args:
        engine: SQLAlchemy engine. Defaults to the global engine.
        revision: Target revision (default ``"head"``).
    """
    from alembic import command

    _eng = engine or get_engine()  # noqa: F841 — ensure engine is initialised
    command.upgrade(_alembic_config(), revision)


def downgrade(engine: Engine | None = None, revision: str = "-1") -> None:
    """Run Alembic downgrade.

    Args:
        engine: SQLAlchemy engine. Defaults to the global engine.
        revision: Target revision (default: one step back).
    """
    from alembic import command

    _eng = engine or get_engine()  # noqa: F841 — ensure engine is initialised
    cfg = _alembic_config()
    command.downgrade(cfg, revision)


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
    with eng.connect() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": PROVISION_LOCK_KEY})
        try:
            SQLModel.metadata.create_all(eng)
            upgrade(engine=eng)
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": PROVISION_LOCK_KEY})
            lock_conn.commit()


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
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": db_name},
        ).fetchone()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    engine.dispose()


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
    with engine.connect() as conn:
        # Terminate any active connections to the target database.
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": db_name},
        )
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
    engine.dispose()


def _alembic_config() -> Config:
    """Build an Alembic Config pointing at this package's migrations.

    The ``[tool.alembic]`` section in ``pyproject.toml`` is used by the
    Alembic CLI when run from the package directory. This function is
    the programmatic equivalent — it resolves the migrations path from
    the package layout so it works regardless of the working directory.

    Returns:
        Configured ``alembic.config.Config``.
    """
    from pathlib import Path

    from alembic.config import Config

    migrations_dir = Path(__file__).resolve().parent / "migrations"
    cfg = Config()
    cfg.set_main_option("script_location", str(migrations_dir))
    return cfg
