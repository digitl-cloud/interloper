"""Alembic environment configuration.

Reads the database URL from the engine singleton initialised by
``init_engine()``, so migrations share the exact same connection as the
rest of the application.
"""

from __future__ import annotations

from alembic import context
from sqlmodel import SQLModel

import interloper_db.models as _models  # noqa: F401 — register all models

target_metadata = SQLModel.metadata


def run_migrations_online() -> None:
    """Run migrations against a live database."""
    from interloper_db.engine import get_engine

    connectable = get_engine()

    with connectable.connect() as connection:
        # Bound how long any migration will WAIT for a lock. Without this, a DDL
        # statement (e.g. ALTER TABLE) that can't immediately acquire its lock on
        # a busy table blocks indefinitely — and while it waits at the head of
        # the lock queue it also blocks every new query on that table, taking the
        # API down (504s) and wedging the db-init job forever. Failing fast turns
        # that silent deadlock into a loud, retryable migration error.
        #
        # This caps lock *acquisition* only, not statement runtime, so long
        # `CREATE INDEX CONCURRENTLY` builds are unaffected.
        connection.exec_driver_sql("SET lock_timeout = '10s'")
        # The SET above autobegins a transaction on the SQLAlchemy 2.0
        # connection. Commit it before configuring Alembic: otherwise Alembic
        # sees a pre-existing ("external") transaction, declines to manage
        # transactions itself, and any migration using ``autocommit_block``
        # asserts on a transaction it never opened. The SET is a session GUC,
        # so it persists past this commit.
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            # Wrap each migration in its own transaction. Required so that a
            # migration can open an ``autocommit_block`` (e.g. to build indexes
            # ``CONCURRENTLY``, which Postgres forbids inside a transaction).
            # Without per-migration transactions Alembic keeps a single
            # transaction open for the whole run and ``autocommit_block``
            # asserts there is a transaction to suspend, breaking ``db init``.
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
