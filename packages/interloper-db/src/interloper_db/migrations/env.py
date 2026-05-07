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
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
