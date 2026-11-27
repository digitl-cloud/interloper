"""A SQLite database's schema: the models' tables, the triggers Postgres's migrations hold, a migration revision."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Engine

from interloper_db.errors import SQLiteRevisionError
from interloper_db.sqlite.folds import SQLiteFolds
from interloper_db.sqlite.notifications import SQLiteNotifications

if TYPE_CHECKING:
    from alembic.config import Config

logger = logging.getLogger(__name__)


class SQLiteSchema:
    """Build a SQLite database from the models, with its triggers, as the migrations build a Postgres one.

    Migrations never run on SQLite: a new database is stamped at the head, and
    one at another revision is refused with `SQLiteRevisionError`, to be
    deleted and made anew by what owns the file.
    """

    def __init__(self, engine: Engine) -> None:
        """Hold the engine the schema is written through.

        Args:
            engine: An engine on a SQLite database, set up by `SQLiteConnection`.
        """
        self.engine = engine

    def create(self, migrations: Config) -> None:
        """Create the tables and recreate the triggers, stamping a new database at the migration head, and say which.

        Args:
            migrations: The Alembic configuration of this package's migrations, run against the engine.

        Raises:
            SQLiteRevisionError: When the database is at another revision than the head.
        """
        from alembic import command
        from alembic.runtime.migration import MigrationContext
        from alembic.script import ScriptDirectory
        from sqlmodel import SQLModel

        head = ScriptDirectory.from_config(migrations).get_current_head()
        with self.engine.begin() as connection:
            revision = MigrationContext.configure(connection).get_current_revision()
            if revision is not None and revision != head:
                raise SQLiteRevisionError(Path(self.engine.url.database or ""), revision, str(head))
            SQLModel.metadata.create_all(connection)
            for statement in self.statements():
                connection.exec_driver_sql(statement)
        if revision is None:
            command.stamp(migrations, "head")
            logger.info("Database created at %s", head)
        else:
            logger.info("Database up to date")

    def statements(self) -> Iterator[str]:
        """The statements that recreate every trigger a SQLite database carries.

        Yields:
            One DDL statement at a time.
        """
        yield from SQLiteFolds().statements()
        yield from SQLiteNotifications().statements()
