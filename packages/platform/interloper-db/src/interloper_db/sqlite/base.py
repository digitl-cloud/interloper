"""A SQLite database's schema: the models' tables, the triggers Postgres's migrations hold, a migration revision."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Engine
from sqlalchemy.engine import URL

from interloper_db.sqlite.folds import SQLiteFolds

if TYPE_CHECKING:
    from alembic.config import Config


class SQLiteSchema:
    """Bring a SQLite database up to what this release serves, as `create_all` does a Postgres one.

    A new database is built from the models and stamped at the migration
    head: migrations 0001 to 0003 hold Postgres's extension, functions and
    triggers, and never run on SQLite. An existing one gains any table a
    newer release declares and runs the migrations after its revision, which
    are written to run on both dialects. Either way its triggers are dropped
    and created again, so the file always carries this release's.
    """

    def __init__(self, engine: Engine) -> None:
        """Hold the engine the schema is written through.

        Args:
            engine: An engine on a SQLite database, set up by `SQLiteConnection`.
        """
        self.engine = engine

    @staticmethod
    def create_folder(url: URL) -> None:
        """Create the folder a database file goes in, which SQLite will not; an in-memory database has none.

        Args:
            url: The database's URL.
        """
        if url.database and url.database != ":memory:":
            Path(url.database).parent.mkdir(parents=True, exist_ok=True)

    def create(self, migrations: Config) -> None:
        """Create the tables, stamp or upgrade the migration revision, and recreate the triggers.

        Args:
            migrations: The Alembic configuration of this package's migrations, run against the engine.
        """
        from alembic import command
        from alembic.runtime.migration import MigrationContext
        from sqlmodel import SQLModel

        with self.engine.begin() as connection:
            revision = MigrationContext.configure(connection).get_current_revision()
            SQLModel.metadata.create_all(connection)
        if revision is None:
            command.stamp(migrations, "head")
        else:
            command.upgrade(migrations, "head")
        with self.engine.begin() as connection:
            for statement in self.statements():
                connection.exec_driver_sql(statement)

    def statements(self) -> Iterator[str]:
        """The statements that recreate every trigger a SQLite database carries.

        Yields:
            One DDL statement at a time.
        """
        yield from SQLiteFolds().statements()
