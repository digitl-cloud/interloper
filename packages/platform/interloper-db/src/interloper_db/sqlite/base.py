"""A SQLite database's schema: the models' tables, the triggers Postgres's migrations hold, a migration revision."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Engine
from sqlalchemy.engine import URL

from interloper_db.sqlite.folds import SQLiteFolds
from interloper_db.sqlite.notifications import SQLiteNotifications

if TYPE_CHECKING:
    from alembic.config import Config


class SQLiteSchema:
    """Build a SQLite database from the models, with its triggers, as the migrations build a Postgres one.

    Migrations never run on SQLite: a new database is stamped at the head, and
    one at another revision is refused, to be deleted and made anew.
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
        """Create the tables and recreate the triggers, stamping a new database at the migration head.

        Args:
            migrations: The Alembic configuration of this package's migrations, run against the engine.

        Raises:
            SystemExit: When the database is at another revision than the head.
        """
        from alembic import command
        from alembic.runtime.migration import MigrationContext
        from alembic.script import ScriptDirectory
        from sqlmodel import SQLModel

        head = ScriptDirectory.from_config(migrations).get_current_head()
        with self.engine.begin() as connection:
            revision = MigrationContext.configure(connection).get_current_revision()
            if revision not in (None, head):
                raise SystemExit(
                    f"Error: the SQLite database {self.engine.url.database} is at revision {revision}, and this "
                    f"release needs {head}. SQLite databases are never migrated: delete it, with its -wal and -shm "
                    "files, to start afresh."
                )
            SQLModel.metadata.create_all(connection)
            for statement in self.statements():
                connection.exec_driver_sql(statement)
        if revision is None:
            command.stamp(migrations, "head")

    def statements(self) -> Iterator[str]:
        """The statements that recreate every trigger a SQLite database carries.

        Yields:
            One DDL statement at a time.
        """
        yield from SQLiteFolds().statements()
        yield from SQLiteNotifications().statements()
