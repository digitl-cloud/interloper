"""Tests for ``interloper_db.sqlite.base``: how a SQLite database is brought up to what the release serves."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, inspect
from sqlmodel import SQLModel

from interloper_db import engine as engine_module
from interloper_db.engine import init_engine
from interloper_db.errors import SQLiteRevisionError
from interloper_db.provision import _alembic_config
from interloper_db.sqlite import SQLiteFolds, SQLiteNotifications, SQLiteSchema


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    """An engine on a SQLite file that does not exist yet.

    Args:
        tmp_path: Pytest's temporary directory, which holds the file.

    Yields:
        The engine, disposed once the test finishes.
    """
    engine = init_engine(f"sqlite:///{tmp_path}/interloper.db")
    try:
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None


def _revision(engine: Engine) -> str | None:
    with engine.connect() as connection:
        return MigrationContext.configure(connection).get_current_revision()


def _expected_triggers() -> set[str]:
    return set(SQLiteFolds.TRIGGERS) | set(SQLiteNotifications().triggers())


def _triggers(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return set(connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'trigger'").scalars())


class TestCreate:
    """Tables from the models, the migration revision, the triggers."""

    def test_a_new_database_says_it_was_created(self, engine: Engine, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO, logger="interloper_db")

        SQLiteSchema(engine).create(_alembic_config(engine))

        head = ScriptDirectory.from_config(_alembic_config(engine)).get_current_head()
        assert [record.getMessage() for record in caplog.records if record.name.startswith("interloper_db")] == [
            f"Database created at {head}"
        ]

    def test_an_existing_database_says_it_is_up_to_date(self, engine: Engine, caplog: pytest.LogCaptureFixture) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))
        caplog.clear()
        caplog.set_level(logging.INFO, logger="interloper_db")

        SQLiteSchema(engine).create(_alembic_config(engine))

        assert [record.getMessage() for record in caplog.records if record.name.startswith("interloper_db")] == [
            "Database up to date"
        ]

    def test_a_new_database_holds_every_table_the_models_declare(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))

        assert set(SQLModel.metadata.tables) <= set(inspect(engine).get_table_names())

    def test_a_new_database_is_stamped_at_the_head(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))

        assert _revision(engine) == ScriptDirectory.from_config(_alembic_config(engine)).get_current_head()

    def test_it_carries_the_fold_and_notifying_triggers(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))

        assert _triggers(engine) == _expected_triggers()

    def test_an_existing_database_gets_its_triggers_back(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TRIGGER trg_events_fold_execution")

        SQLiteSchema(engine).create(_alembic_config(engine))

        assert _triggers(engine) == _expected_triggers()

    def test_an_existing_database_gains_a_table_a_newer_release_declares(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE conversations")

        SQLiteSchema(engine).create(_alembic_config(engine))

        assert "conversations" in inspect(engine).get_table_names()

    def test_a_database_at_another_revision_is_refused(self, engine: Engine) -> None:
        SQLiteSchema(engine).create(_alembic_config(engine))
        with engine.begin() as connection:
            connection.exec_driver_sql("UPDATE alembic_version SET version_num = '0002'")

        with pytest.raises(SQLiteRevisionError, match=r"at revision 0002.*delete it") as raised:
            SQLiteSchema(engine).create(_alembic_config(engine))

        assert raised.value.path == Path(str(engine.url.database))
        assert raised.value.revision == "0002"
        assert raised.value.head == ScriptDirectory.from_config(_alembic_config(engine)).get_current_head()
