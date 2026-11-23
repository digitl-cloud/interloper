"""Tests for ``interloper_db.sqlite.base``: how a SQLite database is brought up to what the release serves."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, inspect
from sqlalchemy.engine import make_url
from sqlmodel import SQLModel

from interloper_db import engine as engine_module
from interloper_db.engine import init_engine
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

        with pytest.raises(SystemExit, match=r"at revision 0002.*delete it"):
            SQLiteSchema(engine).create(_alembic_config(engine))


class TestCreateFolder:
    """The folder a database file goes in."""

    def test_the_folder_of_a_file_is_created(self, tmp_path: Path) -> None:
        SQLiteSchema.create_folder(make_url(f"sqlite:///{tmp_path}/a/b/interloper.db"))

        assert (tmp_path / "a" / "b").is_dir()

    @pytest.mark.parametrize("url", ["sqlite://", "sqlite:///:memory:"])
    def test_an_in_memory_database_has_none(self, url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)

        SQLiteSchema.create_folder(make_url(url))

        assert list(tmp_path.iterdir()) == []
