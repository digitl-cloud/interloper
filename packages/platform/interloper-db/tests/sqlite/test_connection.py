"""Tests for ``interloper_db.sqlite.connection``: how each SQLite connection is set up and how it begins."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import Engine, event, text

from interloper_db import engine as engine_module
from interloper_db.engine import init_engine
from interloper_db.sqlite import SQLiteConnection


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    """An engine on a SQLite file, set up by `SQLiteConnection` as `init_engine` sets every SQLite one up.

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


class TestConfigure:
    """The pragmas and functions every connection runs with."""

    def test_the_journal_is_a_write_ahead_log(self, engine: Engine) -> None:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"

    def test_foreign_keys_are_enforced(self, engine: Engine) -> None:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1

    def test_a_locked_database_is_waited_for(self, engine: Engine) -> None:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30_000

    def test_gen_random_uuid_gives_an_id_as_sqlite_stores_them(self, engine: Engine) -> None:
        with engine.connect() as connection:
            value = connection.execute(text("SELECT gen_random_uuid()")).scalar()

        assert isinstance(value, str)
        assert len(value) == 32
        assert UUID(hex=value).version == 4


class TestNotify:
    """``notify()`` holds a payload on the connection until a new transaction begins."""

    def test_a_payload_is_held_on_its_connection(self, engine: Engine) -> None:
        with engine.connect() as connection:
            connection.execute(text("SELECT notify('a'), notify('b')"))

            assert connection.connection.info[SQLiteConnection.NOTIFICATIONS] == ["a", "b"]

    def test_a_new_transaction_drops_what_the_last_one_held(self, engine: Engine) -> None:
        with engine.connect() as connection:
            connection.execute(text("SELECT notify('a')"))
            connection.rollback()
            connection.execute(text("SELECT 1"))

            assert connection.connection.info[SQLiteConnection.NOTIFICATIONS] == []


class TestBegin:
    """Every transaction takes the write lock as it begins."""

    def test_a_transaction_holds_the_write_lock_from_its_first_read(self, engine: Engine, tmp_path: Path) -> None:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            other = sqlite3.connect(tmp_path / "interloper.db", timeout=0)
            try:
                with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                    other.execute("BEGIN IMMEDIATE")
            finally:
                other.close()

    def test_the_lock_is_released_with_the_transaction(self, engine: Engine, tmp_path: Path) -> None:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            connection.commit()
            other = sqlite3.connect(tmp_path / "interloper.db", timeout=0)
            try:
                other.execute("BEGIN IMMEDIATE")
                other.rollback()
            finally:
                other.close()

    def test_a_second_transaction_waits_for_the_first(self, engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(SQLiteConnection, "BUSY_TIMEOUT_MILLISECONDS", 0)
        engine.dispose()
        with engine.connect() as first, engine.connect() as second:
            first.execute(text("SELECT 1"))

            with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                second.execute(text("SELECT 1"))

    def test_the_begin_is_not_among_the_executed_statements(self, engine: Engine) -> None:
        statements: list[str] = []
        event.listen(engine, "before_cursor_execute", lambda *args: statements.append(args[2]))
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        assert statements == ["SELECT 1"]
