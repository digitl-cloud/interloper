"""Tests for ``interloper_db.provision``.

The routines here are Postgres-specific — an advisory lock, a maintenance
connection to ``postgres``, ``pg_terminate_backend`` — so the engine is
faked and the statements it is handed are asserted, matching the package's
SQLite-backed unit-test style rather than requiring a live server.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql

from interloper_db import engine as engine_module
from interloper_db import provision as provision_module
from interloper_db.provision import (
    _alembic_config,
    connect,
    create_all,
    downgrade,
    drop_database,
    ensure_database,
    upgrade,
)


class FakeConnection:
    """Records every statement executed against it, plus its execution options."""

    def __init__(self, rows: list[Any] | None = None) -> None:
        """Set up the fake.

        Args:
            rows: Results ``fetchone`` and ``scalar`` hand back in order; an
                exhausted queue yields ``None``, standing in for "no such row".
        """
        self.statements: list[tuple[str, dict[str, Any] | None]] = []
        self.options: dict[str, Any] = {}
        self._rows = list(rows or [])

    def execution_options(self, **options: Any) -> Self:
        """Record execution options, as SQLAlchemy sets them on the connection in place.

        Args:
            **options: The options set.

        Returns:
            This connection.
        """
        self.options.update(options)
        return self

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeConnection:
        """Record a statement and return self as its result proxy.

        Args:
            statement: The SQLAlchemy text clause executed.
            params: Bound parameters, when any.

        Returns:
            This connection, standing in for the result.
        """
        self.statements.append((str(statement), params))
        return self

    def fetchone(self) -> Any:
        """Pop the next queued row.

        Returns:
            The next row, or ``None`` once the queue is empty.
        """
        return self._rows.pop(0) if self._rows else None

    def scalar(self) -> Any:
        """Pop the next queued row as a scalar.

        Returns:
            The next row, or ``None`` once the queue is empty.
        """
        return self.fetchone()

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This connection.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """Leave the context.

        Args:
            *args: Exception triple, ignored.
        """


class FakeEngine:
    """A Postgres engine stand-in handing out one recording connection."""

    def __init__(self, connection: FakeConnection) -> None:
        """Set up the fake.

        Args:
            connection: The connection every ``connect`` call returns.
        """
        self.connection = connection
        self.dialect = postgresql.dialect()
        self.disposed = 0

    def connect(self) -> FakeConnection:
        """Hand out the recording connection.

        Returns:
            The connection.
        """
        return self.connection

    def dispose(self) -> None:
        """Record a dispose."""
        self.disposed += 1


def _sql(connection: FakeConnection) -> str:
    return " | ".join(statement for statement, _ in connection.statements)


class TestMigrations:
    """``upgrade`` / ``downgrade`` delegate to Alembic with this package's config."""

    @pytest.fixture
    def alembic(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
        """Record the Alembic commands instead of running them.

        Args:
            monkeypatch: Fixture used to swap the command module.

        Returns:
            The recorded ``upgrade`` and ``downgrade`` calls.
        """
        from alembic import command

        recorded: dict[str, list[Any]] = {"upgrade": [], "downgrade": [], "stamp": []}
        monkeypatch.setattr(command, "upgrade", lambda config, revision: recorded["upgrade"].append((config, revision)))
        monkeypatch.setattr(
            command, "stamp", lambda config, revision, purge: recorded["stamp"].append((revision, purge))
        )
        monkeypatch.setattr(
            command, "downgrade", lambda config, revision: recorded["downgrade"].append((config, revision))
        )
        return recorded

    @pytest.fixture
    def engine(self) -> sqlalchemy.Engine:
        """An engine the migrations are handed, read only for the revision it is at.

        Returns:
            An in-memory SQLite engine, one database for every connection.
        """
        return sqlalchemy.create_engine("sqlite://", poolclass=sqlalchemy.pool.StaticPool)

    @staticmethod
    def _at(engine: sqlalchemy.Engine, revision: str) -> None:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32))")
            connection.exec_driver_sql("DELETE FROM alembic_version")
            connection.exec_driver_sql("INSERT INTO alembic_version VALUES (?)", (revision,))

    def _migrating_to(self, engine: sqlalchemy.Engine, revision: str, monkeypatch: pytest.MonkeyPatch) -> None:
        from alembic import command

        monkeypatch.setattr(command, "upgrade", lambda config, target: self._at(engine, revision))

    def test_upgrade_says_what_it_migrated(
        self,
        alembic: dict[str, list[Any]],
        engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        self._at(engine, "0003")
        self._migrating_to(engine, "0004", monkeypatch)

        with caplog.at_level(logging.INFO, logger="interloper_db"):
            upgrade(engine=engine)

        assert caplog.messages == ["Database migrated from 0003 to 0004"]

    def test_upgrade_says_it_created_a_new_database(
        self,
        alembic: dict[str, list[Any]],
        engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        self._migrating_to(engine, "0004", monkeypatch)

        with caplog.at_level(logging.INFO, logger="interloper_db"):
            upgrade(engine=engine)

        assert caplog.messages == ["Database created at 0004"]

    def test_upgrade_with_nothing_to_run_says_up_to_date(
        self,
        alembic: dict[str, list[Any]],
        engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        self._at(engine, TestConnect._head())

        with caplog.at_level(logging.INFO, logger="interloper_db"):
            upgrade(engine=engine)

        assert caplog.messages == ["Database up to date"]

    def test_downgrade_says_what_it_undid(
        self,
        alembic: dict[str, list[Any]],
        engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from alembic import command

        self._at(engine, "0004")
        monkeypatch.setattr(command, "downgrade", lambda config, target: self._at(engine, "0003"))

        with caplog.at_level(logging.INFO, logger="interloper_db"):
            downgrade(engine=engine)

        assert caplog.messages == ["Database downgraded from 0004 to 0003"]

    def test_upgrade_targets_head_by_default(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: engine)

        upgrade()

        ((_config, revision),) = alembic["upgrade"]
        assert revision == "head"

    def test_a_database_at_the_folded_head_is_stamped_at_the_chain_that_folded_it(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine
    ) -> None:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
            connection.exec_driver_sql("INSERT INTO alembic_version VALUES ('026')")

        upgrade(engine=engine)

        assert alembic["stamp"] == [("0003", True)]
        assert alembic["upgrade"][0][1] == "head"

    def test_a_database_on_the_current_chain_is_not_stamped(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine
    ) -> None:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
            connection.exec_driver_sql("INSERT INTO alembic_version VALUES ('0003')")

        upgrade(engine=engine)

        assert alembic["stamp"] == []

    def test_upgrade_takes_an_explicit_revision(self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine) -> None:
        upgrade(engine=engine, revision="0007")

        assert alembic["upgrade"][0][1] == "0007"

    def test_downgrade_steps_back_one_by_default(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: engine)

        downgrade()

        assert alembic["downgrade"][0][1] == "-1"

    def test_downgrade_takes_an_explicit_revision(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine
    ) -> None:
        downgrade(engine=engine, revision="base")

        assert alembic["downgrade"][0][1] == "base"

    def test_the_given_engine_is_the_one_migrated(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine
    ) -> None:
        upgrade(engine=engine)
        downgrade(engine=engine)

        assert alembic["upgrade"][0][0].attributes["engine"] is engine
        assert alembic["downgrade"][0][0].attributes["engine"] is engine

    def test_without_an_engine_the_global_one_is_migrated(
        self, alembic: dict[str, list[Any]], engine: sqlalchemy.Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: engine)

        upgrade()

        assert alembic["upgrade"][0][0].attributes["engine"] is engine


class TestAlembicConfig:
    """The config resolves from the package layout, not the working directory."""

    def test_the_script_location_is_the_packages_migrations(self) -> None:
        config = _alembic_config(sqlalchemy.create_engine("sqlite://"))

        script_location = config.get_main_option("script_location")
        assert script_location is not None
        location = Path(script_location)
        assert location.is_absolute()
        assert location.name == "migrations"
        assert (location / "env.py").is_file()


class TestCreateAll:
    """Table creation is bracketed by an advisory lock, then hands off to Alembic."""

    @pytest.fixture
    def harness(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
        """Fake the engine, the metadata create and the Alembic upgrade.

        Args:
            monkeypatch: Fixture used to swap the collaborators.

        Yields:
            The recording connection plus what ``create_all`` was handed.
        """
        from sqlmodel import SQLModel

        connection = FakeConnection(rows=[True])
        engine = FakeEngine(connection)
        created: dict[str, Any] = {}
        upgrades: list[Any] = []

        monkeypatch.setattr(
            SQLModel.metadata,
            "create_all",
            lambda bind, tables=None: created.update(bind=bind, tables=tables),
        )
        monkeypatch.setattr(provision_module, "upgrade", lambda engine: upgrades.append(engine))
        yield {"connection": connection, "engine": engine, "created": created, "upgrades": upgrades}

    def test_the_advisory_lock_brackets_the_work(self, harness: dict[str, Any]) -> None:
        # CREATE TABLE IF NOT EXISTS is not race-safe in Postgres, so
        # concurrent provisioners must serialize on the lock.
        create_all(harness["engine"])

        sql = _sql(harness["connection"])
        assert "pg_try_advisory_lock" in sql
        assert "pg_advisory_unlock" in sql
        assert sql.index("pg_try_advisory_lock") < sql.index("pg_advisory_unlock")

    def test_the_lock_and_unlock_share_one_key(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        keys = {params["k"] for _statement, params in harness["connection"].statements if params}
        assert len(keys) == 1

    def test_the_lock_is_held_outside_a_transaction(self, harness: dict[str, Any]) -> None:
        # A migration's CREATE INDEX CONCURRENTLY waits out every older transaction, the lock holder's included.
        create_all(harness["engine"])

        assert harness["connection"].options == {"isolation_level": "AUTOCOMMIT"}

    def test_a_taken_lock_is_polled_until_free(self, harness: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
        # A sibling blocked in pg_advisory_lock would hold a snapshot the holder's CREATE INDEX CONCURRENTLY waits out.
        connection = FakeConnection(rows=[False, False, True])
        harness["engine"].connection = connection
        upgrades_while_waiting: list[int] = []
        monkeypatch.setattr(
            provision_module, "sleep", lambda seconds: upgrades_while_waiting.append(len(harness["upgrades"]))
        )

        create_all(harness["engine"])

        sql = _sql(connection)
        assert sql.count("pg_try_advisory_lock") == 3
        assert "pg_advisory_lock" not in sql
        assert upgrades_while_waiting == [0, 0]
        assert len(harness["upgrades"]) == 1

    def test_alembic_runs_after_the_tables_exist(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        assert harness["created"]["bind"] is harness["engine"]
        assert harness["upgrades"] == [harness["engine"]]

    def test_every_table_comes_from_the_models(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        assert harness["created"]["tables"] is None

    def test_the_lock_is_released_even_when_the_work_fails(
        self, harness: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            provision_module, "upgrade", lambda engine: (_ for _ in ()).throw(RuntimeError("migration broke"))
        )

        with pytest.raises(RuntimeError, match="migration broke"):
            create_all(harness["engine"])

        assert "pg_advisory_unlock" in _sql(harness["connection"])

    def test_without_an_engine_the_global_one_is_used(
        self, harness: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: harness["engine"])

        create_all()

        assert harness["created"]["bind"] is harness["engine"]


@pytest.fixture
def maintenance(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Intercept the maintenance-database engine the provisioners build.

    Args:
        monkeypatch: Fixture used to swap ``sqlalchemy.create_engine``.

    Returns:
        A callable taking the rows ``fetchone`` should yield and returning
        the recorded DSN, kwargs, connection and engine.
    """

    def install(rows: list[Any] | None = None) -> dict[str, Any]:
        record: dict[str, Any] = {}
        connection = FakeConnection(rows)
        engine = FakeEngine(connection)

        def create_engine(dsn: str, **kwargs: Any) -> FakeEngine:
            record.update(dsn=dsn, kwargs=kwargs)
            return engine

        monkeypatch.setattr(sqlalchemy, "create_engine", create_engine)
        record["connection"] = connection
        record["engine"] = engine
        return record

    return install


class TestEnsureDatabase:
    """``CREATE DATABASE`` is issued through the ``postgres`` maintenance database."""

    def test_it_connects_to_the_maintenance_database(self, maintenance: Any) -> None:
        record = maintenance()

        ensure_database("postgresql+psycopg://user:pw@host:5432/interloper")

        assert record["dsn"] == "postgresql+psycopg://user:pw@host:5432/postgres"
        # CREATE DATABASE cannot run inside a transaction.
        assert record["kwargs"]["isolation_level"] == "AUTOCOMMIT"

    def test_a_missing_database_is_created(self, maintenance: Any) -> None:
        record = maintenance(rows=[])

        ensure_database("postgresql+psycopg://host/interloper")

        sql = _sql(record["connection"])
        assert "SELECT 1 FROM pg_database" in sql
        assert 'CREATE DATABASE "interloper"' in sql

    def test_an_existing_database_is_left_alone(self, maintenance: Any) -> None:
        record = maintenance(rows=[(1,)])

        ensure_database("postgresql+psycopg://host/interloper")

        assert "CREATE DATABASE" not in _sql(record["connection"])

    def test_the_engine_is_disposed(self, maintenance: Any) -> None:
        record = maintenance()

        ensure_database("postgresql+psycopg://host/interloper")

        assert record["engine"].disposed == 1

    @pytest.mark.parametrize("dsn", ["postgresql+psycopg://host", "postgresql+psycopg://host/"])
    def test_a_dsn_naming_no_database_is_a_no_op(self, maintenance: Any, dsn: str) -> None:
        record = maintenance()

        ensure_database(dsn)

        assert record == {} or "dsn" not in record

    def test_a_sqlite_url_is_left_alone(self, maintenance: Any, tmp_path: Path) -> None:
        record = maintenance()

        ensure_database(f"sqlite:///{tmp_path}/project/.interloper/interloper.db")

        assert list(tmp_path.iterdir()) == []
        assert "dsn" not in record


class TestDropDatabase:
    """``DROP DATABASE`` first evicts the sessions that would block it."""

    def test_active_connections_are_terminated_before_the_drop(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql+psycopg://host/interloper")

        sql = _sql(record["connection"])
        assert "pg_terminate_backend" in sql
        assert 'DROP DATABASE IF EXISTS "interloper"' in sql
        assert sql.index("pg_terminate_backend") < sql.index("DROP DATABASE")

    def test_the_terminate_spares_this_session(self, maintenance: Any) -> None:
        # Otherwise the statement kills the connection issuing it.
        record = maintenance()

        drop_database("postgresql+psycopg://host/interloper")

        assert "pid <> pg_backend_pid()" in _sql(record["connection"])

    def test_it_connects_to_the_maintenance_database(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql+psycopg://user:pw@host:5432/interloper")

        assert record["dsn"] == "postgresql+psycopg://user:pw@host:5432/postgres"
        assert record["kwargs"]["isolation_level"] == "AUTOCOMMIT"

    def test_the_engine_is_disposed(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql+psycopg://host/interloper")

        assert record["engine"].disposed == 1

    @pytest.mark.parametrize("dsn", ["postgresql+psycopg://host", "postgresql+psycopg://host/"])
    def test_a_dsn_naming_no_database_is_a_no_op(self, maintenance: Any, dsn: str) -> None:
        record = maintenance()

        drop_database(dsn)

        assert record == {} or "dsn" not in record


class TestBootstrap:
    """One procedure brings a database up: ensure it, bind the engine, create the tables."""

    def test_ensures_binds_and_creates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[str, Any]] = []
        engine = object()

        def init_engine(dsn: str, statement_timeout: float | None = None) -> object:
            calls.append(("init", statement_timeout))
            return engine

        monkeypatch.setattr(provision_module, "ensure_database", lambda dsn: calls.append(("ensure", dsn)))
        monkeypatch.setattr(provision_module, "init_engine", init_engine)
        monkeypatch.setattr(provision_module, "create_all", lambda bound: calls.append(("create", bound)))

        assert provision_module.bootstrap("postgresql+psycopg://db/x", statement_timeout=30) is engine
        assert calls == [("ensure", "postgresql+psycopg://db/x"), ("init", 30), ("create", engine)]

    def test_a_sqlite_database_is_brought_up_to_what_connect_serves(self, tmp_path: Path) -> None:
        url = f"sqlite:///{tmp_path}/interloper.db"
        try:
            provision_module.bootstrap(url).dispose()
            engine = connect(url)
            engine.dispose()
        finally:
            engine_module._engine = None

        assert (tmp_path / "interloper.db").is_file()


class TestConnect:
    """``connect`` binds the engine to a database this release can serve, and writes nothing to it."""

    @pytest.fixture
    def database(self, tmp_path: Path) -> Iterator[Any]:
        """Make a SQLite database at a revision, and forget the process engine afterwards.

        Args:
            tmp_path: Where the database file lives.

        Yields:
            A function taking the revision (``None`` for no schema) and returning the database's URL.
        """
        url = f"sqlite:///{tmp_path / 'interloper.db'}"

        def at(revision: str | None) -> str:
            engine = sqlalchemy.create_engine(url)
            with engine.begin() as connection:
                if revision is not None:
                    connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
                    connection.exec_driver_sql("INSERT INTO alembic_version VALUES (?)", (revision,))
            engine.dispose()
            return url

        yield at
        engine_module._engine = None

    @staticmethod
    def _head() -> str:
        from alembic.script import ScriptDirectory

        head = ScriptDirectory(str(Path(provision_module.__file__).parent / "migrations")).get_current_head()
        assert head is not None
        return head

    @staticmethod
    def _tables(url: str) -> set[str]:
        engine = sqlalchemy.create_engine(url)
        try:
            return set(sqlalchemy.inspect(engine).get_table_names())
        finally:
            engine.dispose()

    def test_a_database_at_the_head_is_served(self, database: Any) -> None:
        url = database(self._head())

        engine = connect(url)

        assert str(engine.url) == url
        assert engine_module.get_engine() is engine

    def test_a_database_behind_the_release_stops_the_command_naming_db_init(self, database: Any) -> None:
        url = database("0001")

        with pytest.raises(
            SystemExit, match=rf"revision 0001, and this release needs {self._head()}.*interloper db init"
        ):
            connect(url)

    def test_a_database_without_a_schema_stops_the_command(self, database: Any) -> None:
        with pytest.raises(SystemExit, match=r"no schema.*interloper db init"):
            connect(database(None))

    def test_a_database_at_the_folded_head_is_behind(self, database: Any) -> None:
        with pytest.raises(SystemExit, match="revision 026"):
            connect(database(provision_module.FOLDED_HEAD))

    def test_a_database_ahead_of_the_release_is_served_with_a_warning(
        self, database: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        url = database("9999")

        with caplog.at_level("WARNING", logger="interloper_db.provision"):
            connect(url)

        assert "ahead of this release" in caplog.text

    def test_a_served_database_adds_no_line_of_its_own(self, database: Any, caplog: pytest.LogCaptureFixture) -> None:
        url = database(self._head())

        with caplog.at_level("INFO", logger="interloper_db"):
            connect(url)

        assert caplog.records == []

    def test_nothing_is_written(self, database: Any) -> None:
        url = database("0001")

        with pytest.raises(SystemExit):
            connect(url)

        assert self._tables(url) == {"alembic_version"}
