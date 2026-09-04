"""Tests for ``interloper_db.provision``.

The routines here are Postgres-specific — an advisory lock, a maintenance
connection to ``postgres``, ``pg_terminate_backend`` — so the engine is
faked and the statements it is handed are asserted, matching the package's
SQLite-backed unit-test style rather than requiring a live server.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy
from typing_extensions import Self

from interloper_db import provision as provision_module
from interloper_db.provision import (
    _alembic_config,
    create_all,
    downgrade,
    drop_database,
    ensure_database,
    upgrade,
)


class FakeConnection:
    """Records every statement executed against it, plus commits."""

    def __init__(self, rows: list[Any] | None = None) -> None:
        """Set up the fake.

        Args:
            rows: Results ``fetchone`` hands back in order; an exhausted
                queue yields ``None``, standing in for "no such row".
        """
        self.statements: list[tuple[str, dict[str, Any] | None]] = []
        self.commits = 0
        self._rows = list(rows or [])

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

    def commit(self) -> None:
        """Record a commit."""
        self.commits += 1

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
    """Engine stand-in handing out one recording connection."""

    def __init__(self, connection: FakeConnection) -> None:
        """Set up the fake.

        Args:
            connection: The connection every ``connect`` call returns.
        """
        self.connection = connection
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

        recorded: dict[str, list[Any]] = {"upgrade": [], "downgrade": []}
        monkeypatch.setattr(
            command, "upgrade", lambda config, revision: recorded["upgrade"].append((config, revision))
        )
        monkeypatch.setattr(
            command, "downgrade", lambda config, revision: recorded["downgrade"].append((config, revision))
        )
        return recorded

    def test_upgrade_targets_head_by_default(
        self, alembic: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: object())

        upgrade()

        (_config, revision), = alembic["upgrade"]
        assert revision == "head"

    def test_upgrade_takes_an_explicit_revision(self, alembic: dict[str, list[Any]]) -> None:
        upgrade(engine=object(), revision="0007")  # ty: ignore[invalid-argument-type]

        assert alembic["upgrade"][0][1] == "0007"

    def test_downgrade_steps_back_one_by_default(
        self, alembic: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(provision_module, "get_engine", lambda: object())

        downgrade()

        assert alembic["downgrade"][0][1] == "-1"

    def test_downgrade_takes_an_explicit_revision(self, alembic: dict[str, list[Any]]) -> None:
        downgrade(engine=object(), revision="base")  # ty: ignore[invalid-argument-type]

        assert alembic["downgrade"][0][1] == "base"

    def test_without_an_engine_the_global_one_is_resolved(
        self, alembic: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Resolved for its side effect only: alembic's env.py reads the
        # global singleton, so an uninitialized engine must still surface.
        resolved: list[bool] = []
        monkeypatch.setattr(provision_module, "get_engine", lambda: resolved.append(True))

        upgrade()

        assert resolved == [True]


class TestAlembicConfig:
    """The config resolves from the package layout, not the working directory."""

    def test_the_script_location_is_the_packages_migrations(self) -> None:
        config = _alembic_config()

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

        connection = FakeConnection()
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
        assert "pg_advisory_lock" in sql
        assert "pg_advisory_unlock" in sql
        assert sql.index("pg_advisory_lock") < sql.index("pg_advisory_unlock")

    def test_the_lock_and_unlock_share_one_key(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        keys = {params["k"] for _statement, params in harness["connection"].statements if params}
        assert len(keys) == 1

    def test_the_unlock_is_committed(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        assert harness["connection"].commits == 1

    def test_alembic_runs_after_the_tables_exist(self, harness: dict[str, Any]) -> None:
        create_all(harness["engine"])

        assert harness["created"]["bind"] is harness["engine"]
        assert harness["upgrades"] == [harness["engine"]]

    def test_view_backed_models_are_left_to_alembic(self, harness: dict[str, Any]) -> None:
        # A view cannot be created by metadata.create_all; the migrations own it.
        create_all(harness["engine"])

        tables = harness["created"]["tables"]
        assert tables
        assert all(not table.info.get("is_view") for table in tables)

    def test_a_view_backed_model_exists_to_be_excluded(self) -> None:
        # Guards the filter above from silently becoming a no-op.
        from sqlmodel import SQLModel

        assert any(table.info.get("is_view") for table in SQLModel.metadata.sorted_tables)

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

        ensure_database("postgresql://user:pw@host:5432/interloper")

        assert record["dsn"] == "postgresql://user:pw@host:5432/postgres"
        # CREATE DATABASE cannot run inside a transaction.
        assert record["kwargs"]["isolation_level"] == "AUTOCOMMIT"

    def test_a_missing_database_is_created(self, maintenance: Any) -> None:
        record = maintenance(rows=[])

        ensure_database("postgresql://host/interloper")

        sql = _sql(record["connection"])
        assert "SELECT 1 FROM pg_database" in sql
        assert 'CREATE DATABASE "interloper"' in sql

    def test_an_existing_database_is_left_alone(self, maintenance: Any) -> None:
        record = maintenance(rows=[(1,)])

        ensure_database("postgresql://host/interloper")

        assert "CREATE DATABASE" not in _sql(record["connection"])

    def test_the_engine_is_disposed(self, maintenance: Any) -> None:
        record = maintenance()

        ensure_database("postgresql://host/interloper")

        assert record["engine"].disposed == 1

    @pytest.mark.parametrize("dsn", ["postgresql://host", "postgresql://host/"])
    def test_a_dsn_naming_no_database_is_a_no_op(self, maintenance: Any, dsn: str) -> None:
        record = maintenance()

        ensure_database(dsn)

        assert record == {} or "dsn" not in record


class TestDropDatabase:
    """``DROP DATABASE`` first evicts the sessions that would block it."""

    def test_active_connections_are_terminated_before_the_drop(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql://host/interloper")

        sql = _sql(record["connection"])
        assert "pg_terminate_backend" in sql
        assert 'DROP DATABASE IF EXISTS "interloper"' in sql
        assert sql.index("pg_terminate_backend") < sql.index("DROP DATABASE")

    def test_the_terminate_spares_this_session(self, maintenance: Any) -> None:
        # Otherwise the statement kills the connection issuing it.
        record = maintenance()

        drop_database("postgresql://host/interloper")

        assert "pid <> pg_backend_pid()" in _sql(record["connection"])

    def test_it_connects_to_the_maintenance_database(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql://user:pw@host:5432/interloper")

        assert record["dsn"] == "postgresql://user:pw@host:5432/postgres"
        assert record["kwargs"]["isolation_level"] == "AUTOCOMMIT"

    def test_the_engine_is_disposed(self, maintenance: Any) -> None:
        record = maintenance()

        drop_database("postgresql://host/interloper")

        assert record["engine"].disposed == 1

    @pytest.mark.parametrize("dsn", ["postgresql://host", "postgresql://host/"])
    def test_a_dsn_naming_no_database_is_a_no_op(self, maintenance: Any, dsn: str) -> None:
        record = maintenance()

        drop_database(dsn)

        assert record == {} or "dsn" not in record
