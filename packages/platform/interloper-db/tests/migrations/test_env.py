"""Tests for the Alembic environment, ``interloper_db/migrations/env.py``.

The environment applies Postgres session settings, so the engine is faked:
its connections record what they are handed and, like a pooled connection,
go back to the engine for reuse when closed unless invalidated. Alembic's own
migration machinery is stubbed, so only what ``env.py`` does around it runs.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, Self

import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory

import interloper_db


class FakeConnection:
    """A connection that logs its statements and commits, and returns to its engine's pool on close."""

    def __init__(self, engine: FakeEngine) -> None:
        """Set up the fake.

        Args:
            engine: The engine whose pool takes the connection back on close.
        """
        self.engine = engine
        self.invalidated = False

    def exec_driver_sql(self, statement: str) -> None:
        """Log a statement.

        Args:
            statement: The SQL executed.
        """
        self.engine.log.append(statement)

    def commit(self) -> None:
        """Log a commit."""
        self.engine.log.append("COMMIT")

    def invalidate(self) -> None:
        """Discard the connection, keeping it out of the pool once closed."""
        self.invalidated = True

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This connection.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """Close the connection, returning it to the pool unless invalidated.

        Args:
            *args: Exception triple, ignored.
        """
        if not self.invalidated:
            self.engine.pool.append(self)


class FakeEngine:
    """Engine stand-in sharing one log across its connections and keeping the ones returned to it."""

    def __init__(self) -> None:
        """Set up the fake."""
        self.log: list[str] = []
        self.pool: list[FakeConnection] = []

    def connect(self) -> FakeConnection:
        """Open a connection.

        Returns:
            A new connection.
        """
        return FakeConnection(self)


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub Alembic's migration machinery, logging each step to the engine's log.

    Args:
        monkeypatch: Fixture used to swap the environment context's methods.

    Returns:
        The keyword arguments ``env.py`` configures Alembic with.
    """
    recorded: dict[str, Any] = {}

    def configure(self: EnvironmentContext, **kwargs: Any) -> None:
        recorded.update(kwargs)
        kwargs["connection"].engine.log.append("configure")

    def begin_transaction(self: EnvironmentContext) -> AbstractContextManager[None]:
        return nullcontext()

    def run_migrations(self: EnvironmentContext, **kwargs: Any) -> None:
        recorded["connection"].engine.log.append("migrate")

    monkeypatch.setattr(EnvironmentContext, "configure", configure)
    monkeypatch.setattr(EnvironmentContext, "begin_transaction", begin_transaction)
    monkeypatch.setattr(EnvironmentContext, "run_migrations", run_migrations)
    return recorded


def run_env(engine: FakeEngine) -> None:
    """Execute ``env.py`` the way an Alembic command does, against *engine*.

    Args:
        engine: The engine handed over in the config's attributes.
    """
    config = Config()
    config.set_main_option("script_location", str(Path(interloper_db.__file__).parent / "migrations"))
    config.attributes["engine"] = engine
    script = ScriptDirectory.from_config(config)
    with EnvironmentContext(config, script):
        script.run_env()


class TestRunMigrationsOnline:
    """Migrations wait at most 10 s for a lock and run uncapped, without leaking either setting."""

    def test_the_session_settings_never_reach_the_pool(self, configured: dict[str, Any]) -> None:
        # A pooled connection left with them would serve requests with no statement timeout.
        engine = FakeEngine()

        run_env(engine)

        assert "SET statement_timeout = 0" in engine.log
        assert engine.pool == []

    def test_the_session_settings_never_reach_the_pool_when_a_migration_fails(
        self, configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def run_migrations(self: EnvironmentContext, **kwargs: Any) -> None:
            raise RuntimeError("migration broke")

        monkeypatch.setattr(EnvironmentContext, "run_migrations", run_migrations)
        engine = FakeEngine()

        with pytest.raises(RuntimeError, match="migration broke"):
            run_env(engine)

        assert engine.pool == []

    def test_the_settings_are_committed_before_alembic_takes_over(self, configured: dict[str, Any]) -> None:
        # Alembic declines to manage transactions on a connection already inside one.
        engine = FakeEngine()

        run_env(engine)

        assert engine.log == ["SET lock_timeout = '10s'", "SET statement_timeout = 0", "COMMIT", "configure", "migrate"]

    def test_each_migration_runs_in_its_own_transaction(self, configured: dict[str, Any]) -> None:
        run_env(FakeEngine())

        assert configured["transaction_per_migration"] is True
