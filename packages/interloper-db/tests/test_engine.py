"""Tests for ``interloper_db.engine``.

The engine is a process-wide singleton, so every test restores whatever it
found — otherwise an engine installed here leaks into the suites that run
later.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from interloper.errors import ConfigError
from sqlalchemy import Engine, create_engine

from interloper_db import engine as engine_module
from interloper_db.engine import engine_from_settings, get_engine, init_engine


@pytest.fixture(autouse=True)
def restore_engine() -> Iterator[None]:
    """Put the module-level engine back the way the test found it.

    Yields:
        ``None``; the teardown restores the saved engine.
    """
    saved = engine_module._engine
    engine_module._engine = None
    yield
    engine_module._engine = saved


@pytest.fixture
def created_kwargs(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Capture the keyword arguments ``init_engine`` forwards to ``create_engine``.

    Args:
        monkeypatch: Pytest's monkeypatch fixture.

    Returns:
        A dict filled with the kwargs when ``init_engine`` runs; no engine is actually created, so no driver is needed.
    """
    captured: dict[str, object] = {}

    def fake_create_engine(dsn: str, **kwargs: object) -> Engine:
        captured.update(kwargs)
        return create_engine("sqlite://")

    monkeypatch.setattr(engine_module, "create_engine", fake_create_engine)
    return captured


class TestInitEngine:
    """Explicit DSN, environment fallback, and the actionable failure."""

    def test_an_explicit_dsn_builds_the_engine(self) -> None:
        engine = init_engine("sqlite://")

        assert isinstance(engine, Engine)
        assert engine.url.drivername == "sqlite"

    def test_the_engine_becomes_the_process_singleton(self) -> None:
        engine = init_engine("sqlite://")

        assert get_engine() is engine

    def test_it_falls_back_to_the_database_url_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "sqlite://")

        assert init_engine().url.drivername == "sqlite"

    def test_no_dsn_anywhere_is_an_actionable_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(ConfigError, match="Database DSN required: pass dsn= or set DATABASE_URL"):
            init_engine()

    def test_extra_kwargs_reach_create_engine(self) -> None:
        engine = init_engine("sqlite://", echo=True)

        assert engine.echo is True

    def test_a_postgres_engine_gets_the_timeout_in_milliseconds_as_a_connection_option(
        self, created_kwargs: dict[str, object]
    ) -> None:
        init_engine("postgresql+psycopg://u:p@localhost/db", statement_timeout=2.5)

        assert created_kwargs["connect_args"] == {"options": "-c statement_timeout=2500"}

    def test_a_sub_millisecond_timeout_still_caps_statements(self, created_kwargs: dict[str, object]) -> None:
        init_engine("postgresql+psycopg://u:p@localhost/db", statement_timeout=0.0001)

        assert created_kwargs["connect_args"] == {"options": "-c statement_timeout=1"}

    def test_the_timeout_is_merged_into_existing_connect_args(self, created_kwargs: dict[str, object]) -> None:
        init_engine(
            "postgresql+psycopg://u:p@localhost/db",
            statement_timeout=30,
            connect_args={"application_name": "api", "options": "-c search_path=public"},
        )

        assert created_kwargs["connect_args"] == {
            "application_name": "api",
            "options": "-c search_path=public -c statement_timeout=30000",
        }

    def test_no_timeout_leaves_postgres_connect_args_untouched(self, created_kwargs: dict[str, object]) -> None:
        init_engine("postgresql+psycopg://u:p@localhost/db", connect_args={"application_name": "api"})

        assert created_kwargs["connect_args"] == {"application_name": "api"}

    def test_sqlite_ignores_the_timeout(self, created_kwargs: dict[str, object]) -> None:
        init_engine("sqlite://", statement_timeout=5)

        assert "connect_args" not in created_kwargs

    def test_re_initializing_replaces_the_singleton(self) -> None:
        first = init_engine("sqlite://")
        second = init_engine("sqlite://")

        assert second is not first
        assert get_engine() is second


class TestGetEngine:
    """Reading the singleton before it exists is a programming error."""

    def test_before_initialization_it_is_an_actionable_error(self) -> None:
        with pytest.raises(RuntimeError, match=r"Database engine not initialized. Call init_engine\(\) first."):
            get_engine()


class TestEngineFromSettings:
    """The lazy accessor the store layer uses when nothing installed an engine."""

    def test_an_existing_engine_is_reused_without_reading_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        engine = init_engine("sqlite://")
        monkeypatch.setattr(
            "interloper.settings.AppSettings.get",
            classmethod(lambda cls: pytest.fail("settings must not be read when an engine exists")),
        )

        assert engine_from_settings() is engine

    def test_without_an_engine_it_initializes_from_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from types import SimpleNamespace

        monkeypatch.setattr(
            "interloper.settings.AppSettings.get",
            classmethod(lambda cls: SimpleNamespace(postgres=SimpleNamespace(dsn="sqlite://", statement_timeout=None))),
        )

        engine = engine_from_settings()

        assert engine.url.drivername == "sqlite"
        assert get_engine() is engine

    def test_the_configured_statement_timeout_reaches_the_engine(
        self, monkeypatch: pytest.MonkeyPatch, created_kwargs: dict[str, object]
    ) -> None:
        from types import SimpleNamespace

        postgres = SimpleNamespace(dsn="postgresql+psycopg://u:p@localhost/db", statement_timeout=15)
        monkeypatch.setattr(
            "interloper.settings.AppSettings.get", classmethod(lambda cls: SimpleNamespace(postgres=postgres))
        )

        engine_from_settings()

        assert created_kwargs["connect_args"] == {"options": "-c statement_timeout=15000"}
