"""Database engine singleton."""

from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine

_engine: Engine | None = None


def init_engine(dsn: str | None = None, **kwargs: object) -> Engine:
    """Initialize the global database engine.

    Args:
        dsn: PostgreSQL connection string. Falls back to ``DATABASE_URL`` env var.
        **kwargs: Additional kwargs forwarded to ``create_engine``.

    Returns:
        The SQLAlchemy engine.

    Raises:
        ValueError: If no DSN is provided and ``DATABASE_URL`` is not set.
    """
    global _engine  # noqa: PLW0603
    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        from interloper.errors import ConfigError

        raise ConfigError("Database DSN required: pass dsn= or set DATABASE_URL")
    _engine = create_engine(dsn, **kwargs)
    return _engine


def get_engine() -> Engine:
    """Return the global database engine.

    Returns:
        The SQLAlchemy engine.

    Raises:
        RuntimeError: If ``init_engine`` has not been called.
    """
    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call init_engine() first.")
    return _engine


def engine_from_settings() -> Engine:
    """Return the process engine, initializing it from settings when absent.

    Returns:
        The SQLAlchemy engine.
    """
    if _engine is not None:
        return _engine
    from interloper.settings import AppSettings

    return init_engine(AppSettings.get().postgres.dsn)
