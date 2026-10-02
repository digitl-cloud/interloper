"""Database engine singleton."""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import make_url

_engine: Engine | None = None


def init_engine(dsn: str | None = None, statement_timeout: float | None = None, **kwargs: Any) -> Engine:
    """Initialize the global database engine.

    Args:
        dsn: PostgreSQL connection string. Falls back to ``DATABASE_URL`` env var.
        statement_timeout: Seconds after which the server cancels a running statement. Sent as the libpq
            ``statement_timeout`` connection option, merged into any ``connect_args`` passed. ``None`` leaves the
            server default; other backends (SQLite) ignore it.
        **kwargs: Additional kwargs forwarded to ``create_engine``.

    Returns:
        The SQLAlchemy engine.

    Raises:
        ConfigError: If no DSN is provided and ``DATABASE_URL`` is not set.
    """
    global _engine
    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        from interloper.errors import ConfigError

        raise ConfigError("Database DSN required: pass dsn= or set DATABASE_URL")
    if statement_timeout is not None and make_url(dsn).get_backend_name() == "postgresql":
        connect_args = dict(kwargs.pop("connect_args", None) or {})
        option = f"-c statement_timeout={max(1, round(statement_timeout * 1000))}"
        connect_args["options"] = f"{connect_args['options']} {option}" if connect_args.get("options") else option
        kwargs["connect_args"] = connect_args
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

    postgres = AppSettings.get().postgres
    return init_engine(postgres.dsn, statement_timeout=postgres.statement_timeout)
