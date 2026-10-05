"""Column primitives shared by every table."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Dialect, TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, text
from sqlmodel import Field as SQLField


class TZDateTime(TypeDecorator[datetime]):
    """A TIMESTAMPTZ column whose values read back aware on every dialect.

    Postgres returns aware values on its own; SQLite, which the tests run
    on, stores no offset and hands the value back naive. Every stored
    instant is UTC, so a naive value is read as UTC here rather than at
    every call site.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Read a stored instant as aware UTC.

        Args:
            value: The driver's value, naive on SQLite.
            dialect: The dialect the value came from.

        Returns:
            The aware instant, or ``None`` for a null column.
        """
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


# JSONB on Postgres; plain JSON elsewhere (in-memory SQLite test databases).
PortableJSON = JSON().with_variant(JSONB(), "postgresql")


def timestamp_column(**kwargs: Any) -> Any:
    """Build a nullable TIMESTAMPTZ column defaulting to the insert time.

    ``CURRENT_TIMESTAMP`` rather than ``now()`` so the tables also work on the
    in-memory SQLite databases the tests use (identical semantics on Postgres).

    Args:
        **kwargs: Extra keyword arguments forwarded to the SQLAlchemy
            ``Column`` (``index``, ``nullable``, …).

    Returns:
        A SQLModel field descriptor for the column.
    """
    return SQLField(default=None, sa_column=Column(TZDateTime, server_default=text("CURRENT_TIMESTAMP"), **kwargs))
