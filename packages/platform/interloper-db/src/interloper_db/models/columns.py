"""Primitives shared by every table: column types, column factories, and the table a model maps to."""

from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import JSON, DateTime, Dialect, Table, TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, SQLModel, text
from sqlmodel import Field as SQLField


class UTCDateTime(TypeDecorator[datetime]):
    """A timestamp on a database with no time zones (SQLite): stored as UTC, read back aware.

    SQLite keeps a datetime's wall-clock digits and drops its offset, so an
    aware value is converted to UTC on the way in, and every value read is
    marked UTC, as Postgres's ``timestamptz`` hands it back.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Convert an aware value to naive UTC; a naive one is taken to be UTC already.

        Args:
            value: The value being written, or ``None``.
            dialect: The dialect writing it.

        Returns:
            The naive UTC value, or ``None``.
        """
        if value is None or value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Mark a value read back as UTC.

        Args:
            value: The naive value the database holds, or ``None``.
            dialect: The dialect reading it.

        Returns:
            The aware UTC value, or ``None``.
        """
        return None if value is None else value.replace(tzinfo=UTC)


# TIMESTAMPTZ on Postgres, which hands back aware values; UTC kept by the type on SQLite.
TZDateTime = DateTime(timezone=True).with_variant(UTCDateTime(), "sqlite")

# JSONB on Postgres; plain JSON elsewhere (SQLite).
PortableJSON = JSON().with_variant(JSONB(), "postgresql")


def timestamp_column(**kwargs: Any) -> Any:
    """Build a nullable TIMESTAMPTZ column defaulting to the insert time.

    ``CURRENT_TIMESTAMP`` rather than ``now()`` so the tables also work on
    SQLite (identical semantics on Postgres).

    Args:
        **kwargs: Extra keyword arguments forwarded to the SQLAlchemy
            ``Column`` (``index``, ``nullable``, …).

    Returns:
        A SQLModel field descriptor for the column.
    """
    return SQLField(default=None, sa_column=Column(TZDateTime, server_default=text("CURRENT_TIMESTAMP"), **kwargs))


def table_of(model: type[SQLModel]) -> Table:
    """The table a ``table=True`` model maps to.

    SQLAlchemy sets ``__table__`` on the class without declaring it, so the
    type checker cannot see it; the shared metadata holds the same table
    under the model's name.

    Args:
        model: The model class.

    Returns:
        The model's table.
    """
    # Typed str-or-callable for the declaration; read off the class, SQLAlchemy has already resolved it.
    return SQLModel.metadata.tables[cast("str", model.__tablename__)]
