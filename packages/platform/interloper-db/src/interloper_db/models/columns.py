"""Primitives shared by every table: column types, column factories, and the table a model maps to."""

from typing import Any, cast

from sqlalchemy import JSON, DateTime, Table
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, SQLModel, text
from sqlmodel import Field as SQLField

# All datetime columns use TIMESTAMPTZ so SQLAlchemy returns timezone-aware values.
TZDateTime = DateTime(timezone=True)

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
