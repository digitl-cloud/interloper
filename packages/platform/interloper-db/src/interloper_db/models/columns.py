"""Column primitives shared by every table."""

from typing import Any

from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, text
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
