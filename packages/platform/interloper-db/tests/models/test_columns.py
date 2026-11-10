"""Tests for ``interloper_db.models.columns``.

The column primitives carry the Postgres/SQLite portability the whole
schema depends on, so the variance is asserted directly rather than
inferred from a table that happens to work.
"""

from __future__ import annotations

from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect

from interloper_db.models.columns import PortableJSON, TZDateTime, timestamp_column


def test_datetimes_are_timezone_aware() -> None:
    """TIMESTAMPTZ, so SQLAlchemy hands back aware values rather than naive ones."""
    assert TZDateTime.timezone is True


def test_json_is_jsonb_on_postgres() -> None:
    """JSONB where it exists, for indexing and containment operators."""
    assert isinstance(PortableJSON.dialect_impl(postgresql_dialect()), JSONB)


def test_json_falls_back_to_plain_json_elsewhere() -> None:
    """The in-memory SQLite test databases get plain JSON."""
    assert not isinstance(PortableJSON.dialect_impl(sqlite_dialect()), JSONB)
    assert isinstance(PortableJSON, JSON)


class TestTimestampColumn:
    """The nullable, insert-stamped timestamp every table reuses."""

    def test_it_defaults_to_the_insert_time(self) -> None:
        # CURRENT_TIMESTAMP rather than now(), so SQLite works identically.
        column = timestamp_column().sa_column

        assert column.server_default is not None
        assert "CURRENT_TIMESTAMP" in str(column.server_default.arg)

    def test_it_is_a_timezone_aware_datetime(self) -> None:
        column = timestamp_column().sa_column

        assert column.type.timezone is True

    def test_extra_kwargs_reach_the_column(self) -> None:
        column = timestamp_column(index=True).sa_column

        assert column.index is True
