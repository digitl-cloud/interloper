"""Tests for ``interloper_db.models.columns``.

The column primitives carry the Postgres/SQLite portability the whole
schema depends on, so the variance is asserted directly rather than
inferred from a table that happens to work.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import JSON, Column, DateTime, MetaData, Table, create_engine, insert, select
from sqlalchemy.dialects.postgresql import JSONB, psycopg
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect

from interloper_db.models.columns import PortableJSON, TZDateTime, UTCDateTime, timestamp_column


def test_datetimes_are_timezone_aware() -> None:
    """TIMESTAMPTZ, so SQLAlchemy hands back aware values rather than naive ones."""
    assert TZDateTime.timezone is True


class TestTZDateTime:
    """Postgres's timestamptz untouched; SQLite's timestamps kept in UTC."""

    def test_postgres_gets_the_plain_timestamptz(self) -> None:
        dialect = psycopg.dialect()
        plain = DateTime(timezone=True).dialect_impl(dialect)

        implementation = TZDateTime.dialect_impl(dialect)

        assert type(implementation) is type(plain)
        assert TZDateTime.compile(dialect=dialect) == "TIMESTAMP WITH TIME ZONE"

    def test_postgres_processes_no_value(self) -> None:
        dialect = psycopg.dialect()
        implementation = TZDateTime.dialect_impl(dialect)

        assert implementation.bind_processor(dialect) is None
        assert implementation.result_processor(dialect, None) is None

    def test_sqlite_gets_the_utc_type(self) -> None:
        assert isinstance(TZDateTime.dialect_impl(sqlite_dialect()), UTCDateTime)


class TestUTCDateTime:
    """What SQLite stores and hands back."""

    @pytest.fixture
    def table(self) -> Table:
        """A one-column table of the type.

        Returns:
            The table, not yet created.
        """
        return Table("stamps", MetaData(), Column("at", TZDateTime))

    def _round_trip(self, table: Table, value: datetime | None) -> tuple[datetime | None, str | None]:
        engine = create_engine("sqlite://")
        table.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(insert(table).values(at=value))
            stored = connection.exec_driver_sql("SELECT at FROM stamps").scalar()
            read = connection.execute(select(table.c.at)).scalar()
        return read, stored

    def test_an_aware_value_is_stored_in_utc_and_read_back_aware(self, table: Table) -> None:
        paris = datetime(2026, 11, 22, 11, 30, tzinfo=timezone(timedelta(hours=1)))

        read, stored = self._round_trip(table, paris)

        assert stored == "2026-11-22 10:30:00.000000"
        assert read == paris
        assert read is not None and read.tzinfo is UTC

    def test_a_naive_value_is_taken_to_be_utc(self, table: Table) -> None:
        read, stored = self._round_trip(table, datetime(2026, 11, 22, 10, 30))

        assert stored == "2026-11-22 10:30:00.000000"
        assert read == datetime(2026, 11, 22, 10, 30, tzinfo=UTC)

    def test_none_stays_none(self, table: Table) -> None:
        assert self._round_trip(table, None) == (None, None)


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
