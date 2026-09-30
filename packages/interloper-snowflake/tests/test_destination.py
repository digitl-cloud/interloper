"""Tests for ``interloper_snowflake.destination``."""

import datetime
from decimal import Decimal
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.destination import IOContext
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_snowflake import SnowflakeConnection, SnowflakeDestination
from interloper_snowflake import destination as destination_module


class _Nested(BaseModel):
    x: int | None = None


class _AllTypes(Schema):
    flag: bool | None = Field(...)
    n: int = Field(...)
    cost: float | None = Field(...)
    amount: Decimal | None = Field(...)
    at: datetime.datetime | None = Field(...)
    day: datetime.date | None = Field(...)
    blob: bytes | None = Field(...)
    name: str | None = Field(...)
    anything: Any = Field(None)
    nested: _Nested | None = Field(...)
    tags: list[str] = Field(...)


class _DaySchema(Schema):
    day: datetime.date | None = Field(...)
    cost: float | None = Field(...)


@il.asset(dataset="marts")
def ads_stats() -> list:
    return []


@il.asset
def undated() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day"))
def daily() -> list:
    return []


@il.asset(dataset="marts", partitioning=PartitionConfig(column="region"))
def regional() -> list:
    return []


@pytest.fixture
def loads(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def write_pandas(conn, frame, **kwargs):
        calls.append({"conn": conn, "frame": frame, **kwargs})
        return True, 1, len(frame), []

    monkeypatch.setattr(destination_module, "write_pandas", write_pandas)
    return calls


def _destination(**overrides: Any) -> SnowflakeDestination:
    connection = SnowflakeConnection(id="sf", account="acct", user="loader", password="<password>")
    return SnowflakeDestination(
        id="dest", connection=connection, database="ANALYTICS", warehouse="LOAD_WH", **overrides
    )


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


REF = '"ANALYTICS"."marts"'


class TestNaming:
    def test_asset_dataset_is_the_schema(self, session, loads):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert f'TRUNCATE TABLE {REF}."ads_stats"' not in session.sql
        assert f"CREATE SCHEMA IF NOT EXISTS {REF}" in session.sql
        assert loads[0]["database"] == "ANALYTICS"
        assert loads[0]["schema"] == "marts"
        assert loads[0]["table_name"] == "ads_stats"

    def test_default_dataset_fallback(self, session, loads):
        _destination(default_dataset="raw").write(_ctx(undated(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert 'CREATE SCHEMA IF NOT EXISTS "ANALYTICS"."raw"' in session.sql
        assert loads[0]["schema"] == "raw"

    def test_no_dataset_raises(self, session, loads):
        with pytest.raises(ConfigError, match="requires a dataset"):
            _destination().write(_ctx(undated()), [{"a": 1}])

    def test_quotes_are_doubled(self):
        assert destination_module._quote('we"ird') == '"we""ird"'

    def test_existence_probe_is_parameterised(self, session, loads):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        probe = next(s for s in session.statements if "information_schema" in s[0])
        assert probe == (
            'SELECT 1 FROM "ANALYTICS".information_schema.tables WHERE table_schema = %s AND table_name = %s',
            ("marts", "ads_stats"),
        )


class TestDDL:
    def test_every_type(self, session, loads):
        row = dict.fromkeys(_AllTypes.model_fields)
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == (
            f'CREATE TABLE IF NOT EXISTS {REF}."ads_stats" ('
            '"flag" BOOLEAN, "n" NUMBER(38,0), "cost" FLOAT, "amount" NUMBER(38,9), '
            '"at" TIMESTAMP_NTZ, "day" DATE, "blob" BINARY, "name" VARCHAR, "anything" VARCHAR, '
            '"nested" VARIANT, "tags" VARIANT)'
        )

    def test_infers_without_a_schema(self, session, loads):
        _destination().write(_ctx(ads_stats()), [{"id": 1, "label": "a"}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == f'CREATE TABLE IF NOT EXISTS {REF}."ads_stats" ("id" NUMBER(38,0), "label" VARCHAR)'

    def test_existing_table_is_not_created(self, session, loads):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert not any(s.startswith("CREATE") for s in session.sql)


class TestSession:
    def test_use_warehouse_once(self, session, loads):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 2.0}])

        assert session.sql.count('USE WAREHOUSE "LOAD_WH"') == 1
        assert session.sql[0] == 'USE WAREHOUSE "LOAD_WH"'

    def test_load_goes_through_write_pandas(self, session, loads):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        call = loads[0]
        assert call["conn"] is session
        assert call["quote_identifiers"] is True
        assert call["auto_create_table"] is False
        assert call["use_logical_type"] is True
        assert list(call["frame"].columns) == ["day", "cost"]


class TestWrite:
    def test_whole_table_truncates(self, session, loads):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert session.sql == ['USE WAREHOUSE "LOAD_WH"', "BEGIN", f'TRUNCATE TABLE {REF}."ads_stats"', "COMMIT"]
        assert len(loads) == 1

    def test_missing_table_deletes_nothing(self, session, loads):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert not any(s.startswith(("TRUNCATE", "DELETE")) for s in session.sql)

    def test_time_partition_deletes_by_bounds(self, session, loads):
        session.tables.add(("marts", "daily"))
        partition = TimePartition(datetime.date(2024, 1, 1))
        _destination().write(_ctx(daily(), partition, _DaySchema), [{"day": datetime.date(2024, 1, 1), "cost": 1.0}])

        delete = next(s for s in session.statements if s[0].startswith("DELETE"))
        assert delete == (
            f'DELETE FROM {REF}."daily" WHERE "day" >= %s AND "day" < %s',
            (datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),
        )

    def test_non_time_partition_deletes_by_equality(self, session, loads):
        session.tables.add(("marts", "regional"))
        _destination().write(_ctx(regional(), Partition("eu")), [{"region": "eu", "cost": 1.0}])

        delete = next(s for s in session.statements if s[0].startswith("DELETE"))
        assert delete == (f'DELETE FROM {REF}."regional" WHERE "region" = %s', ("eu",))

    def test_window_is_one_batch(self, session, loads):
        session.tables.add(("marts", "daily"))
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"day": datetime.date(2024, 1, d), "cost": 1.0} for d in (1, 2, 3)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        deletes = [s for s in session.sql if s.startswith("DELETE")]
        assert len(deletes) == 3
        assert session.sql.count("BEGIN") == 1
        assert session.sql[-1] == "COMMIT"
        assert len(loads) == 1
        assert len(loads[0]["frame"]) == 3

    def test_extra_columns_warn_and_drop(self, session, loads):
        session.tables.add(("marts", "ads_stats"))
        with pytest.warns(UserWarning, match=r"Columns \['extra'\] are not in the schema"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0, "extra": "x"}])

        assert list(loads[0]["frame"].columns) == ["day", "cost"]


class TestTransaction:
    def test_statements_share_the_transaction_cursor(self, session, loads):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        in_block = [
            cursor
            for (sql, _), cursor in zip(session.statements, session.cursors_used, strict=True)
            if sql != 'USE WAREHOUSE "LOAD_WH"'
        ]
        assert len({id(cursor) for cursor in in_block}) == 1
        assert in_block[0].closed

    def test_rollback_on_failure(self, session, monkeypatch):
        session.tables.add(("marts", "ads_stats"))

        def fail(*args, **kwargs):
            raise RuntimeError("copy failed")

        monkeypatch.setattr(destination_module, "write_pandas", fail)
        with pytest.raises(RuntimeError, match="copy failed"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": None, "cost": 1.0}])

        assert session.sql[-1] == "ROLLBACK"
        assert "COMMIT" not in session.sql

    def test_outside_a_transaction_each_statement_gets_a_fresh_cursor(self, session):
        session.tables.add(("marts", "ads_stats"))
        destination = _destination()
        destination.read(_ctx(ads_stats()))
        destination.read(_ctx(ads_stats()))

        selects = [
            c
            for (sql, _), c in zip(session.statements, session.cursors_used, strict=True)
            if sql.startswith("SELECT *")
        ]
        assert selects[0] is not selects[1]


class TestRead:
    def test_whole_table(self, session):
        session.tables.add(("marts", "ads_stats"))
        frame = pd.DataFrame([{"cost": 1.0}])
        session.respond("SELECT *", frame=frame)

        result = _destination().read(_ctx(ads_stats()))

        assert result is frame
        assert session.sql[-1] == f'SELECT * FROM {REF}."ads_stats"'

    def test_time_partition_by_bounds(self, session):
        session.tables.add(("marts", "daily"))
        _destination().read(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1))))

        assert session.statements[-1] == (
            f'SELECT * FROM {REF}."daily" WHERE "day" >= %s AND "day" < %s',
            (datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),
        )

    def test_missing_table_raises(self, session):
        with pytest.raises(DataNotFoundError, match=r"does not exist\. Has the asset been materialized\?"):
            _destination().read(_ctx(ads_stats()))


class TestCount:
    def test_groups_by_the_partition_column(self, session):
        session.tables.add(("marts", "daily"))
        session.respond("SELECT TO_VARCHAR", rows=[("2024-01-01", 3), ("2024-01-02", 5)])

        counts = _destination().partition_row_counts(_ctx(daily()))

        assert counts == {"2024-01-01": 3, "2024-01-02": 5}
        assert session.sql[-1] == (
            f'SELECT TO_VARCHAR("day") AS partition_value, COUNT(*) AS cnt FROM {REF}."daily" GROUP BY 1'
        )

    def test_missing_table_raises(self, session):
        with pytest.raises(DataNotFoundError):
            _destination().partition_row_counts(_ctx(daily()))
