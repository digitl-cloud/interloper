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


def _destination(**overrides: Any) -> SnowflakeDestination:
    connection = SnowflakeConnection(id="sf", account="acct", user="loader", password="<password>")
    return SnowflakeDestination(
        id="dest", connection=connection, database="ANALYTICS", warehouse="LOAD_WH", **overrides
    )


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


REF = '"ANALYTICS"."marts"'


STAGE = f'{REF}."interloper_load"'
DAY = {"day": None, "cost": 1.0}


def _copy(table: str, columns: str, projection: str, location: str) -> str:
    return (
        f'COPY INTO {REF}."{table}" ({columns}) FROM (SELECT {projection} FROM {location}) '
        "FILE_FORMAT=(TYPE=PARQUET USE_LOGICAL_TYPE=TRUE BINARY_AS_TEXT=FALSE) PURGE=TRUE"
    )


def _location(session) -> str:
    put = next(sql for sql in session.sql if sql.startswith("PUT "))
    location = put.split(" ")[2]
    assert location.startswith(f"@{STAGE}/")
    return location


class TestNaming:
    def test_asset_dataset_is_the_schema(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert f"CREATE SCHEMA IF NOT EXISTS {REF}" in session.sql
        assert any(sql.startswith(f'COPY INTO {REF}."ads_stats"') for sql in session.sql)

    def test_default_dataset_fallback(self, session):
        _destination(default_dataset="raw").write(_ctx(undated(), schema=_DaySchema), [DAY])

        assert 'CREATE SCHEMA IF NOT EXISTS "ANALYTICS"."raw"' in session.sql
        assert 'CREATE TEMPORARY STAGE IF NOT EXISTS "ANALYTICS"."raw"."interloper_load"' in session.sql

    def test_no_dataset_raises(self, session):
        with pytest.raises(ConfigError, match="requires a dataset"):
            _destination().write(_ctx(undated()), [{"a": 1}])

    def test_quotes_are_doubled(self):
        assert destination_module._quote('we"ird') == '"we""ird"'

    def test_existence_probe_is_parameterised(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        probe = next(s for s in session.statements if "information_schema" in s[0])
        assert probe == (
            'SELECT 1 FROM "ANALYTICS".information_schema.tables WHERE table_schema = %s AND table_name = %s',
            ("marts", "ads_stats"),
        )


class TestDDL:
    def test_every_type(self, session):
        row = dict.fromkeys(_AllTypes.model_fields)
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == (
            f'CREATE TABLE IF NOT EXISTS {REF}."ads_stats" ('
            '"flag" BOOLEAN, "n" NUMBER(38,0), "cost" FLOAT, "amount" NUMBER(38,9), '
            '"at" TIMESTAMP_NTZ, "day" DATE, "blob" BINARY, "name" VARCHAR, "anything" VARCHAR, '
            '"nested" VARIANT, "tags" VARIANT)'
        )

    def test_infers_without_a_schema(self, session):
        _destination().write(_ctx(ads_stats()), [{"id": 1, "label": "a"}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == f'CREATE TABLE IF NOT EXISTS {REF}."ads_stats" ("id" NUMBER(38,0), "label" VARCHAR)'

    def test_existing_table_is_not_created(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert not any(s.startswith(("CREATE SCHEMA", "CREATE TABLE")) for s in session.sql)


class TestSession:
    def test_destination_opens_its_own_session(self, session):
        _ = _destination().client

        assert session.connect_kwargs == {
            "account": "acct",
            "user": "loader",
            "password": "<password>",
            "role": None,
            "autocommit": True,
            "warehouse": "LOAD_WH",
            "database": "ANALYTICS",
        }

    def test_destinations_sharing_a_connection_get_separate_sessions(self, session):
        connection = SnowflakeConnection(id="sf", account="acct", user="loader", password="<password>")
        first = SnowflakeDestination(id="a", connection=connection, database="ANALYTICS", warehouse="LOAD_WH")
        second = SnowflakeDestination(id="b", connection=connection, database="ANALYTICS", warehouse="ADHOC_WH")
        _ = first.client, second.client, first.client

        assert [call["warehouse"] for call in session.connect_calls] == ["LOAD_WH", "ADHOC_WH"]
        assert not any(sql.startswith("USE ") for sql in session.sql)

    def test_stage_is_created_once_per_schema(self, session):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert session.sql.count(f"CREATE TEMPORARY STAGE IF NOT EXISTS {STAGE}") == 1


class TestStaging:
    def test_put_uploads_the_aligned_frame(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"cost": 1.5, "day": datetime.date(2024, 1, 1)}])

        put = next(sql for sql in session.sql if sql.startswith("PUT "))
        assert put.startswith("PUT 'file://")
        assert put.endswith(" OVERWRITE=TRUE AUTO_COMPRESS=FALSE")
        assert list(session.uploads[0].columns) == ["day", "cost"]
        assert session.uploads[0].to_dict("records") == [{"day": datetime.date(2024, 1, 1), "cost": 1.5}]

    def test_every_type_survives_the_parquet_file(self, session):
        row = {
            "flag": True,
            "n": 1,
            "cost": 1.5,
            "amount": Decimal("1.25"),
            "at": datetime.datetime(2024, 1, 1, 12),
            "day": datetime.date(2024, 1, 1),
            "blob": b"\x00\x01",
            "name": "a",
            "anything": "x",
            "nested": {"x": 1},
            "tags": ["a", "b"],
        }
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        uploaded = session.uploads[0].to_dict("records")[0]
        assert list(session.uploads[0].columns) == list(_AllTypes.model_fields)
        assert uploaded["amount"] == Decimal("1.25")
        assert uploaded["blob"] == b"\x00\x01"
        assert uploaded["nested"] == {"x": 1}
        assert list(uploaded["tags"]) == ["a", "b"]

    def test_copy_projects_columns_by_name(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        location = _location(session)
        copy = next(sql for sql in session.sql if sql.startswith("COPY INTO"))
        assert copy == _copy("ads_stats", '"day", "cost"', '$1:"day", $1:"cost"', location)

    def test_each_write_gets_its_own_prefix(self, session):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        puts = [sql.split(" ")[2] for sql in session.sql if sql.startswith("PUT ")]
        assert len(set(puts)) == 2

    def test_extra_columns_warn_and_drop(self, session):
        session.tables.add(("marts", "ads_stats"))
        with pytest.warns(UserWarning, match=r"Columns \['extra'\] are not in the schema"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{**DAY, "extra": "x"}])

        assert list(session.uploads[0].columns) == ["day", "cost"]
        assert '"extra"' not in next(sql for sql in session.sql if sql.startswith("COPY INTO"))


class TestWrite:
    def test_new_table_runs_every_ddl_before_begin(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        location = _location(session)
        assert session.sql == [
            f"CREATE SCHEMA IF NOT EXISTS {REF}",
            f'CREATE TABLE IF NOT EXISTS {REF}."ads_stats" ("day" DATE, "cost" FLOAT)',
            f"CREATE TEMPORARY STAGE IF NOT EXISTS {STAGE}",
            session.sql[3],
            "BEGIN",
            _copy("ads_stats", '"day", "cost"', '$1:"day", $1:"cost"', location),
            "COMMIT",
        ]
        assert session.sql[3].startswith("PUT ")

    def test_whole_table_truncates_inside_the_transaction(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        location = _location(session)
        assert session.sql == [
            f"CREATE TEMPORARY STAGE IF NOT EXISTS {STAGE}",
            session.sql[1],
            "BEGIN",
            f'TRUNCATE TABLE {REF}."ads_stats"',
            _copy("ads_stats", '"day", "cost"', '$1:"day", $1:"cost"', location),
            "COMMIT",
        ]

    def test_time_partition_deletes_by_bounds(self, session):
        session.tables.add(("marts", "daily"))
        partition = TimePartition(datetime.date(2024, 1, 1))
        _destination().write(_ctx(daily(), partition, _DaySchema), [{"day": datetime.date(2024, 1, 1), "cost": 1.0}])

        assert [sql.split(" ")[0] for sql in session.sql] == ["CREATE", "PUT", "BEGIN", "DELETE", "COPY", "COMMIT"]
        delete = next(s for s in session.statements if s[0].startswith("DELETE"))
        assert delete == (
            f'DELETE FROM {REF}."daily" WHERE "day" >= %s AND "day" < %s',
            (datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)),
        )

    def test_non_time_partition_deletes_by_equality(self, session):
        session.tables.add(("marts", "regional"))
        _destination().write(_ctx(regional(), Partition("eu")), [{"region": "eu", "cost": 1.0}])

        assert [sql.split(" ")[0] for sql in session.sql] == ["CREATE", "PUT", "BEGIN", "DELETE", "COPY", "COMMIT"]
        delete = next(s for s in session.statements if s[0].startswith("DELETE"))
        assert delete == (f'DELETE FROM {REF}."regional" WHERE "region" = %s', ("eu",))

    def test_window_is_one_batch(self, session):
        session.tables.add(("marts", "daily"))
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"day": datetime.date(2024, 1, d), "cost": 1.0} for d in (1, 2, 3)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        assert [sql.split(" ")[0] for sql in session.sql] == [
            "CREATE",
            "PUT",
            "BEGIN",
            "DELETE",
            "DELETE",
            "DELETE",
            "COPY",
            "COMMIT",
        ]
        assert len(session.uploads) == 1
        assert len(session.uploads[0]) == 3

    def test_empty_data_stages_nothing(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [])

        assert session.statements == []

    def test_insert_outside_write_stages_itself(self, session):
        session.tables.add(("marts", "ads_stats"))
        destination = _destination()
        destination.insert("ads_stats", "marts", [DAY], _ctx(ads_stats(), schema=_DaySchema))

        assert [sql.split(" ")[0] for sql in session.sql] == ["CREATE", "PUT", "COPY"]


class TestTransaction:
    def test_statements_share_the_transaction_cursor(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        pairs = list(zip(session.statements, session.cursors_used, strict=True))
        begin = next(i for i, ((sql, _), _) in enumerate(pairs) if sql == "BEGIN")
        in_block = [cursor for _, cursor in pairs[begin:]]
        assert len({id(cursor) for cursor in in_block}) == 1
        assert in_block[0].closed

    def test_failed_copy_rolls_back(self, session):
        session.tables.add(("marts", "daily"))
        session.failures["COPY INTO"] = RuntimeError("copy failed")
        partition = TimePartition(datetime.date(2024, 1, 1))
        with pytest.raises(RuntimeError, match="copy failed"):
            _destination().write(
                _ctx(daily(), partition, _DaySchema), [{"day": datetime.date(2024, 1, 1), "cost": 1.0}]
            )

        assert [sql.split(" ")[0] for sql in session.sql] == ["CREATE", "PUT", "BEGIN", "DELETE", "COPY", "ROLLBACK"]
        assert "COMMIT" not in session.sql

    def test_failed_put_never_opens_a_transaction(self, session):
        session.tables.add(("marts", "daily"))
        session.failures["PUT "] = RuntimeError("upload failed")
        partition = TimePartition(datetime.date(2024, 1, 1))
        with pytest.raises(RuntimeError, match="upload failed"):
            _destination().write(
                _ctx(daily(), partition, _DaySchema), [{"day": datetime.date(2024, 1, 1), "cost": 1.0}]
            )

        assert "BEGIN" not in session.sql
        assert not any(sql.startswith("DELETE") for sql in session.sql)

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
