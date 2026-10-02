"""Tests for ``interloper_databricks.destination``."""

import copy
import datetime
import json
import threading
import time
from decimal import Decimal
from typing import Any

import interloper as il
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from interloper.destination import IOContext
from interloper.destination.database import PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import BaseModel, Field, ValidationError

from interloper_databricks import DatabricksConnection, DatabricksDestination
from interloper_databricks import destination as destination_module


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


class _Described(Schema):
    day: datetime.date | None = Field(..., description="Report day")
    cost: float | None = Field(..., description="Spend, in the account's currency")


class _Flagged(Schema):
    active: bool | None = Field(...)


@il.asset(dataset="marts")
def ads_stats() -> list:
    return []


@il.asset
def undated() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day"))
def daily() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day", granularity=TimeGranularity.MONTH))
def monthly() -> list:
    return []


@il.asset(dataset="marts", partitioning=PartitionConfig(column="region"))
def regional() -> list:
    return []


@il.asset(dataset="marts", partitioning=PartitionConfig(column="active"))
def by_flag() -> list:
    return []


@il.asset(dataset="Marts")
def described() -> list:
    """Ad spend per day.

    It's the reference table.
    """  # noqa: DOC201
    return []


VOLUME = "/Volumes/main/staging/loads"
REF = "`main`.`marts`"
DAY = {"day": None, "cost": 1.0}
JAN = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]


def _destination(**overrides: Any) -> DatabricksDestination:
    connection = DatabricksConnection(id="dbx", host="dbc-1.cloud.databricks.com", access_token="<pat>")
    fields = {"warehouse": "/sql/1.0/warehouses/abc", "catalog": "main", "staging_volume": "main.staging.loads"}
    return DatabricksDestination(id="dest", connection=connection, **{**fields, **overrides})


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _staged(session) -> str:
    put = next(sql for sql in session.sql if sql.startswith("PUT "))
    path = put.split("'")[3]
    assert path.startswith(f"{VOLUME}/interloper/")
    assert path.endswith(".parquet")
    return path


def _read(path: str) -> str:
    return f"read_files('{path}', format => 'parquet')"


def _insert(session) -> str:
    return next(sql for sql in session.sql if sql.startswith("INSERT "))


class TestNaming:
    def test_asset_dataset_is_the_schema(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert f"CREATE SCHEMA IF NOT EXISTS {REF}" in session.sql
        assert _insert(session).startswith(f"INSERT OVERWRITE {REF}.`ads_stats` BY NAME ")

    def test_default_dataset_fallback(self, session):
        _destination(default_dataset="raw").write(_ctx(undated(), schema=_DaySchema), [DAY])

        assert "CREATE SCHEMA IF NOT EXISTS `main`.`raw`" in session.sql
        assert _insert(session).startswith("INSERT OVERWRITE `main`.`raw`.`undated` ")

    def test_no_dataset_raises(self, session):
        with pytest.raises(ConfigError, match="requires a dataset"):
            _destination().write(_ctx(undated()), [{"a": 1}])

    def test_backticks_are_doubled(self):
        assert destination_module._quote("we`ird") == "`we``ird`"

    def test_existence_probe_is_lower_case(self, session):
        _destination().write(_ctx(described(), schema=_DaySchema), [DAY])

        probe = next(sql for sql, _ in session.statements if "information_schema" in sql)
        assert probe == (
            "SELECT 1 FROM `main`.information_schema.tables "
            "WHERE table_schema = 'marts' AND table_name = 'described'"
        )

    def test_existing_mixed_case_table_is_found(self, session):
        session.tables.add(("marts", "described"))
        _destination().write(_ctx(described(), schema=_DaySchema), [DAY])

        assert not any(sql.startswith("CREATE") for sql in session.sql)


class TestConfig:
    @pytest.mark.parametrize("volume", ["loads", "staging.loads", "main..loads", "a.b.c.d"])
    def test_staging_volume_needs_three_parts(self, volume):
        with pytest.raises(ValidationError, match=r"catalog\.schema\.volume"):
            _destination(staging_volume=volume)

    def test_volume_path(self):
        assert _destination().volume_path == VOLUME

    def test_deep_copy_gets_a_fresh_lock(self):
        destination = _destination()
        clone = copy.deepcopy(destination)

        assert clone._lock is not destination._lock
        assert destination.model_copy(deep=True).catalog == "main"


class TestSession:
    def test_destination_opens_its_own_session(self, session):
        _ = _destination().client

        assert session.connect_kwargs == {
            "server_hostname": "dbc-1.cloud.databricks.com",
            "access_token": "<pat>",
            "http_path": "/sql/1.0/warehouses/abc",
            "catalog": "main",
        }

    def test_destinations_sharing_a_connection_get_separate_sessions(self, session):
        connection = DatabricksConnection(id="dbx", host="dbc-1.cloud.databricks.com", access_token="<pat>")
        fields = {"catalog": "main", "staging_volume": "main.staging.loads"}
        first = DatabricksDestination(id="a", connection=connection, warehouse="/sql/1.0/warehouses/a", **fields)
        second = DatabricksDestination(id="b", connection=connection, warehouse="/sql/1.0/warehouses/b", **fields)
        _ = first.client, second.client, first.client

        assert [call["http_path"] for call in session.connect_calls] == [
            "/sql/1.0/warehouses/a",
            "/sql/1.0/warehouses/b",
        ]

    def test_every_statement_gets_a_closed_cursor(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert len(session.cursors) == len(session.statements)
        assert all(cursor.closed for cursor in session.cursors)

    def test_concurrent_writes_never_interleave(self, session):
        destination = _destination()
        session.tables.update({("marts", "ads_stats"), ("marts", "daily")})

        def slow_put(sql: str) -> None:
            if sql.startswith("PUT "):
                time.sleep(0.05)

        session.on_execute = slow_put
        writes = [
            threading.Thread(target=destination.write, args=(_ctx(ads_stats(), schema=_DaySchema), [DAY])),
            threading.Thread(target=destination.write, args=(_ctx(daily(), schema=_DaySchema), [DAY])),
        ]
        for thread in writes:
            thread.start()
        for thread in writes:
            thread.join()

        kinds = [sql.split(" ")[0] for sql in session.sql]
        assert kinds == ["PUT", "INSERT", "REMOVE", "PUT", "INSERT", "REMOVE"]


class TestDDL:
    def test_every_type(self, session):
        row = dict.fromkeys(_AllTypes.model_fields)
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == (
            f"CREATE TABLE IF NOT EXISTS {REF}.`ads_stats` ("
            "`flag` BOOLEAN, `n` BIGINT, `cost` DOUBLE, `amount` DECIMAL(38,9), "
            "`at` TIMESTAMP, `day` DATE, `blob` BINARY, `name` STRING, `anything` STRING, "
            "`nested` VARIANT, `tags` VARIANT) USING DELTA"
        )

    def test_infers_without_a_schema(self, session):
        _destination().write(_ctx(ads_stats()), [{"id": 1, "label": "a"}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == f"CREATE TABLE IF NOT EXISTS {REF}.`ads_stats` (`id` BIGINT, `label` STRING) USING DELTA"

    def test_existing_table_is_not_created(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert not any(s.startswith(("CREATE SCHEMA", "CREATE TABLE")) for s in session.sql)

    def test_partitioned_table_clusters_on_its_partition_column(self, session):
        _destination().write(_ctx(daily(), TimePartition(JAN[0]), _DaySchema), [{"day": JAN[0], "cost": 1.0}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == (
            f"CREATE TABLE IF NOT EXISTS {REF}.`daily` (`day` DATE, `cost` DOUBLE) USING DELTA CLUSTER BY (`day`)"
        )
        assert "PARTITIONED BY" not in create

    def test_unclusterable_partition_column_is_not_a_key(self, session):
        _destination().write(_ctx(by_flag(), Partition("true"), _Flagged), [{"active": True}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert "CLUSTER BY" not in create

    def test_partition_column_past_the_statistics_columns_is_not_a_key(self, session):
        fields = {f"c{i:02d}": (str | None, Field(None)) for i in range(32)}
        wide = type("Wide", (Schema,), {"__annotations__": {k: v[0] for k, v in fields.items()} | {"region": str}})
        _destination().write(_ctx(regional(), Partition("eu"), wide), [{"region": "eu"}])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create.count("STRING") == 33
        assert "CLUSTER BY" not in create

    def test_descriptions_become_comments(self, session):
        _destination().write(_ctx(described(), schema=_Described), [DAY])

        create = next(s for s in session.sql if s.startswith("CREATE TABLE"))
        assert create == (
            "CREATE TABLE IF NOT EXISTS `main`.`Marts`.`described` ("
            "`day` DATE COMMENT 'Report day', `cost` DOUBLE COMMENT 'Spend, in the account\\'s currency') "
            "USING DELTA COMMENT 'Ad spend per day.\n\nIt\\'s the reference table.'"
        )


class TestStaging:
    def test_put_streams_the_aligned_frame(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"cost": 1.5, "day": JAN[0]}])

        put = next(sql for sql in session.sql if sql.startswith("PUT "))
        assert put == f"PUT '__input_stream__' INTO '{_staged(session)}' OVERWRITE"
        assert list(session.uploads[0].columns) == ["day", "cost"]
        assert session.uploads[0].to_dict("records") == [{"day": JAN[0], "cost": 1.5}]

    def test_every_type_survives_the_parquet_file(self, session):
        row = {
            "flag": True,
            "n": 1,
            "cost": 1.5,
            "amount": Decimal("1.25"),
            "at": datetime.datetime(2024, 1, 1, 12),
            "day": JAN[0],
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
        assert json.loads(uploaded["nested"]) == {"x": 1}
        assert json.loads(uploaded["tags"]) == ["a", "b"]

    def test_naive_datetimes_are_written_as_utc_instants(self, session):
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [{"n": 1, "at": datetime.datetime(2024, 1, 1, 12)}])

        schema = pq.read_schema(pa.BufferReader(session.raw_uploads[0]))
        assert schema.field("at").type == pa.timestamp("us", tz="UTC")
        assert session.uploads[0]["at"][0] == pd.Timestamp("2024-01-01T12:00:00Z")

    def test_aware_datetimes_keep_their_instant(self, session):
        at = datetime.datetime(2024, 1, 1, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [{"n": 1, "at": at}])

        assert session.uploads[0]["at"][0] == pd.Timestamp("2024-01-01T10:00:00Z")

    def test_missing_nested_values_stay_null(self, session):
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [{"n": 1, "nested": None, "tags": None}])

        uploaded = session.uploads[0].to_dict("records")[0]
        assert uploaded["nested"] is None
        assert uploaded["tags"] is None

    def test_all_null_column_is_written_as_strings(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        schema = pq.read_schema(pa.BufferReader(session.raw_uploads[0]))
        assert schema.field("day").type == pa.string()

    def test_insert_casts_each_column_by_name(self, session):
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [dict.fromkeys(_AllTypes.model_fields)])

        assert _insert(session) == (
            f"INSERT OVERWRITE {REF}.`ads_stats` BY NAME SELECT "
            "CAST(`flag` AS BOOLEAN) AS `flag`, CAST(`n` AS BIGINT) AS `n`, CAST(`cost` AS DOUBLE) AS `cost`, "
            "CAST(`amount` AS DECIMAL(38,9)) AS `amount`, CAST(`at` AS TIMESTAMP) AS `at`, "
            "CAST(`day` AS DATE) AS `day`, CAST(`blob` AS BINARY) AS `blob`, CAST(`name` AS STRING) AS `name`, "
            "CAST(`anything` AS STRING) AS `anything`, PARSE_JSON(`nested`) AS `nested`, "
            f"PARSE_JSON(`tags`) AS `tags` FROM {_read(_staged(session))}"
        )

    def test_each_write_gets_its_own_file(self, session):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        puts = [sql.split("'")[3] for sql in session.sql if sql.startswith("PUT ")]
        assert len(set(puts)) == 2

    def test_extra_columns_warn_and_drop(self, session):
        session.tables.add(("marts", "ads_stats"))
        with pytest.warns(UserWarning, match=r"Columns \['extra'\] are not in the schema"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{**DAY, "extra": "x"}])

        assert list(session.uploads[0].columns) == ["day", "cost"]
        assert "`extra`" not in _insert(session)


class TestWrite:
    def test_new_table_runs_ddl_then_one_insert(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        path = _staged(session)
        assert session.sql == [
            f"CREATE SCHEMA IF NOT EXISTS {REF}",
            f"CREATE TABLE IF NOT EXISTS {REF}.`ads_stats` (`day` DATE, `cost` DOUBLE) USING DELTA",
            f"PUT '__input_stream__' INTO '{path}' OVERWRITE",
            (
                f"INSERT OVERWRITE {REF}.`ads_stats` BY NAME SELECT CAST(`day` AS DATE) AS `day`, "
                f"CAST(`cost` AS DOUBLE) AS `cost` FROM {_read(path)}"
            ),
            f"REMOVE '{path}'",
        ]
        assert session.files == set()

    def test_time_partition_replaces_its_bounds(self, session):
        session.tables.add(("marts", "daily"))
        _destination().write(_ctx(daily(), TimePartition(JAN[0]), _DaySchema), [{"day": JAN[0], "cost": 1.0}])

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT", "INSERT", "REMOVE"]
        assert _insert(session).startswith(
            f"INSERT INTO {REF}.`daily` BY NAME REPLACE WHERE `day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-02' "
            "SELECT "
        )

    def test_monthly_partition_replaces_the_whole_month(self, session):
        session.tables.add(("marts", "monthly"))
        partition = TimePartition(JAN[0], TimeGranularity.MONTH)
        _destination().write(_ctx(monthly(), partition, _DaySchema), [{"day": JAN[1], "cost": 1.0}])

        assert "REPLACE WHERE `day` >= DATE'2024-01-01' AND `day` < DATE'2024-02-01' SELECT" in _insert(session)

    def test_non_time_partition_replaces_by_equality(self, session):
        session.tables.add(("marts", "regional"))
        _destination().write(_ctx(regional(), Partition("eu")), [{"region": "eu", "cost": 1.0}])

        assert f"INSERT INTO {REF}.`regional` BY NAME REPLACE WHERE `region` = 'eu' SELECT" in _insert(session)

    def test_window_is_one_contiguous_range(self, session):
        session.tables.add(("marts", "daily"))
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"day": datetime.date(2024, 1, d), "cost": 1.0} for d in (1, 2, 3)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT", "INSERT", "REMOVE"]
        assert "REPLACE WHERE `day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-04' SELECT" in _insert(session)
        assert len(session.uploads) == 1
        assert len(session.uploads[0]) == 3

    def test_write_partition_replaces_one_partition(self, session):
        session.tables.add(("marts", "regional"))
        _destination().write_partition(_ctx(regional()), Partition("us"), [{"region": "us"}])

        assert "REPLACE WHERE `region` = 'us' SELECT" in _insert(session)

    def test_write_partition_without_a_partition_overwrites(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().write_partition(_ctx(ads_stats()), None, [DAY])

        assert _insert(session).startswith(f"INSERT OVERWRITE {REF}.`ads_stats` BY NAME SELECT")

    def test_empty_data_runs_nothing(self, session):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [])

        assert session.statements == []

    def test_insert_hook_appends(self, session):
        session.tables.add(("marts", "ads_stats"))
        _destination().insert("ads_stats", "marts", [DAY], _ctx(ads_stats(), schema=_DaySchema))

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT", "INSERT", "REMOVE"]
        assert _insert(session).startswith(f"INSERT INTO {REF}.`ads_stats` BY NAME SELECT")


class TestCleanup:
    def test_failed_insert_still_removes_the_file(self, session):
        session.tables.add(("marts", "daily"))
        session.failures["INSERT "] = RuntimeError("DELTA_REPLACE_WHERE_MISMATCH")
        with pytest.raises(RuntimeError, match="DELTA_REPLACE_WHERE_MISMATCH"):
            _destination().write(_ctx(daily(), TimePartition(JAN[0]), _DaySchema), [{"day": JAN[1], "cost": 1.0}])

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT", "INSERT", "REMOVE"]
        assert session.sql[-1] == f"REMOVE '{_staged(session)}'"
        assert session.files == set()

    def test_failed_removal_only_warns(self, session):
        session.tables.add(("marts", "ads_stats"))
        session.failures["REMOVE "] = RuntimeError("volume unavailable")
        with pytest.warns(UserWarning, match="Could not remove the staged file .*volume unavailable"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT", "INSERT", "REMOVE"]

    def test_failed_removal_does_not_hide_a_failed_insert(self, session):
        session.tables.add(("marts", "ads_stats"))
        session.failures["INSERT "] = RuntimeError("insert failed")
        session.failures["REMOVE "] = RuntimeError("volume unavailable")
        with pytest.raises(RuntimeError, match="insert failed"), pytest.warns(UserWarning):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

    def test_failed_upload_inserts_nothing(self, session):
        session.tables.add(("marts", "daily"))
        session.failures["PUT "] = RuntimeError("upload failed")
        with pytest.raises(RuntimeError, match="upload failed"):
            _destination().write(_ctx(daily(), TimePartition(JAN[0]), _DaySchema), [{"day": JAN[0], "cost": 1.0}])

        assert [sql.split(" ")[0] for sql in session.sql] == ["PUT"]


class TestPredicate:
    def test_single_time_partition(self):
        where = PartitionFilter("day", bounds=(JAN[0], JAN[1]))

        assert destination_module._predicate([where]) == "`day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-02'"

    def test_contiguous_bounds_merge_in_any_order(self):
        days = [datetime.date(2024, 1, d) for d in (1, 2, 3, 4)]
        filters = [PartitionFilter("day", bounds=(days[i], days[i + 1])) for i in (2, 0, 1)]

        assert destination_module._predicate(filters) == "`day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-04'"

    def test_gaps_become_separate_ranges(self):
        filters = [
            PartitionFilter("day", bounds=(datetime.date(2024, 1, 5), datetime.date(2024, 1, 6))),
            PartitionFilter("day", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))),
        ]

        assert destination_module._predicate(filters) == (
            "(`day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-02') OR "
            "(`day` >= DATE'2024-01-05' AND `day` < DATE'2024-01-06')"
        )

    def test_one_value_is_equality(self):
        assert destination_module._predicate([PartitionFilter("region", value="eu")]) == "`region` = 'eu'"

    def test_several_values_are_a_list(self):
        filters = [PartitionFilter("region", value=v) for v in ("eu", "us")]

        assert destination_module._predicate(filters) == "`region` IN ('eu', 'us')"

    def test_hourly_bounds_are_timestamps(self):
        start = datetime.datetime(2024, 1, 1, 10)
        where = PartitionFilter("at", bounds=(start, start + datetime.timedelta(hours=1)))

        assert destination_module._predicate([where]) == (
            "`at` >= TIMESTAMP'2024-01-01T10:00:00Z' AND `at` < TIMESTAMP'2024-01-01T11:00:00Z'"
        )


class TestLiteral:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("eu", "'eu'"),
            ("it's", "'it\\'s'"),
            ("a\\b", "'a\\\\b'"),
            (datetime.date(2024, 1, 1), "DATE'2024-01-01'"),
            (datetime.datetime(2024, 1, 1, 10), "TIMESTAMP'2024-01-01T10:00:00Z'"),
            (
                datetime.datetime(2024, 1, 1, 10, tzinfo=datetime.timezone.utc),
                "TIMESTAMP'2024-01-01T10:00:00+00:00'",
            ),
            (None, "NULL"),
            (True, "TRUE"),
            (3, "3"),
            (Decimal("1.50"), "1.50"),
        ],
    )
    def test_renders(self, value, expected):
        assert destination_module._literal(value) == expected


class TestDelete:
    def test_missing_table_is_a_no_op(self, session):
        _destination().delete("daily", "marts", None)

        assert session.sql == []

    def test_whole_table(self, session):
        session.tables.add(("marts", "daily"))
        _destination().delete("daily", "marts", None)

        assert session.sql == [f"DELETE FROM {REF}.`daily`"]

    def test_by_filter(self, session):
        session.tables.add(("marts", "regional"))
        _destination().delete("regional", "marts", PartitionFilter("region", value="eu"))

        assert session.sql == [f"DELETE FROM {REF}.`regional` WHERE `region` = 'eu'"]


class TestRead:
    def test_whole_table_as_a_dataframe(self, session):
        session.tables.add(("marts", "ads_stats"))
        session.respond("SELECT *", arrow=pa.table({"cost": [1.0, 2.0]}))

        result = _destination().read(_ctx(ads_stats()))

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict("records") == [{"cost": 1.0}, {"cost": 2.0}]
        assert session.sql[-1] == f"SELECT * FROM {REF}.`ads_stats`"

    def test_time_partition_by_bounds(self, session):
        session.tables.add(("marts", "daily"))
        _destination().read(_ctx(daily(), TimePartition(JAN[0])))

        assert session.sql[-1] == (
            f"SELECT * FROM {REF}.`daily` WHERE `day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-02'"
        )

    def test_missing_table_raises(self, session):
        with pytest.raises(DataNotFoundError, match=r"does not exist\. Has the asset been materialized\?"):
            _destination().read(_ctx(ads_stats()))


class TestCount:
    def test_groups_by_the_partition_column(self, session):
        session.tables.add(("marts", "daily"))
        session.respond("SELECT CAST", rows=[("2024-01-01", 3), ("2024-01-02", 5)])

        counts = _destination().partition_row_counts(_ctx(daily()))

        assert counts == {"2024-01-01": 3, "2024-01-02": 5}
        assert session.sql[-1] == (
            f"SELECT CAST(`day` AS STRING) AS partition_value, COUNT(*) AS cnt FROM {REF}.`daily` GROUP BY 1"
        )

    def test_missing_table_raises(self, session):
        with pytest.raises(DataNotFoundError):
            _destination().partition_row_counts(_ctx(daily()))
