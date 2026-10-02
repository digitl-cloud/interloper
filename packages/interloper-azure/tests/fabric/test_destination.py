"""Tests for ``interloper_azure.fabric.destination``."""

import datetime
import json
import math
import threading
from decimal import Decimal
from typing import Any

import interloper as il
import pytest
from interloper.destination import IOContext
from interloper.errors import DataNotFoundError
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_azure import AzureConnection, FabricWarehouseDestination
from interloper_azure.fabric import destination as destination_module
from interloper_azure.fabric.destination import MAX_PARAMETERS, MAX_ROWS, batch_size


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


SERVER = "abc-xyz.datawarehouse.fabric.microsoft.com"
DAY = {"day": datetime.date(2024, 1, 1), "cost": 1.0}
COLUMNS_PROBE = (
    "SELECT c.name FROM sys.columns AS c "
    "JOIN sys.tables AS t ON c.object_id = t.object_id "
    "JOIN sys.schemas AS s ON t.schema_id = s.schema_id "
    "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id"
)


def _destination(**overrides: Any) -> FabricWarehouseDestination:
    connection = AzureConnection(id="az", tenant_id="tenant", client_id="client", client_secret="<secret>")
    fields = {"server": SERVER, "warehouse": "Sales", **overrides}
    return FabricWarehouseDestination(id="dest", connection=connection, **fields)


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _parameters(warehouse, prefix: str) -> list[Any]:
    return next(s.parameters for s in warehouse.statements if s.sql.startswith(prefix))


class TestMetadata:
    def test_decorator(self):
        definition = FabricWarehouseDestination.definition()
        assert (definition.key, definition.name, definition.icon, definition.tags) == (
            "fabric_warehouse_destination",
            "Microsoft Fabric Warehouse",
            "icon:fabric",
            ["Cloud"],
        )

    def test_warehouse_is_the_discriminator(self):
        extra = FabricWarehouseDestination.model_fields["warehouse"].json_schema_extra
        assert extra["x-discriminator"] is True

    def test_server_is_picked_from_the_connections_workspaces(self):
        extra = FabricWarehouseDestination.model_fields["server"].json_schema_extra
        assert extra["x-fetch"] == {"provider": "connection.workspaces", "label_key": "name", "value_key": "server"}


class TestSession:
    def test_connects_with_the_principals_token(self, warehouse):
        destination = _destination()
        destination.connect()

        args, kwargs = warehouse.connects[0]
        assert args == (
            (
                "Server={abc-xyz.datawarehouse.fabric.microsoft.com};Database={Sales};"
                "Encrypt=yes;TrustServerCertificate=no"
            ),
        )
        assert kwargs == {"autocommit": True, "token_provider": destination.connection.credential}

    def test_names_are_braced(self, warehouse):
        _destination(warehouse="We;ird}").connect()

        assert "Database={We;ird}}};" in warehouse.connects[0][0][0]

    def test_reads_open_and_close_their_own_session(self, warehouse):
        warehouse.table("marts", "ads_stats", "id")
        destination = _destination()
        destination.read(_ctx(ads_stats()))
        destination.read(_ctx(ads_stats()))

        assert len(warehouse.sessions) == 2
        assert all(session.closed for session in warehouse.sessions)


class TestNaming:
    def test_asset_dataset_is_the_schema(self, warehouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert "CREATE SCHEMA [marts]" in warehouse.sql
        assert any(sql.startswith("INSERT INTO [marts].[ads_stats] ") for sql in warehouse.sql)

    def test_default_dataset_fallback(self, warehouse):
        _destination(default_dataset="raw").write(_ctx(undated(), schema=_DaySchema), [DAY])

        assert any(sql.startswith("CREATE TABLE [raw].[undated] ") for sql in warehouse.sql)

    def test_dbo_is_the_final_fallback(self, warehouse):
        _destination().write(_ctx(undated(), schema=_DaySchema), [DAY])

        assert any(sql.startswith("CREATE TABLE [dbo].[undated] ") for sql in warehouse.sql)
        assert not any(sql.startswith("CREATE SCHEMA") for sql in warehouse.sql)

    def test_brackets_are_doubled(self):
        assert destination_module._quote("we]ird") == "[we]]ird]"

    def test_catalog_probe_is_parameterised(self, warehouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        probe = warehouse.statements[0]
        assert (probe.sql, probe.parameters) == (COLUMNS_PROBE, ["marts", "ads_stats"])


class TestDDL:
    def test_every_type(self, warehouse):
        row = dict.fromkeys(_AllTypes.model_fields)
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        create = next(s for s in warehouse.sql if s.startswith("CREATE TABLE"))
        assert create == (
            "CREATE TABLE [marts].[ads_stats] ("
            "[flag] bit NULL, [n] bigint NULL, [cost] float NULL, [amount] decimal(38,9) NULL, "
            "[at] datetime2(6) NULL, [day] date NULL, [blob] varbinary(max) NULL, [name] varchar(max) NULL, "
            "[anything] varchar(max) NULL, [nested] varchar(max) NULL, [tags] varchar(max) NULL)"
        )

    def test_infers_without_a_schema(self, warehouse):
        _destination().write(_ctx(ads_stats()), [{"id": 1, "label": "a"}])

        create = next(s for s in warehouse.sql if s.startswith("CREATE TABLE"))
        assert create == "CREATE TABLE [marts].[ads_stats] ([id] bigint NULL, [label] varchar(max) NULL)"

    def test_schema_is_checked_in_the_catalog(self, warehouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        check = next(s for s in warehouse.statements if s.sql.startswith("SELECT 1 FROM sys.schemas"))
        assert (check.sql, check.parameters) == ("SELECT 1 FROM sys.schemas WHERE name = ?", ["marts"])

    def test_existing_schema_is_not_created(self, warehouse):
        warehouse.schemas.add("marts")
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert not any(sql.startswith("CREATE SCHEMA") for sql in warehouse.sql)
        assert any(sql.startswith("CREATE TABLE") for sql in warehouse.sql)

    def test_existing_table_is_not_created(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost")
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert not any(sql.startswith("CREATE") for sql in warehouse.sql)

    def test_ddl_commits_before_the_transaction_opens(self, warehouse):
        _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert warehouse.verbs == ["CREATE", "CREATE", "BEGIN", "INSERT", "COMMIT"]


class TestWriteShapes:
    def test_whole_table_deletes_every_row(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost")
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        assert warehouse.sql == [
            "BEGIN TRANSACTION",
            "DELETE FROM [marts].[ads_stats]",
            "INSERT INTO [marts].[ads_stats] ([day], [cost]) VALUES (?, ?)",
            "COMMIT TRANSACTION",
        ]
        assert _parameters(warehouse, "INSERT") == [datetime.date(2024, 1, 1), 1.0]

    def test_time_partition_deletes_by_half_open_bounds(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert warehouse.verbs == ["BEGIN", "DELETE", "INSERT", "COMMIT"]
        delete = next(s for s in warehouse.statements if s.sql.startswith("DELETE"))
        assert delete.sql == "DELETE FROM [marts].[daily] WHERE [day] >= ? AND [day] < ?"
        assert delete.parameters == [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]

    def test_other_partition_deletes_by_equality(self, warehouse):
        warehouse.table("marts", "regional", "region", "cost")
        _destination().write(_ctx(regional(), Partition("eu")), [{"region": "eu", "cost": 1.0}])

        delete = next(s for s in warehouse.statements if s.sql.startswith("DELETE"))
        assert (delete.sql, delete.parameters) == ("DELETE FROM [marts].[regional] WHERE [region] = ?", ["eu"])

    def test_window_deletes_each_partition_and_inserts_once(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        rows = [{"day": datetime.date(2024, 1, d), "cost": 1.0} for d in (1, 2, 3)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        assert warehouse.verbs == ["BEGIN", "DELETE", "DELETE", "DELETE", "INSERT", "COMMIT"]
        deletes = sorted(s.parameters for s in warehouse.statements if s.sql.startswith("DELETE"))
        assert deletes == [
            [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)],
            [datetime.date(2024, 1, 3), datetime.date(2024, 1, 4)],
        ]
        assert _parameters(warehouse, "INSERT") == [
            datetime.date(2024, 1, 1),
            1.0,
            datetime.date(2024, 1, 2),
            1.0,
            datetime.date(2024, 1, 3),
            1.0,
        ]

    def test_new_table_has_nothing_to_delete(self, warehouse):
        _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert "DELETE" not in warehouse.verbs

    def test_empty_data_writes_nothing(self, warehouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [])

        assert warehouse.statements == []


class TestInsert:
    def test_rows_share_one_multi_row_statement(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost")
        rows = [{"day": datetime.date(2024, 1, 1), "cost": 1.0}, {"day": datetime.date(2024, 1, 2), "cost": 2.0}]
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), rows)

        insert = next(s for s in warehouse.statements if s.sql.startswith("INSERT"))
        assert insert.sql == "INSERT INTO [marts].[ads_stats] ([day], [cost]) VALUES (?, ?), (?, ?)"
        assert insert.parameters == [datetime.date(2024, 1, 1), 1.0, datetime.date(2024, 1, 2), 2.0]

    def test_batches_split_at_the_row_cap(self, warehouse):
        warehouse.table("marts", "ads_stats", "id")
        _destination().write(_ctx(ads_stats()), [{"id": i} for i in range(MAX_ROWS + 1)])

        inserts = [s for s in warehouse.statements if s.sql.startswith("INSERT")]
        assert [len(s.parameters) for s in inserts] == [MAX_ROWS, 1]
        assert inserts[1].sql == "INSERT INTO [marts].[ads_stats] ([id]) VALUES (?)"
        assert inserts[1].parameters == [MAX_ROWS]
        assert warehouse.verbs == ["BEGIN", "DELETE", "INSERT", "INSERT", "COMMIT"]

    def test_batches_stay_under_the_parameter_cap(self, warehouse):
        columns = [f"c{i}" for i in range(7)]
        warehouse.table("marts", "ads_stats", *columns)
        rows = [dict.fromkeys(columns, 1) for _ in range(700)]
        _destination().write(_ctx(ads_stats()), rows)

        sizes = [len(s.parameters) for s in warehouse.statements if s.sql.startswith("INSERT")]
        assert sizes == [299 * 7, 299 * 7, 102 * 7]
        assert max(sizes) <= MAX_PARAMETERS

    def test_extra_columns_are_dropped_with_a_warning(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost")

        with pytest.warns(UserWarning, match=r"\['extra'\] are not in the schema for 'marts.ads_stats'"):
            _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{**DAY, "extra": 1}])

        insert = next(s for s in warehouse.statements if s.sql.startswith("INSERT"))
        assert insert.sql == "INSERT INTO [marts].[ads_stats] ([day], [cost]) VALUES (?, ?)"

    def test_columns_no_record_carries_are_left_out(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost", "note")
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [DAY])

        insert = next(s for s in warehouse.statements if s.sql.startswith("INSERT"))
        assert insert.sql == "INSERT INTO [marts].[ads_stats] ([day], [cost]) VALUES (?, ?)"

    def test_values_are_bound_for_their_columns(self, warehouse):
        warehouse.table("marts", "ads_stats", "nested", "tags", "cost", "at", "naive")
        aware = datetime.datetime(2024, 1, 1, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        naive = datetime.datetime(2024, 1, 1, 12)
        row = {
            "nested": {"x": 1, "when": datetime.date(2024, 1, 1), "ratio": math.nan},
            "tags": ["a", "b"],
            "cost": math.inf,
            "at": aware,
            "naive": naive,
        }
        _destination().write(_ctx(ads_stats()), [row])

        nested, tags, cost, at, kept = _parameters(warehouse, "INSERT")
        assert json.loads(nested) == {"x": 1, "when": "2024-01-01", "ratio": None}
        assert json.loads(tags) == ["a", "b"]
        assert cost is None
        assert at == datetime.datetime(2024, 1, 1, 10)
        assert kept is naive

    def test_insert_outside_a_write_creates_the_table(self, warehouse):
        _destination().insert("ads_stats", "marts", [DAY], _ctx(ads_stats(), schema=_DaySchema))

        assert warehouse.verbs == ["CREATE", "CREATE", "INSERT"]


class TestTransaction:
    def test_commits_on_one_session(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert {s.session for s in warehouse.statements} == {0}
        assert warehouse.sql[0] == "BEGIN TRANSACTION"
        assert warehouse.sql[-1] == "COMMIT TRANSACTION"
        assert warehouse.sessions[0].closed

    def test_rolls_back_and_reraises(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        warehouse.failures["INSERT"] = RuntimeError("insert failed")

        with pytest.raises(RuntimeError, match="insert failed"):
            _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert warehouse.verbs == ["BEGIN", "DELETE", "INSERT", "ROLLBACK"]
        assert warehouse.sql[-1] == "ROLLBACK TRANSACTION"
        assert warehouse.sessions[0].closed

    def test_failure_before_any_change_has_nothing_to_roll_back(self, warehouse):
        warehouse.failures["CREATE TABLE"] = RuntimeError("no permission")

        with pytest.raises(RuntimeError, match="no permission"):
            _destination().write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), _DaySchema), [DAY])

        assert "ROLLBACK" not in warehouse.verbs
        assert "BEGIN" not in warehouse.verbs

    def test_concurrent_writes_hold_separate_sessions(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        destination = _destination()
        barrier = threading.Barrier(2)

        def write(day: int) -> None:
            barrier.wait()
            destination.write(
                _ctx(daily(), TimePartition(datetime.date(2024, 1, day)), _DaySchema),
                [{"day": datetime.date(2024, 1, day), "cost": 1.0}],
            )

        threads = [threading.Thread(target=write, args=(day,)) for day in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for session in (0, 1):
            verbs = [s.sql.split(" ")[0] for s in warehouse.statements if s.session == session and "sys." not in s.sql]
            assert verbs == ["BEGIN", "DELETE", "INSERT", "COMMIT"]


class TestRead:
    def test_whole_table_as_records(self, warehouse):
        warehouse.table("marts", "ads_stats", "day", "cost")
        warehouse.results["SELECT * FROM"] = (["day", "cost"], [(datetime.date(2024, 1, 1), 1.0)])

        rows = _destination().read(_ctx(ads_stats()))

        assert rows == [{"day": datetime.date(2024, 1, 1), "cost": 1.0}]
        assert warehouse.sql == ["SELECT * FROM [marts].[ads_stats]"]

    def test_time_partition_reads_by_bounds(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        _destination().read(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1))))

        select = warehouse.statements[-1]
        assert select.sql == "SELECT * FROM [marts].[daily] WHERE [day] >= ? AND [day] < ?"
        assert select.parameters == [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]

    def test_other_partition_reads_by_equality(self, warehouse):
        warehouse.table("marts", "regional", "region", "cost")
        _destination().read(_ctx(regional(), Partition("eu")))

        select = warehouse.statements[-1]
        assert (select.sql, select.parameters) == ("SELECT * FROM [marts].[regional] WHERE [region] = ?", ["eu"])

    def test_missing_table_raises(self, warehouse):
        with pytest.raises(DataNotFoundError, match=r"Table 'marts.ads_stats' does not exist. Has the asset been"):
            _destination().read(_ctx(ads_stats()))


class TestCount:
    def test_groups_by_the_partition_column(self, warehouse):
        warehouse.table("marts", "daily", "day", "cost")
        warehouse.results["SELECT CAST("] = (["partition_value", "cnt"], [("2024-01-01", 3), ("2024-01-02", 5)])

        counts = _destination().partition_row_counts(_ctx(daily()))

        assert counts == {"2024-01-01": 3, "2024-01-02": 5}
        assert warehouse.sql == [
            (
                "SELECT CAST([day] AS varchar(max)) AS partition_value, COUNT(*) AS cnt "
                "FROM [marts].[daily] GROUP BY CAST([day] AS varchar(max))"
            )
        ]

    def test_missing_table_raises(self, warehouse):
        with pytest.raises(DataNotFoundError, match="does not exist"):
            _destination().partition_row_counts(_ctx(daily()))


class TestBatchSize:
    @pytest.mark.parametrize(
        ("columns", "rows"),
        [(1, 1000), (2, 1000), (3, 699), (7, 299), (100, 20), (2099, 1), (2100, 1), (0, 1000)],
    )
    def test_rows_per_statement(self, columns, rows):
        assert batch_size(columns) == rows

    def test_never_exceeds_either_cap(self):
        for columns in range(1, 1025):
            size = batch_size(columns)
            assert size <= MAX_ROWS
            assert size * columns <= MAX_PARAMETERS or size == 1
