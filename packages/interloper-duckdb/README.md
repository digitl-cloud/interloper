# interloper-duckdb

DuckDB tables as an interloper destination: a `DuckDBDestination` writing to a
local `.duckdb` file or a MotherDuck database, and the `DuckDBConnection` that
opens it.

## Setup

A **local file** needs nothing but a path; DuckDB creates the file on first
write:

```python
from interloper_duckdb import DuckDBConnection

connection = DuckDBConnection(database="./warehouse.duckdb")
```

A **MotherDuck** database is named `md:<database>` and authenticates with a
service token from the MotherDuck settings page:

```python
connection = DuckDBConnection(database="md:my_db", motherduck_token="...")
```

Both fields also load from the environment (`DUCKDB_DATABASE`,
`DUCKDB_MOTHERDUCK_TOKEN`), so `DuckDBConnection()` works with no arguments.

## Usage

```python
import interloper as il
from interloper_duckdb import DuckDBConnection, DuckDBDestination

destination = DuckDBDestination(
    connection=DuckDBConnection(database="./warehouse.duckdb"),
    default_dataset="raw",
)
```

In a deployed instance you configure this through the UI instead: add a DuckDB
connection, then a DuckDB destination using it.

## Datasets are schemas

An asset's dataset is a DuckDB schema. A table lands in the asset's dataset,
else in the destination's `default_dataset`, else in `main`. The schema and
the table are created on the first write, the columns typed from the asset's
schema (or from one inferred from the data), and never altered afterwards: a
column the table does not have is dropped from the write with a warning.

| Field type | Column type |
|------------|-------------|
| `bool` | `BOOLEAN` |
| `int` | `BIGINT` |
| `float` | `DOUBLE` |
| `Decimal` | `DECIMAL(38,9)` |
| `datetime` | `TIMESTAMP` |
| `date` | `DATE` |
| `bytes` | `BLOB` |
| `str`, `Any` | `VARCHAR` |
| nested model, `list[...]` | `JSON` |

## Partitions

A write replaces what it covers, in one transaction: the whole table for an
unpartitioned asset, the rows inside a time partition's bounds
(`day >= start AND day < end`), or the rows equal to a partition's id for any
other partitioning. A window deletes each partition it covers and inserts the
whole batch once. If the insert fails, the delete is rolled back and the table
keeps its previous rows.

## One writer per file

A local DuckDB file admits one writing process at a time. Concurrent assets in
one process are fine (each write runs on its own cursor), but two processes
writing to the same file, such as two pods or a scheduler and a notebook,
fail to open it. A connection holds the file from its first use until its
process exits, and that includes the connection check the app runs from the
API process. Run the instance's writes in one process, or use MotherDuck,
which serves many writers.

## Querying the tables

The tables are plain DuckDB tables, so any DuckDB client reads them:

```bash
duckdb warehouse.duckdb -c 'SELECT * FROM raw.ads_stats LIMIT 10'
```

```python
import duckdb

duckdb.connect("warehouse.duckdb", read_only=True).sql("SELECT * FROM raw.ads_stats").df()
```

Another process can open the file only while no process holds it for
writing. Open it `read_only`, so the reader does not lock the instance out in
turn.
