# interloper-clickhouse

ClickHouse integration for interloper: a `ClickHouseDestination` that stores
assets as ClickHouse `MergeTree` tables, and the `ClickHouseConnection` that
holds the server address and credentials. It works against self-hosted
ClickHouse and ClickHouse Cloud, over ClickHouse's HTTP interface through the
official [`clickhouse-connect`](https://clickhouse.com/docs/integrations/python) client.

## Setup

The connection needs a host, a user (`default` unless you say otherwise) and
its password.

**ClickHouse Cloud** only accepts TLS, on port 8443. Copy the host from the
service's *Connect* dialog and keep the defaults (`secure` on, no port):

```python
from interloper_clickhouse import ClickHouseConnection

connection = ClickHouseConnection(
    host="abc123.eu-central-1.aws.clickhouse.cloud",
    username="interloper",
    password="...",
)
```

**Self-hosted** servers listen on 8123 for plain HTTP and 8443 for HTTPS. Turn
`secure` off for a server without TLS; the port then defaults to 8123:

```python
connection = ClickHouseConnection(host="clickhouse.internal", password="...", secure=False)
```

Every field also loads from the environment (`CLICKHOUSE_HOST`,
`CLICKHOUSE_PORT`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`,
`CLICKHOUSE_SECURE`), so `ClickHouseConnection()` works with no arguments.

### Grants

The user needs, on the databases the assets write to:

- `CREATE DATABASE`, unless you create the databases yourself
- `CREATE TABLE` and `DROP TABLE`: each write creates a staging table next to
  its target and drops it afterwards
- `INSERT` and `SELECT`
- `ALTER DELETE`, the privilege ClickHouse checks for `DROP PARTITION` and,
  with `INSERT`, for `REPLACE PARTITION` (it also covers the `delete` hook's
  lightweight `DELETE`), and `TRUNCATE` for that hook's whole-table case

A sketch for a dedicated user:

```sql
CREATE USER interloper IDENTIFIED BY '...';
GRANT CREATE DATABASE, CREATE TABLE, DROP TABLE, INSERT, SELECT, ALTER DELETE, TRUNCATE
    ON marts.* TO interloper;
```

Each staging load sets `max_partitions_per_insert_block`, so the user must be
allowed to change settings (not `readonly = 1`).

## Usage

```python
import interloper as il
from interloper_clickhouse import ClickHouseConnection, ClickHouseDestination

destination = ClickHouseDestination(
    connection=ClickHouseConnection(host="abc123.eu-central-1.aws.clickhouse.cloud", password="..."),
    default_dataset="raw",
)
```

In a deployed instance you configure this through the UI instead: add a
ClickHouse connection, then a ClickHouse destination.

## Datasets are databases

An asset's `dataset` is the ClickHouse database its table lives in. An asset
without a dataset falls back to `default_dataset`, then to ClickHouse's
`default` database. A missing database is created on the first write
(`CREATE DATABASE IF NOT EXISTS`), and a missing table is created typed from
the asset's schema (or one inferred from the data):

| Field type | ClickHouse type |
|------------|-----------------|
| `bool` | `Bool` |
| `int` | `Int64` |
| `float` | `Float64` |
| `Decimal` | `Decimal(38, 9)` |
| `datetime` | `DateTime64(6, 'UTC')` |
| `date` | `Date32` |
| `bytes`, `str` | `String` |
| nested models, lists, dicts, `Any` | `String` holding JSON |

A nullable field (and an `Any` field) becomes `Nullable(T)`; every other
column is a plain `T`. Nested and repeated fields are JSON text
rather than the `JSON` type: that type is production-ready only from
ClickHouse 25.3, and it holds JSON objects, so a list field could not be
stored in it.

An existing table is never altered: a column the data carries but the table
does not is dropped with a warning.

## Table layout

Tables use the `MergeTree` engine (ClickHouse Cloud turns it into
`SharedMergeTree` by itself). The layout follows the asset's partitioning, so
that **one interloper partition is exactly one ClickHouse partition**:

| Asset partitioning | `PARTITION BY` | `ORDER BY` |
|--------------------|----------------|------------|
| none | (none) | `tuple()` |
| daily, on a `date` | `day` | `day` |
| daily, on a `datetime` | `toDate(at)` | `at` |
| hourly | `toStartOfHour(at)` | `at` |
| monthly | `toStartOfMonth(day)` | `day` |
| yearly | `toStartOfYear(day)` | `day` |
| any other `PartitionConfig` | `region` | `region` |

A time partition column held as text (ISO dates in a `String`) is parsed with
`parseDateTime64BestEffort` inside the key. Hourly partitions need a datetime
column; a `date` cannot hold them and the write fails with a `ConfigError`.

The partition column is the one column never made `Nullable`, since ClickHouse
keeps nullable columns out of partition and sorting keys. A row without a
partition value fails the load.

ClickHouse recommends keeping a table under about a thousand partitions, and
daily partitions reach that in under three years. Prefer monthly partitioning
for long histories.

If a table already exists with a different partition key (say the asset moved
from daily to monthly), the write fails with a `ConfigError` instead of
replacing the wrong rows: recreate the table, or partition the asset to match.

## Atomic replaces without transactions

ClickHouse has no production multi-statement transactions (they are
experimental and unavailable on ClickHouse Cloud), and `DELETE` is a mutation
rather than a cheap row operation. So the destination does not use core's
delete-then-insert. Each write:

1. creates a staging table `_interloper_staging_<table>_<random>` with
   `CREATE TABLE ... AS <target>`, the same columns, engine and keys;
2. loads the whole batch into it in a single `insert_df` (a window is one
   insert, as ClickHouse wants few large inserts);
3. for each partition the write covers, runs
   `ALTER TABLE <target> REPLACE PARTITION ID '<id>' FROM <staging>`, or
   `DROP PARTITION ID '<id>'` when the batch holds no rows for it. The ids
   come from ClickHouse's own `partitionId` over the table's partition key, so
   they match the parts whatever the column type;
4. drops the staging table (`DROP TABLE IF EXISTS ... SYNC`), whether or not
   the steps before it succeeded.

An unpartitioned table is one partition, `tuple()`, replaced the same way. A
whole write to a partitioned table replaces every partition the table or the
batch holds.

What this guarantees:

- **Each partition is replaced atomically.** A reader sees a partition's old
  rows or its new rows, never a mix and never neither.
- **A failed load leaves the target untouched**: nothing reaches the target
  before the staging table holds the whole batch.
- **A window is not atomic as a whole.** It replaces its partitions one after
  the other; a failure part way leaves the earlier ones replaced and the
  later ones as they were. Running the write again converges.
- **A partition in a window that the data does not cover is cleared**, as in
  every other destination.
- Rows whose partition value falls outside the partitions being written are
  not written, with a warning.

Reads select by the asset's partition column with server-side bound
parameters typed as the column (`` `day` >= {start:Date32} AND `day` < {end:Date32} ``),
and come back as pandas DataFrames.

## Notes

One client serves every destination on a connection, across threads: each
statement is its own HTTP request, and session ids are turned off because a
ClickHouse session admits one query at a time.

The destination does not issue `ON CLUSTER` statements. On a self-hosted
cluster, use a `Replicated` database engine so tables and partition changes
reach every replica.
