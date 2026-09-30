# interloper-snowflake

Snowflake integration for interloper: a `SnowflakeDestination` that stores
assets as Snowflake tables, and the `SnowflakeConnection` that holds the
credentials.

The destination is a `DatabaseDestination`: it writes the Snowflake dialect
and nothing else. Partition replacement, windows and reads by partition come
from core, exactly as for BigQuery.

## Setup

The connection needs an account identifier, a user and a password, and
optionally a role (the user's default role otherwise).

The account identifier is the part of your Snowflake URL before
`.snowflakecomputing.com`, in either format:

- `myorg-myaccount`: organisation and account name (preferred)
- `xy12345.eu-central-1`: the legacy account locator with its region, and
  its cloud where your URL carries one (e.g. `xy12345.us-east-2.aws`)

The role the connection runs as needs:

- `USAGE` on the warehouse the destination loads through
- `USAGE` on the database
- `CREATE SCHEMA` on the database, or `USAGE` on every schema the assets
  write to if you create them yourself
- `CREATE TABLE` on those schemas
- `INSERT`, `DELETE`, `SELECT` on the tables, and `TRUNCATE` for
  unpartitioned assets (the table owner has all of them)

A sketch for a dedicated loading role:

```sql
CREATE ROLE interloper;
GRANT USAGE ON WAREHOUSE load_wh TO ROLE interloper;
GRANT USAGE, CREATE SCHEMA ON DATABASE analytics TO ROLE interloper;
GRANT ROLE interloper TO USER interloper_loader;
```

Schemas and tables the role creates are owned by it, so the remaining grants
follow.

## Usage

```python
import interloper as il
from interloper_snowflake import SnowflakeConnection, SnowflakeDestination

destination = SnowflakeDestination(
    connection=SnowflakeConnection(account="myorg-myaccount", user="LOADER", password="..."),
    database="ANALYTICS",
    warehouse="LOAD_WH",
    default_dataset="raw",
)
```

The credentials also load from the environment (`SNOWFLAKE_ACCOUNT`,
`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`), so
`SnowflakeConnection()` works with no arguments.

In a deployed instance you configure this through the UI instead: add a
Snowflake connection, then a Snowflake destination, picking the database and
warehouse from the lists the connection can see.

## Datasets are schemas

An asset's `dataset` is the Snowflake schema its table lives in, inside the
destination's `database`. An asset without a dataset falls back to
`default_dataset`; with neither, the write fails with a `ConfigError`. A
missing schema is created on the first write, and a missing table is created
typed from the asset's schema (or one inferred from the data):

| Field type | Snowflake type |
|------------|----------------|
| `bool` | `BOOLEAN` |
| `int` | `NUMBER(38,0)` |
| `float` | `FLOAT` |
| `Decimal` | `NUMBER(38,9)` |
| `datetime` | `TIMESTAMP_NTZ` |
| `date` | `DATE` |
| `bytes` | `BINARY` |
| `str`, `Any` | `VARCHAR` |
| nested models, lists, dicts | `VARIANT` |

An existing table is never altered: a column the data carries but the table
does not is dropped with a warning.

## Quoted identifiers

Every database, schema, table and column name is double-quoted, so Snowflake
keeps it exactly as the asset spells it. Snowflake folds *unquoted*
identifiers to upper case, so a lower-case asset must be queried with quotes:

```sql
SELECT "cost" FROM "ANALYTICS"."marts"."ads_stats";
-- SELECT cost FROM analytics.marts.ads_stats looks for "COST" in "ADS_STATS" and fails
```

## Partitions

A partitioned write replaces the partition's rows: it deletes them, then loads
the data, inside one `BEGIN ... COMMIT` rolled back on failure (see Notes for
where Snowflake cuts that short). A time
partition deletes by half-open bounds (`"day" >= %s AND "day" < %s`), so a
monthly partition whose rows hold daily dates is replaced whole; any other
partition deletes by equality on its id. A window deletes each partition it
covers and loads the whole batch once. An unpartitioned asset truncates its
table and reloads it.

Loads go through the connector's `write_pandas` (Parquet files on a temporary
stage, then `COPY INTO`), reads through `fetch_pandas_all`, so both directions
stay columnar.

## Notes

The connection's session is opened with autocommit on and shared by every
destination bound to it. Each destination selects its warehouse once
(`USE WAREHOUSE`) the first time it runs a statement, and serialises its own
transactions, since a Snowflake transaction belongs to the session rather than
to a cursor.

Snowflake commits an open transaction when it runs DDL, and `write_pandas`
creates its temporary stage with DDL, so the delete of a replaced partition
may already be committed when the load starts. A failed load can then leave
the partition empty until the next run rewrites it.
## Docker images

The published interloper images do not ship this package. They are built on
Alpine, and `snowflake-connector-python` publishes no musllinux wheels, so
installing it there means compiling its C++ extension from source. Run it from
a glibc-based image (for example a `python:3.12-slim` base with
`pip install interloper-snowflake`), or anywhere outside the images.
