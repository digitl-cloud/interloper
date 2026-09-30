# interloper-sql

A SQL database destination for interloper: `SQLDestination`, a
`DatabaseDestination` over SQLAlchemy Core, and the `SQLConnection` that holds
the database URL.

One backend serves every SQLAlchemy dialect whose driver is installed.
PostgreSQL works out of the box (the package depends on `psycopg[binary]`);
MySQL, SQLite, Redshift, SQL Server and the rest need their own driver.

## Setup

The connection is a single SQLAlchemy URL:

| Database | URL | Driver to install |
|----------|-----|-------------------|
| PostgreSQL | `postgresql+psycopg://user:password@host:5432/db` | none, ships with the package |
| MySQL / MariaDB | `mysql+pymysql://user:password@host:3306/db` | `pymysql` |
| SQL Server | `mssql+pyodbc://user:password@host/db?driver=ODBC+Driver+18+for+SQL+Server` | `pyodbc` (and the ODBC driver) |
| Redshift | `redshift+redshift_connector://user:password@host:5439/db` | `sqlalchemy-redshift`, `redshift_connector` |
| SQLite | `sqlite:///path/to/file.db` | none, in the standard library |

Install the driver next to interloper (`uv pip install pymysql`, or a custom
image built from the slim variant). Special characters in the password must be
URL-encoded.

The URL carries the password, so it is a secret field: encrypted at rest in a
deployed instance and masked in the UI. It also loads from the environment
(`SQL_URL`), so `SQLConnection()` works with no arguments.

The connection's check runs `SELECT 1`.

## Usage

```python
import interloper as il
from interloper_sql import SQLConnection, SQLDestination

warehouse = SQLDestination(
    connection=SQLConnection(url="postgresql+psycopg://user:password@host:5432/analytics"),
    default_dataset="marketing",
)
```

In a deployed instance you configure this through the UI instead: add a SQL
database connection, then a SQL database destination using it.

## Datasets are schemas

An asset's `dataset` is the SQL schema its table lives in. Without one, the
destination's `default_dataset` applies; without either, the table goes in
the connection's default schema (`public` on PostgreSQL, the database itself on
MySQL). A named schema is created on first write when the database supports
schemas and it does not exist yet.

## Tables

A table is created on first write from the asset's effective schema (declared,
or inferred during conform): integers are `BIGINT`, floats `DOUBLE`, decimals
`NUMERIC(38, 9)`, datetimes timezone-aware timestamps, strings `TEXT`, and
nested or repeated fields `JSON`. An existing table is never altered: columns
it lacks are dropped from the write with a warning, and NaN or infinite floats
are written as `NULL`.

## Partitions

Writes replace, inside one transaction:

- unpartitioned: every row is deleted, then the data inserted;
- a time partition: the rows inside its half-open bounds
  (`column >= start AND column < end`) are deleted, then the data inserted;
- any other partition: the rows equal to its id are deleted;
- a window: each partition's rows are deleted, then the whole batch is inserted
  once.

A failed insert rolls the delete back, so a partition is never left empty.
Inserts go in batches of 1000 rows. Reads return rows as `list[dict]`; reading
a table that was never written raises `DataNotFoundError`.
