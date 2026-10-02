# interloper-azure

Microsoft Azure integration for interloper: the `AzureConnection` that holds a
Microsoft Entra service principal, and a `FabricWarehouseDestination` that
stores assets as tables in a Microsoft Fabric Warehouse.

The destination is a `DatabaseDestination`: it writes Fabric's T-SQL dialect
and nothing else. Partition replacement, windows and reads by partition come
from core, exactly as for BigQuery or Snowflake.

It targets a Fabric **Warehouse**. A Lakehouse's SQL analytics endpoint speaks
the same protocol but is read-only: creating tables and inserting or deleting
rows is only supported in a Warehouse.

## Setup

### 1. Create a service principal

In the [Microsoft Entra admin center](https://entra.microsoft.com), under
*App registrations*, register an application (no redirect URI needed), then on
its *Certificates & secrets* page create a client secret. Note three values:

- the *Directory (tenant) ID*
- the *Application (client) ID*
- the secret's *Value* (shown once, not its *Secret ID*)

### 2. Let service principals use Fabric

A Fabric administrator must enable **Service principals can use Fabric APIs**
in the Fabric admin portal (*Tenant settings*, *Developer settings*), either for
the whole organisation or for a security group the principal belongs to. The
same setting governs both the REST API (used by the connection check and the
workspace picker) and SQL connections to a warehouse.

### 3. Give the principal access to the workspace

In the workspace, open *Manage access* and add the app with the
**Contributor** role. Contributor, like Admin and Member, grants `CONTROL` on
every warehouse of the workspace, which covers creating schemas and tables and
writing rows; `CREATE SCHEMA` in particular requires one of those three roles.
Viewer only reads, and sharing a single warehouse with no extra permissions
only lets the principal connect.

### 4. Find the SQL connection string

Open the warehouse's *Settings* and its *SQL connection string* page; the
string looks like `xxxxxxxx-xxxx.datawarehouse.fabric.microsoft.com`. The string
belongs to the workspace: every warehouse in it shares the same one, and the
warehouse's name selects the database. In the UI you do not need to copy it:
the destination's *Workspace* picker lists the workspaces the principal can see
that hold a warehouse, and stores that workspace's connection string.

## Usage

```python
from interloper_azure import AzureConnection, FabricWarehouseDestination

destination = FabricWarehouseDestination(
    connection=AzureConnection(tenant_id="...", client_id="...", client_secret="..."),
    server="xxxxxxxx-xxxx.datawarehouse.fabric.microsoft.com",
    warehouse="Analytics",
    default_dataset="raw",
)
```

The credentials also load from the environment under the standard
azure-identity names (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
`AZURE_CLIENT_SECRET`), so `AzureConnection()` works with no arguments.

In a deployed instance you configure this through the UI: add a Microsoft
Azure connection, then a Microsoft Fabric Warehouse destination, pick the
workspace and type the warehouse name.

## The driver and its system libraries

Statements go through [`mssql-python`](https://github.com/microsoft/mssql-python),
Microsoft's DB-API driver, which signs in with an Entra access token obtained
from the connection's credential (scope `https://database.windows.net/.default`).
It is pip-installable and bundles Microsoft's ODBC driver, but that driver
loads a few system libraries on Linux. On Debian or Ubuntu:

```sh
apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

The published scheduler and core images, the ones that run assets, ship these
libraries. The api and mcp images only describe the destination and never open
a session, so they leave them out.

Without them the package imports fine (the catalog, the connection check and
the workspace picker all work), but opening a warehouse session fails with
`Failed to load the driver`. `mssql-python` also offers an alternate, Rust-based
native provider that needs none of these libraries: install `mssql-python-rs`
and set `MSSQL_PYTHON_NATIVE_PROVIDER=mssql-odbc`. Microsoft labels that
provider alpha.

## Datasets are schemas

An asset's `dataset` is the warehouse schema its table lives in. An asset
without a dataset falls back to `default_dataset`, then to `dbo`. A missing
schema is created on the first write, and a missing table is created typed
from the asset's schema (or one inferred from the data):

| Field type | Fabric Warehouse type |
|------------|-----------------------|
| `bool` | `bit` |
| `int` | `bigint` |
| `float` | `float` |
| `Decimal` | `decimal(38,9)` |
| `datetime` | `datetime2(6)` |
| `date` | `date` |
| `bytes` | `varbinary(max)` |
| `str`, `Any` | `varchar(max)` |
| nested models, lists, dicts | `varchar(max)` holding JSON |

An existing table is never altered: a column the data carries but the table
does not is dropped with a warning. Reads return records; nested values come
back as the JSON text they are stored as. Identifiers are bracketed
(`[marts].[ads_stats]`), and the warehouse's default collation is
case-sensitive, so a table is queried with the exact case the asset gives it.

## How Fabric's T-SQL shaped the design

Fabric Warehouse speaks T-SQL over the SQL Server protocol, but it is not SQL
Server. The differences that matter here:

- **Types.** Tables accept a subset of SQL Server's types: `datetime2` and
  `time` keep at most six fractional digits, and there is no `datetime`,
  `datetimeoffset`, `nvarchar`, `nchar`, `tinyint`, `money` or `json`. Hence
  `varchar` (UTF-8) for text and JSON, and `datetime2(6)` for timestamps;
  timezone-aware values are written as UTC. `varchar(max)` and
  `varbinary(max)` exist but hold at most 16 MB per value. Microsoft
  recommends the shortest fitting `varchar(n)` for query performance;
  `varchar(max)` is used because the destination cannot know a column's
  longest value up front.
- **Schemas.** There is no `CREATE SCHEMA IF NOT EXISTS`, and `CREATE SCHEMA`
  must be alone in its batch, so the destination checks `sys.schemas` first
  and creates the schema in a statement of its own. Table existence is read
  from `sys.tables` and `sys.columns`; the warehouse refuses a query mixing
  system and user tables, so the catalog check never touches the data.
- **Transactions.** Explicit `BEGIN TRANSACTION ... COMMIT TRANSACTION` is
  supported, always under snapshot isolation, and DDL is allowed inside one.
  DDL in a transaction holds locks on the catalog views until commit, though,
  blocking every other write's existence checks, so a new schema and table
  are created and committed before the write's transaction opens; inside it
  run only the `DELETE` and the `INSERT`s.
- **Whole-table replaces delete rather than truncate.** `TRUNCATE TABLE`
  takes a schema-modification lock that blocks readers until commit; a
  `DELETE` does not.
- **Write-write conflicts are per table.** Two transactions deleting from the
  same table conflict even when they touch different rows: the first to commit
  wins and the other fails with error 24556 or 24706. Writes to different
  tables never conflict. Concurrent writes of different partitions of one
  asset (overlapping backfills, for instance) should be avoided, or covered by
  a retry policy.

## Partitions

A partitioned write replaces the partition's rows: it deletes them, then
inserts the data, inside one transaction, rolled back on failure. A time
partition deletes by half-open bounds (`[day] >= ? AND [day] < ?`), so a
monthly partition whose rows hold daily dates is replaced whole; any other
partition deletes by equality on its id. A window deletes each partition it
covers and inserts the whole batch once. An unpartitioned asset deletes every
row and reloads the table. Readers keep seeing the previous rows until the
commit.

## The load path and its limits

Rows are written with parameterised multi-row `INSERT ... VALUES` statements.
Each statement carries as many rows as fit under SQL Server's 2100-parameter
cap (2099 parameters, one per value) and 1000 rows, whichever is smaller: a
5-column table loads 419 rows per statement, a 40-column table 52. Every
statement is a round trip and writes new Parquet files under the table, which
the warehouse compacts in the background.

This needs no infrastructure beyond the warehouse and suits incremental loads:
thousands to low hundreds of thousands of rows per write. Microsoft recommends
against frequent small inserts, and for bulk volumes (millions of rows per
write, or a full reload of a large table) the documented high-throughput path
is `COPY INTO` from files staged in OneLake or Azure Data Lake Storage, which
this destination does not implement.
