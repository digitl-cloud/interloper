# interloper-databricks

Databricks integration for interloper: a `DatabricksDestination` that stores
assets as Delta tables in Unity Catalog through a SQL warehouse, and the
`DatabricksConnection` that holds the workspace credentials.

The destination is a `DatabaseDestination`: the rows a partition covers come
from core, exactly as for BigQuery. What differs is how they are replaced (see
[Partitions](#partitions)).

## Setup

### Credentials

A service principal with an OAuth secret is the recommended credential; a
personal access token works too. The connection takes exactly one of them.

1. In the account console (or the workspace admin settings), create a service
   principal and add it to the workspace.
2. Under its **Secrets** tab, click **Generate secret**. Note the client ID
   and the secret; the secret is shown once. The connection asks for the
   `all-apis` scope, so leave the secret unscoped (or select all APIs): a
   secret restricted to some scopes cannot issue that token.
3. Give the service principal the **Can use** permission on the SQL warehouse
   the destination loads through.

The connection exchanges the client ID and secret for a one-hour access token
at the workspace's `/oidc/v1/token` endpoint (client credentials grant), and
replaces the token before it expires. The connection check calls
`/api/2.0/preview/scim/v2/Me`, and the pickers list SQL warehouses and Unity
Catalog catalogs over the REST API, all with that token.

### Grants

The principal needs, in Unity Catalog:

- `USE CATALOG` on the destination's catalog
- `CREATE SCHEMA` on the catalog, or `USE SCHEMA` on every schema the assets
  write to if you create them yourself
- `CREATE TABLE` on those schemas
- `MODIFY` and `SELECT` on the tables (a table the principal creates is its
  own, so it has both)
- `USE SCHEMA`, `READ VOLUME` and `WRITE VOLUME` on the staging volume and its
  schema

A sketch, with the service principal's application ID as the grantee:

```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG analytics TO `<application-id>`;
GRANT USE SCHEMA ON SCHEMA analytics.staging TO `<application-id>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME analytics.staging.loads TO `<application-id>`;
```

### Staging volume

Every load uploads one Parquet file to a Unity Catalog volume, reads it from
there and removes it. Create a managed volume for it once:

```sql
CREATE SCHEMA IF NOT EXISTS analytics.staging;
CREATE VOLUME IF NOT EXISTS analytics.staging.loads COMMENT 'interloper load staging';
```

and name it on the destination as `analytics.staging.loads`. Files go under
`/Volumes/analytics/staging/loads/interloper/`, one per write. A file whose
removal fails (the load's outcome stands, with a warning) is left there and
can be deleted by hand.

## Usage

```python
import interloper as il
from interloper_databricks import DatabricksConnection, DatabricksDestination

destination = DatabricksDestination(
    connection=DatabricksConnection(
        host="https://dbc-a1b2345c-d6e7.cloud.databricks.com",
        client_id="...",
        client_secret="...",
    ),
    warehouse="/sql/1.0/warehouses/a1b234c567d8e9fa",
    catalog="analytics",
    staging_volume="analytics.staging.loads",
    default_dataset="raw",
)
```

`warehouse` is the warehouse's HTTP path (its **Connection details** tab). The
credentials also load from the environment (`DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, or
`DATABRICKS_ACCESS_TOKEN` for a personal access token; note that Databricks'
own tools name the token `DATABRICKS_TOKEN`).

In a deployed instance you configure this through the UI instead: add a
Databricks connection, then a Databricks destination, picking the warehouse
and the catalog from the lists the connection can see.

## Datasets are schemas

An asset's `dataset` is the schema its table lives in, inside the
destination's `catalog`. An asset without a dataset falls back to
`default_dataset`; with neither, the write fails with a `ConfigError`. A
missing schema is created on the first write, and a missing table is created
as a Delta table typed from the asset's schema (or one inferred from the
data), with field descriptions as column comments and the asset's description
as the table comment:

| Field type | Databricks type |
|------------|-----------------|
| `bool` | `BOOLEAN` |
| `int` | `BIGINT` |
| `float` | `DOUBLE` |
| `Decimal` | `DECIMAL(38,9)` |
| `datetime` | `TIMESTAMP` |
| `date` | `DATE` |
| `bytes` | `BINARY` |
| `str`, `Any` | `STRING` |
| nested models, lists, dicts | `VARIANT` |

A `datetime` is a `TIMESTAMP`, an absolute instant; a naive datetime is taken
as UTC. `VARIANT` columns are queried with the path operator
(`SELECT payload:campaign.id FROM ...`); creating a table with one enables
Delta's `variantType` feature, which readers need Databricks Runtime 15.4 or a
recent Delta client for.

Identifiers are quoted with backticks. Unity Catalog stores schema and table
names in lower case and keeps column names as written; queries match either
case-insensitively.

An existing table is never altered: a column the data carries but the schema
does not is dropped with a warning.

A partitioned asset's table is clustered on its partition column with liquid
clustering (`CLUSTER BY`), which is what Databricks recommends over
partitioning for tables under 1 TB, when that column is one liquid clustering
accepts as a key (a date, timestamp, string or number among the first 32
columns). Tables are never `PARTITIONED BY`.

## Partitions

Each write is one statement, so a partition is replaced atomically:

- a time partition:
  ``INSERT INTO ... BY NAME REPLACE WHERE `day` >= DATE'2024-01-01' AND `day` < DATE'2024-01-02' SELECT ...``
  (half-open bounds, so a monthly partition whose rows hold daily dates is
  replaced whole)
- any other partition: ``REPLACE WHERE `region` = 'eu'``
- a window: one predicate covering every partition in it, a single range from
  its first start to its last end when the partitions are contiguous
- an unpartitioned asset: `INSERT OVERWRITE ... BY NAME SELECT ...`

The `SELECT` reads the staged file with `read_files(..., format => 'parquet')`,
casting each column to the table's type. `REPLACE WHERE` deletes the matching
rows and inserts the new ones in a single Delta commit, so a failed load
leaves the partition's old rows in place.

`REPLACE WHERE` is strict: every row written must match the predicate, or the
statement fails with `DELTA_REPLACE_WHERE_MISMATCH` and writes nothing. A row
whose partition column is null, or falls outside the partition, fails the
write instead of landing where the next replace would not remove it.

Reads come back as a DataFrame built from the result's Arrow batches.

## Notes

Each destination opens its own SQL session through the connection, on its own
warehouse and catalog, so destinations sharing a connection never affect each
other. That session is shared by every asset the destination writes, and the
connector's sessions are not thread-safe, so the destination runs one
statement at a time and holds a write from its upload to its cleanup.

The SQL warehouse does the loading work: it reads the staged file, casts it
and rewrites the replaced rows, so its size bounds load throughput. Since a
destination runs one statement at a time, size the warehouse for the largest
single write (a partition, or a whole window) rather than for concurrency;
several destinations, or several runs, can share a warehouse that scales out.
A stopped warehouse adds its start-up time to the first write after it
auto-stops, which serverless warehouses keep short.
