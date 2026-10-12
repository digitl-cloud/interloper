# Views and references

Follows the star schema placeholders (0.105.0, 0.106.0): seven `*_star_schema` sources, the campaign
matcher on `*.dim_campaigns` and the campaign performance analysis shell, all wired in production and
materializing nothing. This is the design that makes them real.

## Problem

A star schema is a reshaping of data interloper already holds: the union of every account's report,
the latest snapshot of every campaign. Nothing new is fetched. Building it the way connectors are
built, reading each upstream one partition at a time into Python and writing rows back, forces the
model into the connector shape: dimensions become daily snapshots, one failed account cancels the
whole day, every backfill re-runs the model, and the data is stored twice.

The warehouse already does this well. A star schema should be **views** over the raw tables, and the
framework should know two more things an asset can be: a view, whose definition is materialized
instead of its data, and a reference, a table interloper did not produce but can read.

Consumers: client analysts querying the warehouse, and downstream Python assets (the matcher, the
campaign performance analysis) reading through interloper. Portability is across SQL warehouses only.

## Decisions

### 1. Three kinds of asset, one concept

Everything an asset has (lineage, relations, schema, catalog, the read path) is shared. What a run
does differs.

| | Asset (today) | View | Reference |
|---|---|---|---|
| Body | `data()` returns rows | `data()` returns a view definition | none |
| A run | conforms and writes | creates or replaces the view | nothing: the table exists |
| `upstream.data` | read from the destination | the view destination's reference to the upstream | n/a |
| Example | connectors | star schemas | the GA4 BigQuery export |

`View` and `Reference` subclass `Asset` and keep kind `asset`. Nothing in the store, the API, the
toolkit or the app distinguishes them from an asset; the catalog definition carries the class, as it
does for every component.

### 2. A view materializes a definition, and its destination says what that is

A view has exactly **one** declared destination, like any asset. Core adds two hooks to
`Destination`, both refusing by default:

- `reference(asset) -> Any`: this destination's handle on an asset's data, for a view to build on.
- `create_view(asset, definition) -> None`: materialize a definition.

A view's run builds each upstream as `destination.reference(upstream_asset)`, calls `data()` with
them, and passes the result to `destination.create_view(self, definition)`. Core never interprets a
reference or a definition; it is the same move as `upstream.data` being "whatever representation the
destination holds natively" for a data asset. Object stores, files, Sheets and Memory keep the
refusals, so a view on them is a configuration error.

Upstreams are plain assets under today's rules. A view uses what they already are (where they live,
their schema) and never checks, alters or reads them. When the query does not fit the data (a column
one account's table lacks), the warehouse rejects the `CREATE VIEW` and the view's run fails, like a
Python asset failing on a missing field.

### 3. The SQL family

`DatabaseDestination` implements both hooks for every SQL warehouse:

- A reference is a `TableReference(name, schema)`: the qualified table name, quoted by the
  destination, and the asset's schema class (declared, or `None`).
- A definition is SQL text in the destination's `dialect`, or any object with
  `compile(dialect: str) -> str`, which `create_view` calls before issuing the DDL. A raw-SQL view
  composes its text from the references it receives (`reference.name` in an f-string); for a
  many-valued relation the author writes the `UNION ALL` over the list, so heterogeneous upstreams
  need no special rule.
- Backends add `qualified_name(table, dataset)`, `replace_view(table, dataset, sql)`, a `dialect`
  class attribute, and `reaches(other: Destination) -> bool`: whether a view in this destination can
  query that one's tables, decided from configuration alone (same warehouse and location, same
  account, same database file). `reference` raises `ConfigError` when `reaches` is false.

An upstream with several destinations is referenced through its `default_destination_key` one, else
the first the view's destination reaches, the same preference the read path has.

This cycle implements BigQuery (production) and DuckDB (tests). The other warehouses refuse views
until they get the three backend methods, a small change each.

### 4. Validation

Offline, at graph build and manifest load, in `View.validate_relations`: every bound upstream is
reachable from the view's destination; a raw-SQL view's dialect matches its destination's; the view
has one destination. Violations are a `ConfigError` naming the upstream. Bindings are accepted at bind
time as today, since destinations can be attached after bindings or arrive through a job; the app
shows an unreachable upstream as a status on the view, the way catalog drift is shown.

Everything else is the warehouse's verdict at run time, reported as the view's failure.

### 5. Views read no upstream data

`Asset.reads_upstream_data` is a class attribute, `True` for assets, `False` for views and
references. `DAG._check_partition_dependencies` applies the rule "a partitioned asset is never
upstream of an unpartitioned one" only to edges into assets that read upstream data. The rule stays
as it is for data assets: it exists because a run has one partition scope, so an unpartitioned
consumer reading a partitioned upstream would silently read one day and replace its whole table. A
view reads nothing; the warehouse resolves the query over the whole table at query time.

A view is unpartitioned and is **recreated on every run**: `CREATE OR REPLACE VIEW` is cheap and
idempotent, and the view then matches the wiring as of the last run. A star schema sits in a job
after its connectors.

### 6. References

`il.reference` points at a table in a destination and knows how to read it. The location is
`table` and `dataset`, formatted with the owning source's fields (`dataset="analytics_{property_id}"`,
`table="events_*"`). Its schema is declared, or omitted and discovered from the destination on first
read. Nothing is ever created or written, and nothing checks the table against a declared schema:
a run touches no reference, so a mismatch surfaces where the table is used, from a consumer's read
or a view's creation.

### 7. Portable authoring: `sql_view` in `interloper-sql`

`il.view` takes raw SQL and is tied to its dialect. `interloper_sql.sql_view` is the portable layer:
it turns each `TableReference` into an `ibis.Table` from its name and schema (a reference whose
schema is `None` is refused: there is nothing to type the table from), hands `data()` the tables, takes
an Ibis expression back, checks its columns and types against the view's declared schema, and wraps it
in `compile(dialect)`. A many-valued relation hands over a list the author unions; `union(upstreams,
**per_upstream_columns)` covers the common case and stamps per-upstream columns such as the account.

Ibis is a dependency of `interloper-sql` only. Warehouse packages import nothing new; only modules
that define `sql_view`s import `interloper_sql`. A spike precedes the implementation: safe casts and
the latest-row-per-id pattern compile and run correctly on DuckDB and as a BigQuery dry run; the
install footprint of `ibis-framework` without any backend; a raw-SQL escape hatch on unbound tables.

`interloper-sql`'s SQLAlchemy and psycopg stay as they are; moving them behind an extra is out of
scope.

### 8. Scope of this cycle

1. Core: `View`, `Reference`, `il.view`, `il.reference`, the two `Destination` hooks, the
   `DatabaseDestination` implementation, the DAG exemption, `View.validate_relations`.
2. BigQuery and DuckDB backends.
3. The Facebook star schema as raw BigQuery SQL `il.view`s, replacing its placeholder:
   `fact_ad_performance` over every account's `ads_stats`, `dim_campaigns`, `dim_ads`,
   `dim_accounts`, each with its schema class under `schemas/`.
4. A GA4 export source: `il.reference` to `events_*` and a raw-SQL star schema over it, the
   showcase for references and for a view tied to one warehouse.
5. `sql_view` in `interloper-sql`, after the spike, exercised on its own test model. The Facebook
   star schema stays raw SQL for now: porting it would make `interloper-assets` depend on
   `interloper-sql`, and with it on SQLAlchemy, psycopg and Ibis, in every image that ships the
   assets. The port waits for `interloper-sql`'s Postgres driver to move behind an extra.

The other platforms' star schemas, the other warehouses' backends, and real logic for the matcher
and the campaign performance analysis follow the same pattern afterwards.

## Not designed here

- **Partitioned reads of a view.** A partitioned Python consumer of a fact view (the campaign
  performance analysis) would read the whole history each run. The matcher reading `dim_campaigns`
  is the first real case; the read policy is designed on it.
- **Drift of upstream tables.** A dormant account's table that predates a widened schema fails a
  view that selects the new columns. That is a migration of that table, not a view concern.
- **GA4 shard pruning** through views over `events_*`, and partition-templated reference tables for
  Python reads.
- **Recreating a view when its wiring changes** outside a run.

## Verification

- Core: both hooks refuse by default; `TableReference` quoting per backend; `reaches`; the DAG
  exemption with a partitioned upstream and an unpartitioned view; `validate_relations` errors for an
  unreachable upstream, a dialect mismatch and two destinations; a reference formatting its location
  from source fields; the run of a view calls `create_view` with the result of `data()` and reads
  nothing. Memory doubles throughout; no warehouse.
- DuckDB: every Facebook view created over fixture tables for two accounts, including an account with
  no table (the warehouse's own error, reported on the view), and read back through the read path.
- `interloper-sql`: the schema check; every `sql_view` compiled for every dialect Ibis names;
  the Facebook views run on DuckDB.
- Live: the Facebook star schema created in the Swarovski dev project on BigQuery over two accounts,
  queried by hand.
