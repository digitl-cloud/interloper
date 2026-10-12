# Views & references

Two kinds of asset besides the one that writes rows. A **view** materializes its definition: its
destination creates a warehouse view from what `data()` returns. A **reference** materializes
nothing: it points at a table that already exists and reads it. Both are assets in every other
respect, with lineage, relations, a schema and a place in the catalog.

| | Asset | View | Reference |
|---|---|---|---|
| `data()` returns | rows | a view definition | nothing to return |
| A run | conforms and writes | creates or replaces the view | does nothing |
| `upstream.data` | the data read | the destination's reference to the upstream | |

## Views

```py
@il.source
class Reporting(il.Source):
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(self, orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)
```

A view has exactly one destination, declared like any asset's. Each upstream parameter receives
the destination's reference to that upstream in `data`: on a SQL warehouse a `TableReference`,
whose `name` is the qualified table, quoted for the dialect. The function composes the definition
from those references; a many-valued relation is a list, and the author writes the `UNION ALL`.

A raw-SQL view is written in one dialect. `dialect="bigquery"` pins it to BigQuery destinations;
the default, `None`, is the author's assertion that the SQL is portable, and it is checked against
no dialect.

A view is unpartitioned and reads no upstream data, so it may sit on partitioned upstreams: it
refers to the whole table, and the warehouse resolves the query. It is recreated on every run,
cheaply and idempotently, so it follows the accounts bound to it. Put it in a job after its
connectors.

What the graph checks, when it is built: the view has one destination; its dialect, when declared,
is the destination's; every bound upstream is reachable from that destination (`reaches`, decided
from configuration: same BigQuery location, same DuckDB database). Anything the query gets wrong is
the warehouse's verdict when the view is created, reported as the view's failure.

### Portable views

`interloper-sql` adds `sql_view`: each upstream's `data` is an `ibis.Table` typed from the
upstream's declared schema, the function returns an Ibis expression, and the destination compiles it
for its own dialect. The expression's columns must match the view's declared schema. The base package
depends on Ibis only; the SQLAlchemy destination lives behind `interloper-sql[postgres]`, so a package
that defines `sql_view`s pulls in no database driver.

```py
from interloper_sql import sql_view, union

@sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
def totals(self, orders: list[il.Upstream]) -> ibis.Table:
    rows = union(orders, account_id=lambda upstream: upstream.asset.source.discriminator)
    return rows.group_by([rows.date, rows.account_id]).aggregate(total=rows.total.sum())
```

Every upstream of a `sql_view` must declare a schema: that is what types its table.

## References

```py
@il.source
class GoogleAnalytics4Export(il.Source):
    property_id: str = il.InputField(discriminator=True)

    events = il.reference("events_*", schema=Events)
```

A reference names its table as written, wildcard and all; the dataset is the asset's, the source's
by default, and a source whose tables vary per instance sets them in code, as the GA4 export points
`events` at `analytics_<property id>` from its property id. Nothing is created or written.
Downstream assets read it like any asset, and views build on it like any table.

A source whose tables exist in one warehouse only says so with a narrowed `destinations` relation,
checked when a destination is bound: the GA4 export declares
`relations={"destinations": il.Relation("destination", "bigquery_destination", many=True, optional=True)}`
on its `@il.source`, so the app offers it BigQuery destinations only.

## Backends

| Destination | Views |
|---|---|
| BigQuery | yes; `reaches` the same location |
| DuckDB | yes; `reaches` the same database |
| Snowflake, ClickHouse, Databricks, Fabric, SQL | not yet: `table_parts`, `replace_view` and `reaches` to implement |
| GCS, S3, CSV, files, Sheets, Memory | no: a view there is a configuration error |
