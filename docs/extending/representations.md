# Representations

The core never names a concrete table library. A **representation** is everything core needs to
know about one table type: what it is, how to view it generically, how to slice it by partition,
and how to check it against a schema. The core ships the `rows` representation (`list[dict]`);
`interloper-pandas` ships `dataframe`. Adding a third (polars, Arrow) is one class and one entry
point.

## What a representation provides

```py
from interloper.representation import Representation

class ArrowRepresentation(Representation):
    key = "arrow"

    def matches(self, data) -> bool: ...                       # is this an Arrow table?
    def to_rows(self, data) -> list[dict]: ...                 # the data as rows, missing as None
    def from_rows(self, rows) -> pa.Table: ...                 # build from rows
    def columns(self, data) -> list[str]: ...                  # [] when not discoverable
    def filter_eq(self, data, column, value): ...              # rows where column == value (as strings)
    def filter_range(self, data, column, start, end): ...      # rows where start <= column < end (ISO labels)
    def reconcile(self, data, schema, *, strict=False): ...    # the data in the schema's types; strict refuses mismatches
    def infer(self, data) -> type[il.Schema]: ...              # a Schema from the data
    def concat(self, tables) -> pa.Table: ...                  # optional: several tables as one, natively
```

`filter_eq` and `filter_range` are how partitions slice data on write; `filter_range` compares
values as ISO-8601 strings (`iso_label()`), which is what lets a date compare against a
datetime and keeps half-open bounds exact. `to_rows` and `from_rows` are the conversion
protocol: rows are the hub every representation converts through. `concat` is how a windowed
read comes back as one table; it joins through rows unless a representation overrides it with
its library's own join. `reconcile` and `infer`
are what the [conform step](../guide/schema.md#the-conform-step) calls: `reconcile(strict=True)`
under `STRICT`, `reconcile` under `RECONCILE`, `infer` when no schema is declared; `il.Schema.field_specs()` gives the type contract to map onto the
library's dtypes, and the pandas implementation vectorizes them over columns.

Representations are stateless, never serialized and not user-configurable.

## Viewing and converting data

`TableData.from_data(data)` resolves the representation matching the data and binds it to the
data, so a caller never names the data twice:

```py
from interloper.representation import TableData

table = TableData.from_data(data)
table.key                      # "rows" or "dataframe"
table.rows                     # list[dict], missing values as None
table.columns                  # column names, [] when not discoverable
table.filter_eq("id", 3)       # the partition filters, in the data's own type
table.reconcile(schema)        # the schema operations, on the data's own type
table.reconcile(schema, strict=True)
table.infer()
table.to("dataframe")          # the data in any registered representation
```

`to(key)` returns the data itself when it is already in the target representation, and otherwise
rebuilds it from rows (`target.from_rows(table.rows)`). A representation that is
registered converts to and from every other one with no pairwise code, and an unknown key fails
with the registry's error naming the registered keys. A destination that loads one type natively
converts everything to it in one line: BigQuery does `TableData.from_data(data).to("dataframe")`
and has a single load path.

## Registering

```toml
[project.entry-points."interloper.representations"]
arrow = "my_package.arrow:ARROW_REPRESENTATION"
```

The entry may point at an instance or a class. The registry keys it by the representation's own
`key`. `TableData.from_data(data)` binds the first registered representation whose `matches`
accepts the data, and raises `RepresentationError` naming the type and the registered keys when
none does. Nothing falls back silently: the shapes an asset may return besides a table
(generators, models, a lone dict) are unwrapped by the asset before the data reaches a
representation.

## Where it is used

- Conform binds the data through `TableData.from_data(result)` and calls `reconcile`
  or `infer` on it, once per materialization.
- `Partition.slice()` and `TimePartition.slice()` filter through the representation, so window
  writes split correctly for any table type.
- Destinations that store rows read `TableData.from_data(data).rows`; one that loads a
  single type natively converts with `.to(key)`. A destination reads back in the representation
  it holds natively (a DataFrame from BigQuery, rows from a file), and `il.Upstream.rows`
  converts for the consumer that wants rows. A producer never chooses a representation on a
  consumer's behalf.
