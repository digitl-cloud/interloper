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
    def to_records(self, data) -> list[dict]: ...              # view as rows, missing as None
    def from_records(self, rows) -> pa.Table: ...              # build from rows
    def columns(self, data) -> list[str]: ...                  # [] when not discoverable
    def filter_eq(self, data, column, value): ...              # rows where column == value (as strings)
    def filter_range(self, data, column, start, end): ...      # rows where start <= column < end (ISO labels)
    def reconcile(self, data, schema, *, strict=False): ...    # the data in the schema's types; strict refuses mismatches
    def infer(self, data) -> type[il.Schema]: ...              # a Schema from the data
```

`filter_eq` and `filter_range` are how partitions slice data on write; `filter_range` compares
values as ISO-8601 strings (`iso_label()`), which is what lets a date compare against a
datetime and keeps half-open bounds exact. `to_records` and `from_records` are the conversion
protocol: records are the hub every representation converts through. `reconcile` and `infer`
are what the [conform step](../guide/schema.md#the-conform-step) calls: `reconcile(strict=True)`
under `STRICT`, `reconcile` under `RECONCILE`, `infer` when no schema is declared; `il.Schema.field_specs()` gives the type contract to map onto the
library's dtypes, and the pandas implementation vectorizes them over columns.

Representations are stateless, never serialized and not user-configurable.

## Viewing and converting data

`Representation.of(data)` resolves the representation matching the data and binds it to the data
as a `View`, so a caller never names the data twice:

```py
from interloper.representation import Representation

view = Representation.of(data)
view.key                       # "rows" or "dataframe"
view.records                   # list[dict], missing values as None
view.columns                   # column names, [] when not discoverable
view.filter_eq("id", 3)        # the partition filters, in the data's own type
view.reconcile(schema)         # the schema operations, on the data's own type
view.reconcile(schema, strict=True)
view.infer()
view.to("dataframe")           # the data in any registered representation
```

`to(key)` returns the data itself when it is already in the target representation, and otherwise
rebuilds it from records (`target.from_records(view.records)`). A representation that is
registered converts to and from every other one with no pairwise code, and an unknown key fails
with the registry's error naming the registered keys. A destination that loads one type natively
converts everything to it in one line: BigQuery does `Representation.of(data).to("dataframe")`
and has a single load path.

## Registering

```toml
[project.entry-points."interloper.representations"]
arrow = "my_package.arrow:ARROW_REPRESENTATION"
```

The entry may point at an instance or a class. The registry keys it by the representation's own
`key`. `Representation.of(data)` returns the first registered representation whose `matches`
accepts the data, and raises `RepresentationError` naming the type and the registered keys when
none does. Nothing falls back silently: the shapes an asset may return besides a table
(generators, models, a lone dict) are unwrapped by the asset before the data reaches a
representation.

## Where it is used

- Conform resolves the data's view through `Representation.of(result)` and calls `reconcile`
  or `infer` on it, once per materialization.
- `Partition.slice()` and `TimePartition.slice()` filter through the representation, so window
  writes split correctly for any table type.
- Destinations that store records read `Representation.of(data).records`; one that loads a
  single type natively converts with `.to(key)`. A destination reads back in the representation
  it holds natively (a DataFrame from BigQuery, rows from a file), and `il.Upstream.records`
  converts for the consumer that wants rows. A producer never chooses a representation on a
  consumer's behalf.
