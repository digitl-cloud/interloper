# Representations & conformers

The core never names a concrete table library. A **representation** answers "what kind of table
is this, and how do I view it generically" for one data type, and bundles the **conformer** that
implements schema operations on that type. The core ships the `rows` representation
(`list[dict]`); `interloper-pandas` ships `dataframe`. Adding a third (polars, Arrow) is the
same recipe.

## What a representation provides

```py
from interloper.representation import Representation
from interloper.conformer import Conformer

class ArrowRepresentation(Representation):
    key = "arrow"

    def matches(self, data) -> bool: ...                       # is this an Arrow table?
    def to_records(self, data) -> list[dict]: ...              # view as rows, missing as None
    def from_records(self, rows) -> pa.Table: ...              # build from rows
    def columns(self, data) -> list[str]: ...                  # [] when not discoverable
    def filter_eq(self, data, column, value): ...              # rows where column == value (as strings)
    def filter_range(self, data, column, start, end): ...      # rows where start <= column < end (ISO labels)

    @property
    def conformer(self) -> Conformer:
        return ARROW_CONFORMER
```

`filter_eq` and `filter_range` are how partitions slice data on write; `filter_range` compares
values as ISO-8601 strings (`iso_label()`), which is what lets a date compare against a
datetime and keeps half-open bounds exact. `to_records` and `from_records` are the conversion
protocol: records are the hub every representation converts through.

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
view.conformer                 # the schema operations for this representation
view.filter_eq("id", 3)        # the partition filters, in the data's own type
view.to("dataframe")           # the data in any registered representation
```

`to(key)` returns the data itself when it is already in the target representation, and otherwise
rebuilds it from records (`target.from_records(view.records)`). A representation that is
registered converts to and from every other one with no pairwise code, and an unknown key fails
with the registry's error naming the registered keys. A destination that loads one type natively
converts everything to it in one line: BigQuery does `Representation.of(data).to("dataframe")`
and has a single load path.

## What a conformer provides

```py
class ArrowConformer(Conformer):
    def prepare(self, data): ...                          # canonicalize raw output; NormalizerError if not tabular
    def validate(self, data, schema, *, strict=False): ...  # SchemaError on mismatch
    def reconcile(self, data, schema): ...                # align columns, coerce values
    def infer(self, data) -> type[il.Schema]: ...         # a Schema from the data
```

The [conform step](../guide/schema.md#the-conform-step) calls `prepare` once, then `validate`,
`reconcile` or `infer` depending on the materialization strategy. `il.Schema.field_specs()`
gives the type contract to map onto the library's dtypes.

## Registering

```toml
[project.entry-points."interloper.representations"]
arrow = "my_package.arrow:ARROW_REPRESENTATION"
```

The entry may point at an instance or a class. The registry keys it by the representation's own
`key`. `Representation.of(data)` checks every non-rows representation first and falls back to
rows, whose record coercion rejects non-tabular data with a clear error.

## Where it is used

- Conform resolves the conformer through `Representation.of(result)`.
- `Partition.slice()` and `TimePartition.slice()` filter through the representation, so window
  writes split correctly for any table type.
- Destinations that store records read `Representation.of(data).records`; one that loads a
  single type natively converts with `.to(key)`. A destination reads back in the representation
  it holds natively (a DataFrame from BigQuery, rows from a file), and `il.Upstream.records`
  converts for the consumer that wants rows. A producer never chooses a representation on a
  consumer's behalf.
