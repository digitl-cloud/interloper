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
datetime and keeps half-open bounds exact. `to_records` and `from_records` are what
destinations use when they store records, and what `DatabaseDestination` uses to materialize
reads into `read_representation`.

Representations are stateless, never serialized and not user-configurable.

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
- `PartitionedDestination` and `DatabaseDestination` convert through `to_records` and
  `from_records`.
- `DatabaseDestination.read_representation` and `@destination(read_representation=...)` name
  the representation reads should materialize into.
