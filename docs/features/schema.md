# Schema & Normalizer

## Normalizer

The `Normalizer` coerces arbitrary asset return types into `list[dict]` and applies optional
transformations.

### Supported input types

The normalizer accepts: `dict`, `list[dict]`, `BaseModel`, `list[BaseModel]`, `Generator`, and
(with `interloper-pandas` installed) `pandas.DataFrame`.

### Usage

```py
import interloper as il

@il.asset(normalizer=il.Normalizer())
def my_asset():
    return [{"UserName": "alice", "Address": {"City": "NYC"}}]
```

With column normalization on and flattening enabled, the data becomes:

```py
[{"user_name": "alice", "address_city": "NYC"}]
```

### Options

```py
il.Normalizer(
    normalize_columns_names=True,   # Convert column names to snake_case
    flatten_max_level=0,            # 0=disabled, None=unlimited, N=N levels deep
    flatten_separator="_",          # Separator for flattened keys
    fill_missing=True,              # Fill missing keys with None across rows
    drop_na_columns=False,          # Drop columns that are entirely None
    snake_case_digits=False,        # "acosClicks14d" -> "acos_clicks_14d"
    replace_empty_dicts=False,      # {} -> None
    replace_empty_strings=False,    # "" -> None
    column_overrides={},            # Raw name -> normalized name
)
```

The normalizer can be set at the **source level** (applies to all assets) or at the **asset
level** (overrides the source-level one):

```py
@il.source(normalizer=il.Normalizer())
def my_source():
    @il.asset  # inherits the source normalizer
    def asset_a():
        ...

    @il.asset(normalizer=il.Normalizer(flatten_max_level=None))  # overrides
    def asset_b():
        ...

    return [asset_a, asset_b]
```

### pandas

The `interloper-pandas` package provides `DataFrameNormalizer`, a `Normalizer` subclass that
operates on `pandas.DataFrame` natively (vectorized) and returns a DataFrame. Assets can return
DataFrames anywhere — the framework detects the representation and conforms it the same way as
row dicts.

```py
from interloper_pandas import DataFrameNormalizer

@il.asset(normalizer=DataFrameNormalizer(flatten_max_level=1))
def my_asset():
    return pd.DataFrame(...)
```

## Schema

A schema declares the expected output structure of an asset. Define one by subclassing
`il.Schema` (a Pydantic `BaseModel`) or by decorating a plain class with `@il.schema`:

```py
class UserSchema(il.Schema):
    id: int
    name: str
    email: str | None = None
```

```py
@il.schema
class UserSchema:
    id: int
    name: str
    email: str | None = None
```

Both forms are equivalent. Attach a schema to an asset with `schema=`:

```py
@il.asset(schema=UserSchema, normalizer=il.Normalizer())
def users():
    return [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
```

Schemas are backend-agnostic: integration packages translate them into native type systems
(BigQuery fields, pandas dtypes, …) via the schema's field specs.

## Materialization strategy

The `MaterializationStrategy` controls how schemas are enforced. Set it via the
`materialization_strategy=` option on `@asset` or `@source`:

### AUTO (default)

If a schema is provided, validates data against it without coercion. If none is provided, a
schema is inferred from the data (best-effort).

```py
@il.asset(materialization_strategy=il.MaterializationStrategy.AUTO)
def my_asset():
    ...
```

### STRICT

Requires a schema. Validates data and **fails** on any mismatch (extra fields, missing required
fields, wrong types).

```py
@il.asset(
    schema=UserSchema,
    materialization_strategy=il.MaterializationStrategy.STRICT,
)
def my_asset():
    ...
```

### RECONCILE

Requires a schema. Aligns columns to the schema (drops extras, adds missing as `None`) and
coerces values to the schema's types.

```py
@il.asset(
    schema=UserSchema,
    materialization_strategy=il.MaterializationStrategy.RECONCILE,
)
def my_asset():
    ...
```

## Schema inference

Under the `AUTO` strategy with no declared schema, a schema is inferred from the data: each key
becomes an optional field whose type is widened across observed rows (e.g. `int`+`float` →
`float`). The inferred schema is attached to the asset for downstream use (e.g. typed
destination loads).
