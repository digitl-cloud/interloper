# Schemas & data contracts

A schema declares the shape of an asset's output. Declared or inferred, an **effective schema**
exists for every tabular materialization: it is the contract the data is checked against and
the metadata destinations use for typed loads.

## Declaring a schema

Subclass `il.Schema` or decorate a plain class with `@il.schema`:

```py
import datetime as dt

import interloper as il
from pydantic import Field

class AdsStats(il.Schema):
    date: dt.date
    ad_id: str
    impressions: int
    clicks: int
    spend: float = Field(description="Spend in account currency")
    labels: list[str] | None = None
```

- `T | None` marks a nullable column.
- `list[T]` marks a repeated column.
- A nested `BaseModel` marks a record column with sub-fields.
- Columns may be called `name` or `key`; they stay in declaration order.

Attach it with `schema=` on the asset. `AdsStats.field_specs()` returns backend-agnostic
`FieldSpec` entries (`name`, `type`, `nullable`, `repeated`, `fields`, `description`) that
integrations map to their native type systems; `AdsStats.json_schema()` is the JSON Schema of
the data columns.

## Materialization strategy

The strategy decides how strictly the conform step enforces the schema. Set it on the asset, or
on the source as a default for assets still on `AUTO`:

| Strategy | Schema required | Behaviour |
|----------|-----------------|-----------|
| `AUTO` (default) | no | Reconcile when a schema is declared; infer one from the data otherwise. |
| `STRICT` | yes | Validate every row. Extra columns, missing required columns and wrong types fail the materialization. |
| `RECONCILE` | yes | Align columns to the schema (drop extras with a warning, fill missing with defaults or `None`) and coerce values. |

```py
@il.asset(schema=AdsStats, materialization_strategy=il.MaterializationStrategy.STRICT)
def ads_stats(self, ...): ...
```

A strategy that requires a schema, used without one, raises `AssetError`. A declared schema on
an asset returning non-tabular data raises `AssetError` too. Schema mismatches raise
`SchemaError`.

## The conform step

Conform runs on every `run()` and `materialize()`, after the [normalizer](normalization.md):

1. The data's [representation](../extending/representations.md) is resolved: rows, or a
   DataFrame when `interloper-pandas` is installed.
2. The data is canonicalized (`dict`, models, generators become `list[dict]`). Non-tabular
   data without a schema passes through untouched.
3. Without a schema, one is inferred and becomes the effective schema. Inference never fails a
   materialization; on error the effective schema is `None`.
4. With a schema, `STRICT` validates and `AUTO`/`RECONCILE` reconcile.

The effective schema reaches destinations as `IOContext.schema`.

## Schema operations

The same operations are available directly on rows:

```py
schema = il.Schema.infer(rows)                 # dynamic subclass, all columns optional
AdsStats.validate_rows(rows, strict=True)      # raises SchemaError on the first bad row
aligned = AdsStats.reconcile(rows)             # new rows, columns aligned and coerced
```

Inference collects the Python types seen per key; a single type is kept, `int` and `float`
widen to `float`, anything else becomes `Any`, and every column is nullable because a key may be
absent from some rows.

Reconciliation coerces field by field through pydantic type adapters. Extra keys are dropped
(logged once per reconciliation), missing fields receive their default, and values bound for a
`str` column are stringified first, since pydantic never coerces *to* `str` on its own (nested
lists and dicts are JSON-encoded). A value that cannot be coerced, or a missing non-nullable
field, raises `SchemaError`.

## Schemas in the catalog

An asset's declared schema appears in its `AssetDefinition` as `asset_schema`, so the catalog,
the API and the UI can show the expected columns without running anything.
