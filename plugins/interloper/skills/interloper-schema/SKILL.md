---
name: interloper-schema
description: Use when typing, validating or reshaping the rows an Interloper asset returns: declaring a schema, choosing a materialization strategy, flattening or renaming a vendor payload, or fixing SchemaError and NormalizerError failures.
---

# Schemas and normalization

## Overview

Every asset result goes through two steps: the normalizer reshapes rows (flatten, rename), the
conformer applies the schema (validate or coerce). Both run in `run()` and `materialize()`.
Declaring the schema is what makes CSV or database read-backs typed for dependent assets.
Reference: https://docs.interloper.dev/guide/schema/ and https://docs.interloper.dev/guide/normalization/

## Recipe

1. **Schema class** named after the asset, one typed field per column after normalization.
   `T | None` marks nullable, identifiers stay `str`, partitioned assets carry the partition
   column:

   ```py
   import datetime as dt
   import interloper as il

   class Campaigns(il.Schema):
       date: dt.date
       campaign_id: str
       status: str
       start_date: dt.date | None
       metrics_impressions: int
       metrics_ctr: float | None
       metrics_spend_amount: float
       labels: list[str]
       last_modified: dt.datetime
   ```

2. **Pick the strategy.** The default already coerces:

   | Strategy | With a declared schema |
   |----------|------------------------|
   | `AUTO` (default) | reconciles: coerces `"12034"` to `int`, drops extra columns with a warning, fills missing nullable columns with `None`, fails on values it cannot parse. Without a schema it infers one for the destination. |
   | `RECONCILE` | same as AUTO but explicit and requires the schema |
   | `STRICT` | validates only, never transforms: `"12034"` passes for `int` and an ISO string passes for `date`, both stay strings; extra or missing columns fail. For detecting contract drift, not for typing. |

3. **Normalizer** for the reshaping the schema cannot do. Options: `normalize_columns_names`
   (snake_case, default on), `flatten_max_level` (nesting depth to flatten, default `0` is off),
   `flatten_separator`, `fill_missing`, `drop_na_columns`, `snake_case_digits`,
   `column_overrides`, `replace_empty_dicts`, `replace_empty_strings`. Value-level changes are a
   subclass that calls the base first:

   ```py
   class PercentNormalizer(il.Normalizer):
       percent_columns: list[str] = ["metrics_ctr"]

       def normalize(self, data):
           rows = super().normalize(data)
           for row in rows:
               for column in self.percent_columns:
                   value = row.get(column)
                   if isinstance(value, str) and value.endswith("%"):
                       row[column] = float(value.rstrip("%"))
           return rows

   @il.asset(schema=Campaigns, normalizer=PercentNormalizer(flatten_max_level=2))
   def campaigns(self, context: il.ExecutionContext, connection: AcmeConnection) -> list[dict]:
       ...
   ```

   `flatten_max_level=2` flattens `metrics.spend.amount` into `metrics_spend_amount`; with `1`
   the `spend` dict survives and the `amount` column reads as missing. Use an integer, not
   `None`: `None` is dropped when the asset is serialized to a spec and the flattening silently
   stops in a child process. `"2.66%"` becomes `2.66`; divide by 100 if the schema means a ratio.
   A standalone asset has no `self`: `@il.asset(...)` on `def campaigns() -> list[dict]`.

4. **Verify** on the asset alone; `run()` returns the normalized, conformed rows without writing
   (the partition argument is only needed for partitioned assets):

   ```py
   rows = campaigns().run()
   print(rows[0]); print({k: type(v).__name__ for k, v in rows[0].items()})
   ```

## Error signatures

| Message | Meaning | Fix |
|---------|---------|-----|
| `Reconciliation failed on row 0: ... unable to parse string as a number [input_value='2.66%']` | a value the type cannot parse | strip or convert in a normalizer subclass |
| `Reconciliation failed on row 0: 1 validation error for str. Input should be a valid string [input_value=None]` | a required column is missing and `fill_missing` put `None` there: keys still camelCase or nested, or `flatten_max_level` too low | attach or deepen the normalizer, or make the column nullable |
| `Schema validation failed on row 0: extra fields not in schema: ['currency']` | STRICT and the payload grew | add the column (nullable) or drop STRICT |
| `Asset 'x': strategy='reconcile' requires a schema` | strategy without `schema=` | declare it |
| `declares a schema but returned data that cannot be checked against it` | result is not tabular (dict, scalar, object) | return `list[dict]` or a DataFrame, or drop the schema for opaque payloads |

## Common mistakes

- Choosing STRICT to "enforce" types: it never converts anything.
- Doing the reshaping inside `data()`: keep `data()` a faithful copy of the API and put the
  reshaping in the normalizer, where the platform shows and serializes it.
- An upstream asset without a schema: the dependent asset reads strings back from CSV.
- Forgetting the partition column in the schema of a partitioned asset.
- Numeric identifiers typed as `int`: leading zeros and 64-bit ids break; keep them `str`.
