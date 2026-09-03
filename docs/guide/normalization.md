# Normalization

A normalizer reshapes raw asset output before it is checked against the schema: it coerces the
input to rows, flattens nested records, renames columns and aligns keys. Normalization is
optional and configurable; the [conform step](schema.md#the-conform-step) that follows it always
runs.

## Using a normalizer

```py
import interloper as il

@il.asset(normalizer=il.Normalizer(flatten_max_level=1))
def campaigns(self) -> list[dict]:
    return [{"CampaignId": "1", "Budget": {"Amount": 10.0, "Currency": "EUR"}}]

# after normalization:
# [{"campaign_id": "1", "budget_amount": 10.0, "budget_currency": "EUR"}]
```

Set it on a source to apply to every asset that has none of its own:

```py
@il.source(normalizer=il.Normalizer(snake_case_digits=True))
class Amazon(il.Source): ...
```

## Options

| Option | Default | Effect |
|--------|---------|--------|
| `normalize_columns_names` | `True` | Convert column names to `snake_case`. |
| `flatten_max_level` | `0` | Flatten nested dicts: `0` disables, `None` flattens without limit, `n` flattens `n` levels. |
| `flatten_separator` | `"_"` | Joins flattened key segments. |
| `fill_missing` | `True` | Give every row the same keys, filling gaps with `None`. |
| `drop_na_columns` | `False` | Drop columns that are `None` in every row. |
| `snake_case_digits` | `False` | Split letter-digit boundaries: `acosClicks14d` becomes `acos_clicks_14d`. |
| `column_overrides` | `{}` | Raw name to normalized name, applied before snake-casing, for names no rule handles (`eCPAddToCart`). |
| `replace_empty_dicts` | `False` | Replace `{}` with `None` before flattening. |
| `replace_empty_strings` | `False` | Replace `""` with `None`. |

Transformations apply in this order: empty-dict replacement, flattening, empty-string
replacement, column dropping, renaming, key filling.

## Accepted input

`dict`, `list[dict]`, a pydantic model or a list of them, and generators or iterators of those.
`None` yields an empty list. Anything else raises `NormalizerError`. With `interloper-pandas`
installed, `DataFrameNormalizer` accepts and returns DataFrames with the same options.

## Custom normalizers

Subclass `Normalizer` to add vendor-specific reshaping. Override `normalize()` and defer to the
base for the standard transformations, or override `column_name()` for a naming rule:

```py
class PivotActions(il.Normalizer):
    def normalize(self, data):
        rows = self._coerce(data)
        for row in rows:
            for action in row.pop("actions", []) or []:
                row[f"actions_{action['action_type']}"] = action["value"]
        return super().normalize(rows)
```

Normalizers are `Serializable`: a configured subclass round-trips through
[specs](specs.md) with its class and options intact, which is what lets a serialized DAG carry
it into another process.
