# Utilities

Helpers exported from `interloper` (`il.run`, `il.bounded_gather`) and `interloper.utils`.

## Concurrency

| Function | Purpose |
|----------|---------|
| `il.run(coro)` | Run a coroutine to completion from sync code on a persistent background loop. Works inside Jupyter; Ctrl-C cancels; raises `RuntimeError` when called from the bridge's own loop. |
| `il.bounded_gather(coros, *, limit)` | Await coroutines concurrently with at most `limit` in flight; results in input order; the first exception cancels the rest. |
| `interloper.utils.invoke(fn, *args, **kwargs)` | Await an async callable, or run a sync one in a worker thread. |

## Data

| Function | Purpose |
|----------|---------|
| `coerce_to_records(data)` | `dict`, `list[dict]`, pydantic models, generators or `None` to `list[dict]`. Raises `NormalizerError` otherwise. |
| `is_empty(data)` | `True` only when positively empty: `None`, an object with a boolean `empty` attribute set (DataFrames), or a sized container of length zero. Lazy iterables are never consumed and count as non-empty. |

## Text

| Function | Example |
|----------|---------|
| `to_snake_case("AdsStatsByCountry")` | `ads_stats_by_country` |
| `to_label("ads_stats_by_country")` | `Ads Stats By Country` |
| `to_slug_case("Ads Stats")` | `ads-stats` |
| `to_identifier("campaigns__act-123")` | `campaigns__act_123` (invalid characters replaced, underscores kept) |
| `validate_key("2bad")` | raises `ValueError`; keys start with a letter and contain letters, digits and underscores |

## Time

| Function | Purpose |
|----------|---------|
| `coerce_to_date(value)` | `date`, `datetime` (date part) or ISO string to `date`. |
| `coerce_to_datetime(value)` | `datetime`, `date` (midnight) or ISO string to a **naive UTC** datetime; aware inputs are converted to UTC and stripped. |
| `add_months(date, n)` | Shift by whole months, clamping the day to the target month. |
| `assume_utc(dt)` | Attach UTC to a naive datetime; leave aware ones alone. |
| `month_start(dt)` | First day of the UTC month a timestamp falls in. |

## Imports

| Function | Purpose |
|----------|---------|
| `import_from_path("pkg.module.Class")` | Import by dotted path. |
| `import_from_path("pkg.module:Source.asset")` | Composite path: import the module, then walk the attribute chain. Used for source-owned assets. |
| `import_from_path(path, target_type)` | Also check the object is an instance of `target_type`. |
| `get_object_path(obj)` | The dotted path of a class or function. |
| `require_import("pandas", "install interloper-pandas")` | Decorator deferring an `ImportError` to first use of a class or function. |
