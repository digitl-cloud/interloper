# Partitioning

Partitioning slices an asset into discrete units, by time or by a declared list of values. A run
of a partitioned asset always covers a **window**: one partition, or several when the asset allows
it. A partition is the storage unit; the window is the scope. The asset reads the window from its
context and fetches exactly that slice.

## Time partitioning

```py
import datetime as dt

import interloper as il

@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def ads_stats(self, context: il.ExecutionContext) -> list[dict]:
    date = context.partition_date
    rows = fetch_stats(date)
    for row in rows:
        row["date"] = date
    return rows
```

`column` names the column carrying the partition value in the data. Destinations use it to
scope reads, replaces and row counts, so partitioned rows must carry it.

Run for one partition:

```py
il.DAG(source).materialize(il.TimePartition(dt.date(2026, 1, 15)))
```

### TimePartitionConfig

| Option | Default | Meaning |
|--------|---------|---------|
| `column` | required | The partition column. |
| `granularity` | `DAY` | The period one partition covers. |
| `allow_window` | `False` | Whether one run may cover several partitions. |
| `start` | `None` | First partition that exists. Earlier runs are rejected; windows are clamped to it. |

## Granularity

A time partition is a **period identified by its start**. `TimeGranularity` says how long the
period lasts and owns every piece of time arithmetic:

| Granularity | Partition id | Period |
|-------------|--------------|--------|
| `HOUR` | `2026-08-21T13` | one hour, labelled in UTC |
| `DAY` | `2026-08-21` | one day |
| `MONTH` | `2026-08` | one calendar month |
| `YEAR` | `2026` | one calendar year |

Ids are ISO-8601 prefixes: they sort chronologically as strings, embed in paths unchanged, and
each shape names its granularity, so `il.TimePartition.from_key("2026-08")` rebuilds the
partition without being told it is monthly.

```py
g = il.TimeGranularity.MONTH
g.truncate(dt.date(2026, 5, 20))          # 2026-05-01, the period start
g.advance(dt.date(2026, 5, 1), -1)        # 2026-04-01
g.bounds(dt.date(2026, 5, 20))            # (2026-05-01, 2026-06-01), half-open
g.periods_between(a, b)                   # whole periods from a to b
g.format(dt.date(2026, 5, 20))            # "2026-05"
g.parse("2026-05")                        # 2026-05-01
list(g.period_range(start, end))          # period starts, inclusive
```

Rows of a partition may carry values anywhere inside the period (a monthly partition holding
daily dates), so destinations scope by the partition's half-open **bounds**, never by equality
on the id.

## Partitions and windows

```py
partition = il.TimePartition(dt.date(2026, 5, 20), il.TimeGranularity.MONTH)
partition.value          # 2026-05-01, normalized to the period start
partition.id             # "2026-05"
partition.bounds         # (2026-05-01, 2026-06-01)
partition.window         # the one-partition window
partition.filter("date") # the bounds, as a database selects them
partition.slice(rows, "date")   # the rows inside the bounds

window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 7))
str(window)              # "2026-01-01..2026-01-07"; "2026-01-01" for a window of one
len(window)              # 7
list(window)             # TimePartition per day, oldest first
window.bounds            # (2026-01-01, 2026-01-08)
```

Both bounds of a window are inclusive; a window ending before it starts raises `ValueError`.
Every entry point (`asset.run`, `asset.materialize`, `dag.materialize`, `runner.run`) accepts a
bare partition as the window of one.

`TimePartitionWindow.lookback(now, lookback, offset, granularity, start)` builds the trailing
window a schedule should cover: `offset` partitions back from the current one, `lookback`
partitions long, clamped to `start`, or `None` when clamping leaves nothing. `now` may be an
aware datetime in the caller's zone; day, month and year windows then follow that zone's
calendar, while hour windows are always UTC because hour ids are UTC labels.

## List partitioning

An asset partitioned by a declared list of values, such as regions or markets:

```py
@il.asset(partitioning=il.ListPartitionConfig(column="region", values=["eu", "us", "apac"]))
def sales(self, context: il.ExecutionContext) -> list[dict]:
    region = context.partition.value
    return [{"region": region, **row} for row in fetch_sales(region)]
```

The values are the partition ids, in the declared order. A window is a contiguous range of that
order: `config.window("eu", "us")` covers `eu` and `us`, and a backfill from the first value to
the last covers everything. Databases select a list partition by membership, the file and object
destinations key storage by its id. Platform support for list backfills and the run form is a
follow-up; today a list-partitioned asset runs through `asset.run`, `dag.materialize` and the
CLI.

Every partitioning answers the same questions: `check(window)` validates a run's window,
`partition(key)` parses a persisted key, `window(start_key, end_key)` builds a window,
`compatible(other)` says whether both ends of a DAG edge accept the same windows, and
`to_definition()` renders the catalog entry.

## Windowed assets

An asset that can fetch a range in one call declares `allow_window=True` and reads
`context.window`:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def ads_stats(self, context: il.ExecutionContext) -> list[dict]:
    window = context.window
    return fetch_stats(window.start, window.end)
```

Passing a window to a run is then a single execution covering the whole range. A single
partition still works and is presented to the asset as a one-partition window. Destinations
split window writes per partition, so storage looks identical either way.

A run given a window fails as a whole when any enabled partitioned asset in the DAG does
not allow windows.

## What the context exposes

| Accessor | Gives you | Raises when |
|----------|-----------|-------------|
| `context.partition` | The window's only `Partition`: `.id`, and `.bounds` or `.value` by kind | not partitioned, no window given, or the window covers several partitions |
| `context.window` | The `PartitionWindow`: `.start`, `.end`, `len()`, iteration | not partitioned, or no window given |
| `context.partition_date` | `partition.value` as a `date` | not time-partitioned at `DAY`, or the window covers several partitions |

## Validation

Before an asset executes, its partitioning checks the run's window: a partitioned asset with no
window, a window of several partitions without `allow_window`, a window of another kind or
granularity, or one reaching before `start` raise `PartitionError`, named after the asset. An
unpartitioned asset ignores any window. Runners run the same check for every enabled operation
before starting, listing every failing one, so a run no asset can serve fails without executing
anything.

## Extending partitioning

A new kind subclasses `il.PartitionConfig`, `il.Partition` and `il.PartitionWindow` and implements
their abstract contract; the config's `kind` registers it for `from_definition`. The list kind in
`interloper.partitioning.list` is the reference implementation.
