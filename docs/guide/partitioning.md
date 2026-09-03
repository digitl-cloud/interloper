# Partitioning

Partitioning slices an asset into discrete units, almost always by time. A run of a partitioned
asset is always scoped to one partition or to a window of partitions; the asset reads the scope
from its context and fetches exactly that slice.

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

`WEEK` and `QUARTER` exist with full arithmetic but cannot be declared on an asset: a partition
id is a storage contract, and no destination needs those two.

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
partition.slice(rows, "date")   # the rows inside the bounds

window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 7))
str(window)              # "2026-01-01:2026-01-07"
window.partition_count() # 7
list(window)             # TimePartition per day, newest first
```

Both bounds of a window are inclusive; a window ending before it starts raises `ValueError`.
Iteration yields the most recent partition first, so the freshest data lands first when a loop
is interrupted.

`TimePartitionWindow.lookback(now, lookback, offset, granularity, start)` builds the trailing
window a schedule should cover: `offset` partitions back from the current one, `lookback`
partitions long, clamped to `start`, or `None` when clamping leaves nothing. `now` may be an
aware datetime in the caller's zone; day, month and year windows then follow that zone's
calendar, while hour windows are always UTC because hour ids are UTC labels.

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

A run given a window fails as a whole when any materializable partitioned asset in the DAG does
not allow windows.

## What the context exposes

| Accessor | Gives you | Raises when |
|----------|-----------|-------------|
| `context.partition` | The `Partition`: `.value`, `.id`, `.granularity`, `.bounds` | not partitioned, no scope given, or the run holds a window |
| `context.window` | The `PartitionWindow`: `.start`, `.end`, `.partition_count()`, iteration | not time-partitioned, no scope given, or `allow_window` is off |
| `context.partition_date` | `partition.value` as a `date` | the granularity is not `DAY` |

## Validation

Before an asset executes, its scope is checked: a partitioned asset with no scope raises
`PartitionError`; an unpartitioned asset given a scope warns and ignores it; a window needs
`allow_window`; a time-partitioned asset needs a **time** partition of its own granularity; and a
scope reaching before `start` is rejected. Runners perform the same checks for the whole DAG
before starting, so a run that no asset can serve fails without executing anything.

## Custom partition schemes

`il.PartitionConfig(column, allow_window)`, `il.Partition(value)` and `il.PartitionWindow(start,
end)` are the generic bases. A custom partition's `id` is `str(value)` and its `slice()`
selects rows by id equality; subclass to change either. Custom windows implement `__iter__`.
