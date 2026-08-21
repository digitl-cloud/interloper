# Partitioning

Partitioning lets assets process data in slices, typically by date. This enables incremental
processing and efficient backfilling.

## Time partitioning

Add `partitioning` to an asset and access the partition date via `context.partition_date`:

```py
import datetime as dt
import interloper as il

@il.source
def my_source():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily_data(context: il.ExecutionContext):
        date = context.partition_date
        return [{"date": date.isoformat(), "value": 42}]

    return [daily_data]
```

Run for a specific date:

```py
source = my_source(destinations=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 15)))
```

Data is stored in partition-aware paths:

```
./data/{dataset}/{asset_key}/date=2025-01-15/data.pkl
```

## Granularity

A time partition is a **period identified by its start**: the value is the period's first instant,
and the granularity says how long the period lasts. `TimePartitionConfig` declares it:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.DAY))
def daily_data(context: il.ExecutionContext): ...
```

`DAY` is the default. An asset may declare any of the granularities BigQuery time partitioning
offers — `HOUR`, `DAY`, `MONTH`, `YEAR`. `WEEK` and `QUARTER` exist in the vocabulary with full
arithmetic but cannot be declared: a partition id is a storage contract, and no destination needs
those two.

Each granularity has a canonical partition id, an ISO-8601 prefix whose shape carries the
granularity:

| Granularity | Id | Period |
|---|---|---|
| `HOUR` | `2026-08-21T13` | one hour, labelled in UTC |
| `DAY` | `2026-08-21` | one day |
| `MONTH` | `2026-08` | one calendar month |
| `YEAR` | `2026` | one calendar year |

`il.TimePartition.from_key("2026-08")` turns an id back into its `TimePartition`, inferring the
granularity from the shape. Rows of a partition may carry column values anywhere inside the
period (a monthly partition whose rows hold daily dates): destinations scope reads and replaces
by the partition's half-open `bounds`, not by equality on its id.

Every piece of partition arithmetic goes through the granularity, so assets and destinations never
hardcode "a day":

```py
g = il.TimeGranularity.DAY

g.truncate("2026-05-20")                    # 2026-05-20   (start of the period)
g.advance(dt.date(2026, 5, 20), -1)         # 2026-05-19   (n periods back)
g.bounds(dt.date(2026, 5, 20))              # (2026-05-20, 2026-05-21)  half-open
g.periods_between(a, b)                     # whole periods from a to b
g.format(dt.date(2026, 5, 20))              # "2026-05-20" (the partition id)
```

The CLI's date flags take partition keys, so `--date 2026-05` materializes a monthly asset's May
partition and `--start-date 2026-01 --end-date 2026-06` is a six-month window.

## Bounding the history

`start` marks the first partition that exists for an asset. Runs that reach further back are
rejected rather than silently returning empty data:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", start=dt.date(2026, 1, 1)))
def daily_data(context: il.ExecutionContext): ...
```

## Windowed partitioning

Some assets can process a range of periods in a single execution. Enable this with
`allow_window=True` and read `context.window`:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def weekly_summary(context: il.ExecutionContext):
    window = context.window
    return [{"start": window.start.isoformat(), "end": window.end.isoformat(), "value": 100}]
```

A windowed asset is still run one partition at a time by the platform, so a single partition
normalizes to a one-partition window: `context.window` reads the same either way.

Run with a window:

```py
dag.materialize(
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 7),
    ),
)
```

## What the context exposes

The context hands over the **scope itself**, and the scope answers questions about itself. There is
no accessor per granularity:

| Accessor | Gives you |
|----------|-----------|
| `context.partition` | The `Partition` this run covers. Read `.value` (the period's start), `.id` (its canonical key), `.granularity`, `.bounds` (its half-open extent) |
| `context.window` | The `PartitionWindow`, for an asset declaring `allow_window=True`. Read `.start` / `.end` (both inclusive), `.partition_count()`, or iterate it |
| `context.partition_date` | Sugar over `context.partition.value` for the daily case: a `dt.date`, asserting the granularity rather than assuming it |

!!! note

    `context.partition` raises if the run is scoped to a window, and `context.window` raises unless
    the asset declares `allow_window=True`. `context.partition_date` additionally raises if the
    asset is partitioned at any granularity other than `DAY` — read `context.partition` there and
    ask the partition itself.

## TimePartitionConfig

```py
il.TimePartitionConfig(
    column="date",                          # Column carrying the partition value
    allow_window=False,                     # Whether the asset supports windowed runs
    granularity=il.TimeGranularity.DAY,     # The period one partition covers
    start=None,                             # First partition that exists (dt.date)
)
```

## Partition types

| Type | Description |
|------|-------------|
| `TimePartition(value, granularity=DAY)` | A single partition, identified by its period start (also accepts an ISO string or datetime) |
| `TimePartitionWindow(start, end, granularity=DAY)` | A contiguous range of partitions (both bounds inclusive) |

A `TimePartitionWindow` is iterable. It yields `TimePartition` values **from most recent to
oldest**:

```py
window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3))
for partition in window:
    print(partition)
    # 2025-01-03
    # 2025-01-02
    # 2025-01-01

window.partition_count()  # 3
```

For non-time domains, the generic `il.Partition`, `il.PartitionWindow` and `il.PartitionConfig`
base types are available to build custom partition schemes.
