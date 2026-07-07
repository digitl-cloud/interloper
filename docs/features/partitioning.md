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

## Windowed partitioning

Some assets can process a range of dates in a single execution. Enable this with
`allow_window=True` and read the bounds from `context.partition_date_window`:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def weekly_summary(context: il.ExecutionContext):
    start, end = context.partition_date_window
    return [{"start": start.isoformat(), "end": end.isoformat(), "value": 100}]
```

Run with a window:

```py
dag.materialize(
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 7),
    ),
)
```

!!! note

    `context.partition_date` is only available for single-partition runs.
    `context.partition_date_window` is only available when `allow_window=True` and a window is
    passed. Accessing the wrong one raises `AttributeError`.

## TimePartitionConfig

```py
il.TimePartitionConfig(
    column="date",          # Column carrying the partition value
    allow_window=False,     # Whether the asset supports windowed runs
    start_date=None,        # Optional lower bound (dt.date)
)
```

## Partition types

| Type | Description |
|------|-------------|
| `TimePartition(value=date)` | A single date partition (also accepts an ISO string or datetime) |
| `TimePartitionWindow(start, end)` | A date range (inclusive) |

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
