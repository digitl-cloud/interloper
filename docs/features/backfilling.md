# Backfilling

Backfilling means processing a range of partitions. In Interloper there is **no separate
backfiller object**: a range of partitions is just a `TimePartitionWindow`, and you either hand
it to an asset that can fetch the whole range at once, or iterate it one partition at a time.

## Iterating a window

A `TimePartitionWindow` is iterable, yielding a `TimePartition` per period **from most recent to
oldest**. Running the DAG once per partition is an explicit loop:

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

source = my_source(destinations=il.FileDestination("./data"))
dag = il.DAG(source)

window = il.TimePartitionWindow(
    start=dt.date(2025, 1, 1),
    end=dt.date(2025, 1, 7),
)

for partition in window:          # newest first
    dag.materialize(partition)
```

That is seven runs, one per day, freshest first.

!!! note

    Passing the *window itself* to a runner is a different thing: it is a single run covering the
    whole range, and it requires every partitioned asset in the DAG to declare
    `allow_window=True` (see below). A runner never splits a window into one run per partition;
    that fan-out is either your own loop, or a **job** on the platform, which creates one queued
    run per partition and executes them concurrently.

## Windowed backfill (single run)

When every partitioned asset in the DAG declares `allow_window=True`, the window can be passed
straight to a runner: the entire range is handed to the asset as a **single run**, so it can
fetch the whole thing at once.

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def weekly_data(context: il.ExecutionContext):
    window = context.window
    return fetch_range(window.start, window.end)
```

## Stopping on failure

By default the in-process runners stop the current run on the first asset failure
(`fail_fast=True`). Set `fail_fast=False` to keep going and collect every result:

```py
runner = il.AsyncRunner(fail_fast=False)
for partition in window:
    il.run(runner.run(dag, partition))
```

## Progress monitoring

Pass an `on_event` callback to the runner, or subscribe to the global event bus, to track
progress across a backfill:

```py
def on_event(event: il.Event):
    if event.type is il.EventType.RUN_COMPLETED:
        print(f"Completed: {event.metadata.get('partition_or_window')}")

runner = il.AsyncRunner(on_event=on_event)
for partition in window:
    il.run(runner.run(dag, partition))
```

See [Events](events.md) for the full event model.

## Distributed backfilling

For large backfills, use the Docker or Kubernetes runners. They take the same partition scope as
the in-process ones, and each asset (with its ancestors) runs in an isolated container or Job:

```py
from interloper_docker.runner import DockerRunner

runner = DockerRunner(image="interloper:latest-worker", max_containers=4)
for partition in window:
    il.run(runner.run(dag, partition))
```

```py
from interloper_k8s.runner import KubernetesRunner

runner = KubernetesRunner(image="my-repo/interloper:latest", namespace="data", max_jobs=4)
for partition in window:
    il.run(runner.run(dag, partition))
```

See [Runners](runners.md) for all execution strategies and their options.

## Dispatch order

Both the framework and the platform work through a range **newest partition first**. Iterating a
`TimePartitionWindow` yields the most recent partition first, and a platform backfill queues its
latest partitions first, promoting the next-newest as slots free up. The freshest data lands
first, and an interrupted backfill keeps the recent window rather than the ancient tail.

## Scheduled trailing windows

A platform **job** re-materializes a trailing window on every tick. The window is counted in
partitions, not days: `offset` is how many partitions back from the current one it ends, and
`lookback` how many it spans. With daily targets, the defaults (`offset=1`, `lookback=1`) mean
"yesterday only", and `offset=3` suits a vendor whose data settles after three days.

`il.lookback_window` is the same computation, available to anything driving its own schedule:

```py
il.lookback_window(dt.datetime.now(dt.timezone.utc), lookback=7, offset=1)
# 2026-08-11 to 2026-08-17
```

It returns `None` when clamping to an asset's `start` leaves the window empty.
