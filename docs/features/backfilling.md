# Backfilling

Backfilling means processing a range of partitions. In Interloper there is **no separate
backfiller object** — you simply run a DAG over a `TimePartitionWindow`, and the runner iterates
the window for you.

## Running a backfill

Pass a window to any runner's `run()` (or to `dag.materialize()`). The runner walks the window
**from most recent to oldest**, running the DAG once per partition:

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

result = il.run(il.AsyncRunner(max_workers=4).run(dag, window))
print(result.status)
```

This produces seven runs, one per day.

## Windowed backfill (single run)

When an asset declares `allow_window=True`, the entire window is handed to it as a single run
instead of being split into one run per date — let the asset fetch the whole range at once:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def weekly_data(context: il.ExecutionContext):
    start, end = context.partition_date_window
    return fetch_range(start, end)
```

## Stopping on failure

By default the in-process runners stop the current run on the first asset failure
(`fail_fast=True`). Set `fail_fast=False` to keep going and collect every result:

```py
runner = il.AsyncRunner(fail_fast=False)
result = il.run(runner.run(dag, window))
```

## Progress monitoring

Pass an `on_event` callback to the runner, or subscribe to the global event bus, to track
progress across a backfill:

```py
def on_event(event: il.Event):
    if event.type is il.EventType.RUN_COMPLETED:
        print(f"Completed: {event.metadata.get('partition_or_window')}")

result = il.run(il.AsyncRunner(on_event=on_event).run(dag, window))
```

See [Events](events.md) for the full event model.

## Distributed backfilling

For large backfills, use the Docker or Kubernetes runners. They use the same window mechanism —
each asset (with its ancestors) runs in an isolated container or Job:

```py
from interloper_docker.runner import DockerRunner

runner = DockerRunner(image="interloper:latest-worker", max_containers=4)
il.run(runner.run(dag, window))
```

```py
from interloper_k8s.runner import KubernetesRunner

runner = KubernetesRunner(image="my-repo/interloper:latest", namespace="data", max_jobs=4)
il.run(runner.run(dag, window))
```

See [Runners](runners.md) for all execution strategies and their options.
