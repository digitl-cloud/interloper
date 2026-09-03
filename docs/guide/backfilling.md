# Backfilling

Backfilling means materializing a range of partitions. There is no backfiller object: a range is
a `TimePartitionWindow`, and you either iterate it one partition per run, or hand it to assets
that can take the whole range at once.

## One run per partition

```py
import datetime as dt

import interloper as il

dag = il.DAG(source)
window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 31))

for partition in window:          # newest first
    dag.materialize(partition)
```

Iteration yields the most recent partition first, so an interrupted backfill leaves the recent
data in place rather than the ancient tail.

## One run for the whole window

When every materializable partitioned asset in the DAG declares `allow_window=True`, pass the
window itself. Each asset receives the range through `context.window` and fetches it in one
call; destinations split the write per partition:

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
def ads_stats(self, context: il.ExecutionContext) -> list[dict]:
    return fetch_stats(context.window.start, context.window.end)

dag.materialize(window)
```

A runner never splits a window into several runs. If any partitioned asset forbids windows, the
run fails before executing.

## Keeping going on failure

The default in-process runners stop the current run at the first failure. To collect every
partition's outcome instead:

```py
runner = il.AsyncRunner(fail_fast=False)
results = {partition: il.run(runner.run(dag, partition)) for partition in window}
failed = [p for p, r in results.items() if r.failed_ids]
```

## Watching progress

Pass `on_event` to the runner:

```py
def report(event: il.Event) -> None:
    if event.type is il.EventType.RUN_COMPLETED:
        print("done", event.metadata["partition_or_window"])

runner = il.AsyncRunner(on_event=report)
for partition in window:
    il.run(runner.run(dag, partition))
```

Or `ConsoleEventHandler()` from `interloper.events` to see every lifecycle event through the
logging stack. See [Events and logging](events.md).

## Trailing windows

A scheduled workload usually re-materializes a trailing window on every tick: yesterday, or the
last seven days, or the last three days because the vendor restates data. The window is counted
in partitions, and `TimePartitionWindow.lookback` computes it:

```py
now = dt.datetime.now(dt.timezone.utc)
il.TimePartitionWindow.lookback(now, lookback=7, offset=1)          # the seven days ending yesterday
il.TimePartitionWindow.lookback(now, lookback=1, offset=0)          # today, still incomplete
il.TimePartitionWindow.lookback(now, lookback=3, offset=1, granularity=il.TimeGranularity.MONTH)
```

`offset` is how many partitions back from the current one the window ends; `lookback` is how
many it spans. Pass `start=` to clamp to an asset's first partition; the result is `None` when
nothing remains. [Cron jobs](jobs.md) carry these two numbers as fields.

## Bounded history

`TimePartitionConfig(start=...)` marks the first partition an asset has data for. A window
reaching further back is rejected with `PartitionError` rather than fetching empty data, so a
backfill loop should start at the asset's `start`.
