---
name: interloper-backfill
description: Use when running Interloper assets over a range of partitions: a historical load, re-running failed days, monthly or hourly ranges, computing the window a scheduled job should cover (yesterday, last 7 days, last month), or reporting progress per partition.
---

# Backfilling Interloper assets

## Overview

The core has no backfill command: a backfill is a loop over a `TimePartitionWindow`, one run
per partition, newest first. A single windowed run is only possible when every partitioned
asset declares `allow_window=True` on its `TimePartitionConfig`. The platform's Backfills page does the same loop with a
concurrency cap. Reference: https://docs.interloper.dev/guide/backfilling/ and
https://docs.interloper.dev/guide/partitioning/

## Recipe

1. **One runner, one run per partition, failures isolated:**

   ```py
   import datetime as dt
   import interloper as il

   shop = Shop(destinations=il.CSVDestination(base_path="./data"))
   dag = il.DAG(shop)

   def on_event(event: il.Event) -> None:
       if event.type in (il.EventType.RUN_COMPLETED, il.EventType.RUN_FAILED):
           print(event.metadata["partition_or_window"], event.metadata["message"])
       elif event.type is il.EventType.OPERATION_FAILED:
           print("  ", event.metadata["component_key"], event.metadata["error"])

   runner = il.AsyncRunner(fail_fast=False, reraise=False, on_event=on_event)
   results = {}
   for partition in il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 10)):   # newest first
       results[partition.id] = il.run(runner.run(dag, partition))
   redo = {k: [e.component_key for e in r.executions.values() if e.status is not il.ExecutionStatus.COMPLETED]
           for k, r in results.items() if r.status is not il.ExecutionStatus.COMPLETED}   # failed and canceled
   ```

   `fail_fast=False` keeps sibling assets going inside one run; `reraise=False` (the
   `AsyncRunner` default) turns failures into `RunResult` entries instead of exceptions.
   Dependents of a failed asset are canceled, not failed, so re-run lists must include both.
   Partition isolation comes from the loop, not from either flag. `il.run` is the sync bridge
   and is safe in notebooks. To test the failure path, raise inside `data()` for one
   partition (an environment variable switch) and check the other days still land.

2. **Windowed assets in one run.** When every partitioned asset in the DAG has
   `allow_window=True`, pass the window; the asset reads `context.window` once and the
   destination splits the write per partition:

   ```py
   window = il.TimePartitionWindow(dt.date(2026, 5, 1), dt.date(2026, 8, 1), granularity=il.TimeGranularity.MONTH)
   il.run(runner.run(dag, window))                # 4 monthly partitions, ids 2026-08 ... 2026-05
   ```

   Month and year bounds are the first day of the period. A DAG with one asset lacking
   `allow_window` rejects the window with `PartitionError`; loop instead.

3. **Scheduled windows.** `lookback` counts partitions, not days; pass a timezone-aware
   datetime so the calendar is the job's, not UTC's:

   ```py
   from zoneinfo import ZoneInfo
   now = dt.datetime.now(ZoneInfo("Europe/Berlin"))
   il.TimePartitionWindow.lookback(now, lookback=1, offset=1)                                   # yesterday
   il.TimePartitionWindow.lookback(now, lookback=7, offset=1)                                   # last 7 days ending yesterday
   il.TimePartitionWindow.lookback(now, lookback=1, offset=1, granularity=il.TimeGranularity.MONTH)   # last month
   ```

   These are the `lookback` / `offset` fields of a `CronJob`. The result is `None` when the
   optional `start` clamp leaves nothing to cover; `window.start`, `window.end` and each
   `partition.value` are dates.

4. **From the shell**, one process per partition, stop on the first failure or keep going:

   ```sh
   for d in 2026-01-{01..10}; do PYTHONPATH=. interloper run -f shop.yaml --date "$d" || echo "FAILED $d"; done
   ```

   `--start-date`/`--end-date` is one windowed run with the same `allow_window` requirement.

5. **Check the output** per partition: `./data/<dataset>/<table>/<column>=<key>/data.csv`.
   Re-running a partition overwrites it, so a failed day is fixed by running that day again;
   directories from earlier runs are never removed, so list by date, not by count.

## Quick reference

| Need | Use |
|------|-----|
| Runner defaults | `AsyncRunner`: `fail_fast=True, reraise=False, max_workers=...`; `SerialRunner` same flags; the abstract `Runner` defaults to `fail_fast=False, reraise=True` |
| Outcome of one run | `result.status`, `result.executions` (dict of operation id to `component_key`, `status`, `error`), the id lists `completed_ids` / `failed_ids` / `canceled_ids`, `execution_time` |
| Partition count of a window | `window.partition_count()`; `window.id` is `start-end`, events report `start:end` |
| Hourly partitions | `il.TimeGranularity.HOUR`, keys like `2026-01-15T13` |
| Parallel partitions | run several `runner.run(dag, partition)` coroutines with `asyncio.gather`, or use the platform's backfill with its concurrency cap |
| Progress events | `RUN_STARTED`/`RUN_COMPLETED`/`RUN_FAILED` carry `partition_or_window` and `message`; `OPERATION_FAILED` carries `component_key`, `error`, `traceback` |

## Common mistakes

- Passing a window to a DAG whose assets run per partition: `PartitionError: Windowed runs
  require all partitioned operations to set allow_window=True` (`interloper.errors.PartitionError`,
  not on `il`).
- Reading `fail_fast=False` as "keep going to the next partition": it only scopes one run.
- Treating `failed_ids` as asset keys; they are operation ids, `result.executions` carries the keys.
- Calling `lookback` with `dt.date.today()`: the date has no timezone, so the day boundary is
  the process's, not the job's.
- Looking for `il.Backfiller` or `interloper backfill`: neither exists in the core.
