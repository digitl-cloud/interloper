# Coverage reads its window

Follows the executions table (#433), after which coverage was the slowest route.

## Problem

`coverage_by_day` read every asset's coverage rows all-time, because each asset's expected span starts at its first attempted partition and ends at its last. The rows came from joining every execution to its run for the partition key: about 1.2 s of SQL at 600k executions, plus about 1 s of Python building 264k row objects, most of them outside the window the calendar shows.

## Decisions

1. **The partition key is stamped on the execution** (migration 013). `stamp_execution_partition_key`, a `BEFORE INSERT` trigger on `executions`, copies it from the run as the fold creates the row, leaving migration 012's fold as it was. A run's partition key is set at creation and never changes, so the copy cannot drift. The migration backfills it in the same transaction that adds the column, so concurrent folds wait and then meet the trigger.
2. **Two indexes** serve the two questions: `(org_id, partition_key) INCLUDE (component_id, status, run_id)` for a window's rows, and `(org_id, component_id, partition_key)` for an asset's first and last attempted key.
3. **Coverage reads the window plus the bounds.** `_coverage_rows` reads executions alone, filtered by `partition_keys_overlapping(first, last)`: the keys of every granularity whose period meets the days, with one outer string range so the index seeks to the window. `_attempted_spans` gives each asset's first and last attempted day from two index probes per asset. The calendar's result is unchanged.
4. **`partition_key_range` takes the column it filters**, so runs and executions share it.

## Verification

- On the perf database (two organisations, windows of 3, 12 and 18 months) and on the dev database, the windowed calendar equals, group by group and day by day, what the all-time computation gives.
- Migration 013 by hand on Postgres: backfilled keys match their runs on all 1.3M perf executions (65 s), the trigger stamps a live insert, and a fresh database, a downgrade to 012 and a re-upgrade all land in the expected state.

## Measured (perf database, 10M events)

| Route | Big org before | after | Small org before | after |
|---|---|---|---|---|
| coverage, 3 months | 2,507 ms | 785 ms | 686 ms | 341 ms |
| coverage, 12 months | 2,568 ms | 1,553 ms | 724 ms | 682 ms |

## Not done here

Most of what remains, about 1.1 s of the big org's 12-month read, is Python: building a row object per asset and partition, then rolling them up by day. Aggregating by day in SQL, or building the arrays without the intermediate rows, is the next lever.
