---
name: interloper-triage
description: Use when an Interloper run failed or produced no data and the evidence is an event stream, a traceback, CLI output or the run detail page: finding which operation failed and why, what to fix, and whether retrying helps.
---

# Triaging a failed Interloper run

## Overview

Events are the record of a run. The `operation_failed` event carries the error and traceback
(`asset_data_failed` repeats it when `data()` raised); with the default `fail_fast=True` every
operation still queued is `operation_canceled`, dependent or not. Locate the failure, place it
in the operation's lifecycle, match the error signature, fix, re-run the partition.
Reference: https://docs.interloper.dev/reference/events/

## Recipe

1. **Get the events.** `interloper run ... --events json` streams one JSON object per line; the
   platform's run detail page shows the same stream (filter "errors"). Rows are flat: `event_id`,
   `type`, `timestamp`, `run_id`, `component_id`, `component_kind`, `component_key`,
   `partition_or_window`, `message`, plus `error` and `traceback` on failure rows only (absent,
   not null, elsewhere); `asset_data_*` and `dest_*` rows add `qualified_key` (`source.asset`).
   In Python the same keys sit in `event.metadata`. The CLI prints `asset_data_*` and `dest_*`
   events only with `-v`; confirm the capture level before reading their absence as evidence.

   ```sh
   jq -c 'select(.type == "operation_failed") | {component_key, error}' run.jsonl
   jq -r 'select(.type == "operation_failed") | .traceback' run.jsonl
   ```

2. **Place the failure** with the `asset_data_*` events of the same `component_key`:

   | Sequence | Where it broke |
   |----------|----------------|
   | `asset_data_failed` then `operation_failed` | inside your `data()` function |
   | `asset_data_completed` then `operation_failed` | after `data()`: normalizer, schema conform, or destination write |
   | `operation_failed` with no `asset_data_started` | before `data()`: reading a dependency from its destination, building kwargs, resources |

   `operation_canceled` carries the message `canceled (upstream failure)` and no cause field:
   the cause is the `operation_failed` that precedes it. `run_failed.error` is null unless the
   walk itself failed; the detail is always on the operation event.

3. **Match the signature.** Retry only helps for the transient rows.

   | Error | Cause | Fix | Retry |
   |-------|-------|-----|-------|
   | `AssetError: No destination found for upstream asset 'a'` | dependent asset reads `a` from a destination and none is attached | `Source(destinations=...)`, then re-run the whole partition so `a` is written first | no |
   | `SchemaError: ... extra fields not in schema: ['x']` | STRICT strategy and the payload gained a column | add `x: T \| None = None` to the schema, or RECONCILE to drop extras | no |
   | `SchemaError: Reconciliation failed ... input_value='2.66%'` | value the schema type cannot parse | strip or convert in a `Normalizer` subclass | no |
   | `SchemaError: ... Input should be a valid string [input_value=None]` | a required column is missing, usually keys still camelCase or nested | attach a normalizer, or make the column nullable | no |
   | `AssetError: ... strategy='reconcile' requires a schema` | strategy set without `schema=` | declare the schema | no |
   | `PartitionError: Windowed runs require all partitioned operations to set allow_window=True` | window passed to a DAG with per-partition assets | loop one run per partition | no |
   | `PartitionError: This run requires a partition or partition window` | partitioned asset run without `--date` | pass the key | no |
   | `ValidationError: api_key Field required` / `client_id Field required` | env var not set (`SHOP_API_KEY`, `INTERLOPER_<PROVIDER>_CLIENT_ID`) | export it; `--dry-run` never checks credentials | after the fix |
   | `TypeError: X.y() missing 1 required positional argument: 'orders'` | dependency parameter not wired, typically a cross-source `requires` | wire by id, see interloper-manifest | no |
   | `DependencyNotFoundError` | wired upstream id absent from this run | include the upstream source or asset in the run | no |
   | `httpx.HTTPStatusError: 429` / `5xx` / timeouts | vendor limit or outage; core has no retry | backoff honouring `Retry-After` in `data()`, lower `max_workers`, space backfills | yes, later |
   | `ModuleNotFoundError: No module named 'shop'` | CLI import without `PYTHONPATH=.` | set it or install the package | no |
   | `ConnectionCheckError` | credentials rejected by the service | fix the connection, "Test connection" in the UI | after the fix |

4. **Re-run the partition.** CLI: the same `--date`. Platform: "Retry failed" or "Retry all" on
   the run detail page creates a new run. Partition writes replace (CSV overwrites the file,
   database destinations delete the range first), so re-running a completed asset is safe.
   A retry never changes the outcome of a deterministic error.

## Quick reference

| Question | Where to look |
|----------|---------------|
| Which assets never ran | `operation_canceled` rows; `RunResult.canceled_ids` |
| Was anything written | `dest_write_completed` rows (`-v` on the CLI); none means no destination or the failure came first |
| Which partition | `partition_or_window` (`2026-01-15`, `2026-05-01:2026-08-01` for a window) |
| Run outcome in Python | `result.status`, `result.failed_ids`, `result.canceled_ids`, or `result.executions` (dict of operation id to `component_key`, `status`, `error`, `traceback`) |

## Common mistakes

- Reading `run_failed` with a null error as "no information": the error is on `operation_failed`.
- Retrying a deterministic failure (schema, destination, wiring) and expecting a different result.
- Treating an asset that completed without `dest_write_*` events as persisted: no destination
  means nothing was written, silently.
- Fixing the downstream asset when the upstream one has no destination.
