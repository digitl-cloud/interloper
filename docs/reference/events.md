# Event types

Every event carries `id`, `type`, `timestamp` and `metadata`. The metadata keys below are in
addition to the run-level metadata (`run_id`, `backfill_id`, anything the caller passed) that
every event inherits.

## Run

Emitted by the runner's `RunState`.

| Type | When | Metadata |
|------|------|----------|
| `run_started` | The walk begins. | `partition_or_window`, `message` |
| `run_completed` | Every operation completed or was skipped. | `partition_or_window`, `message` |
| `run_failed` | At least one operation failed, or the walk itself broke. | `partition_or_window`, `error` (walk failures only), `message` |

## Operation

Emitted by the runner's `RunState` with a **deterministic id** derived from run id, component id
and type. Common metadata: `component_id`, `component_kind`, `component_key`, `source_id`,
`partition_or_window`, `message`.

| Type | When |
|------|------|
| `operation_queued` | At run start, for every materializable operation. |
| `operation_started` | The operation was submitted. |
| `operation_completed` | `execute()` returned. |
| `operation_failed` | `execute()` raised. Adds `error` and, when the operation captures tracebacks, `traceback`. |
| `operation_canceled` | An upstream operation failed, or a fail-fast break canceled in-flight work. |

## Asset data

Emitted by `Asset.run_async()` around `data()`. Metadata: `component_id`, `component_kind`,
`component_key`, `qualified_key`, `source_id`, `partition_or_window`, `message`.

| Type | When |
|------|------|
| `asset_data_started` | Before `data()`. |
| `asset_data_completed` | After `data()` returned. |
| `asset_data_failed` | `data()` raised. Adds `error`, `traceback`. |

## Destination I/O

Emitted around each `read()` and `write()`. Metadata as for asset data, plus `destination_key`.

| Type | When |
|------|------|
| `dest_read_started`, `dest_read_completed`, `dest_read_failed` | Reading an upstream dependency. Failures add `error`, `traceback`. |
| `dest_write_started`, `dest_write_completed`, `dest_write_failed` | Writing the asset's result. Failures add `error`, `traceback`. |

## User logging

| Type | When | Metadata |
|------|------|----------|
| `log` | `context.logger.<level>(message)`, or an `EventLogger`. Also used by the framework for the empty-result warning. | `component_id`, `component_kind`, `component_key`, `source_id`, `level` (`DEBUG`, `INFO`, `WARNING`, `ERROR`), `message` |

## Emitted by the platform

Defined in the core so every consumer shares one vocabulary; produced by the scheduler, not by
the core runners.

| Type | Meaning |
|------|---------|
| `backfill_started`, `backfill_completed`, `backfill_failed` | Lifecycle of a multi-partition backfill. |
| `hook_fired`, `hook_failed` | A hook's `fire()` ran, or raised. |

## Console rendering levels

`ConsoleEventHandler` maps types to logging levels: failures at `ERROR`, `operation_canceled`
at `WARNING`, `operation_queued` and all asset-data and destination I/O events at `DEBUG`,
everything else at `INFO`. `log` events use their own `level`.

## Serialization

`Event.to_dict()` flattens to `{"event_id", "type", "timestamp", **metadata}`; `to_json()`
encodes it. `Event.from_dict()` and `from_json()` parse it back, generating a new id only when
`event_id` is absent. `Event.from_log_line()` accepts an `@EVENT:{...}` line or a bare JSON
object and returns `None` for anything else. Invalid input raises `EventError`.
