# Events & logging

Every step of a run emits an event on a process-wide bus: runs starting and ending, operations
moving through their lifecycle, `data()` calls, destination reads and writes, and the messages
assets log. Events are how you observe execution, and what the platform persists.

## Receiving events

The simplest way is a runner's `on_event`, which receives only that run's events:

```py
import interloper as il

def on_event(event: il.Event) -> None:
    print(event)

result = il.run(il.AsyncRunner(on_event=on_event).run(dag))
```

`ConsoleEventHandler` is a ready-made handler that renders events through the standard logging
stack on the `interloper.run` logger, so they share the format, stream and verbosity of your
other log lines:

```py
from interloper.events import ConsoleEventHandler

result = il.run(il.AsyncRunner(on_event=ConsoleEventHandler()).run(dag))
```

Failures log at `ERROR`, cancellations at `WARNING`, run and operation lifecycle at `INFO`, and
the high-frequency `data()` and destination I/O events at `DEBUG`.
`ConsoleEventHandler(json_lines=True)` writes raw JSON lines to stdout instead.

To observe everything in the process, subscribe to the bus directly:

```py
il.EventBus.subscribe(on_event)                                          # every event
il.EventBus.subscribe(on_event, event_types=[il.EventType.RUN_FAILED])   # a subset
il.EventBus.unsubscribe(on_event)
```

Handlers run on a background worker thread in subscription order; an exception in one handler
is isolated from the others. `il.EventBus.flush(timeout)` blocks until everything queued so far
has been delivered.

## The event

```py
event.type         # EventType
event.timestamp    # aware UTC datetime, set by the producer
event.metadata     # dict
event.id           # stable id, preserved across serialization
```

Metadata always carries what the producer knows: `run_id`, `component_id`, `component_kind`,
`component_key`, `qualified_key`, `source_id`, `partition_or_window`, plus `message` and, on
failures, `error` and `traceback`. Destination events add `destination_key`; log events add
`level`.

Events serialize with `to_dict()` / `to_json()` and parse back with `Event.from_dict()` /
`Event.from_json()`. Operation lifecycle events get a **deterministic id** derived from run,
component and type, so the same logical event produced twice (a child process and its host)
collapses to one when persisted.

## Event types

| Group | Types |
|-------|-------|
| Run | `RUN_STARTED`, `RUN_COMPLETED`, `RUN_FAILED` |
| Operation | `OPERATION_QUEUED`, `OPERATION_STARTED`, `OPERATION_COMPLETED`, `OPERATION_FAILED`, `OPERATION_CANCELED` |
| Asset data | `ASSET_DATA_STARTED`, `ASSET_DATA_COMPLETED`, `ASSET_DATA_FAILED` |
| Destination I/O | `DEST_READ_STARTED`, `DEST_READ_COMPLETED`, `DEST_READ_FAILED`, `DEST_WRITE_STARTED`, `DEST_WRITE_COMPLETED`, `DEST_WRITE_FAILED` |
| Backfill | `BACKFILL_STARTED`, `BACKFILL_COMPLETED`, `BACKFILL_FAILED` |
| Hooks | `HOOK_FIRED`, `HOOK_FAILED` |
| User | `LOG` |

Backfill and hook events are emitted by the platform, not by the core runners. The full table
with metadata per type is in [Event types](../reference/events.md).

## Logging from assets

`context.logger` emits `LOG` events attributed to the asset, with the standard level names:

```py
@il.asset
def ads_stats(self, context: il.ExecutionContext) -> list[dict]:
    context.logger.info("Requesting report")
    rows = fetch()
    context.logger.debug(f"{len(rows)} rows")
    return rows
```

Because these are events, they reach `on_event`, the console handler, the persisted run log and
any other subscriber, not just a local logger. `EventLogger(component_key, metadata)` from
`interloper.events` builds the same logger for code outside an asset. Emitting an arbitrary
event is `il.EventBus.emit(il.EventType.LOG, metadata={...})`.

## Events across processes

Container-based runners run assets in child processes that have their own bus. With
`INTERLOPER_EVENTS_TO_STDERR=true`, the `interloper run` command subscribes a
`StderrEventHandler` that writes each event as an `@EVENT:{json}` line on stderr; the host
parses those lines with `Event.from_log_line()` and re-emits them with `EventBus.emit_event()`,
preserving ids and timestamps. Subscribers on the host therefore see one unified stream.

## Metrics

The built-in OpenTelemetry metrics are computed from these events by a bus subscriber, so they
cost nothing on the execution path. See [Telemetry](telemetry.md).
