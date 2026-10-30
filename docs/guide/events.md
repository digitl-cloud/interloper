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

`ConsoleEventHandler` maps types to logging levels: failures at `ERROR`, `operation_canceled`
at `WARNING`, `operation_queued` and all asset-data and destination I/O events at `DEBUG`,
everything else at `INFO`. `log` events use their own `level`.

To observe everything in the process, subscribe to the bus directly:

```py
il.EventBus.subscribe(on_event)                                          # every event
il.EventBus.subscribe(on_event, event_types=[il.EventType.RUN_FAILED])   # a subset
il.EventBus.unsubscribe(on_event)
```

Handlers run on a background worker thread in subscription order; an exception in one handler
is logged and isolated from the others. `il.EventBus.flush(timeout)` blocks until everything
queued so far has been delivered; a run's own flush waits with no timeout.

## The event

```py
event.type         # EventType
event.timestamp    # aware UTC datetime, set by the producer
event.metadata     # dict
event.id           # stable id, preserved across serialization
```

Metadata always carries what the producer knows: `run_id`, `component_id`, `component_kind`,
`component_key`, `qualified_key`, `window` and, when the component has an owner, `parent_id`,
plus `message` and, on failures, `error` and `traceback`. Destination events add
`destination_key`, writes add `rows`, log events add `level`, operation events add `attempt`.

Events serialize with `to_dict()` / `to_json()` and parse back with `Event.from_dict()` /
`Event.from_json()`. Operation lifecycle events get a **deterministic id** derived from run,
component and type, so the same logical event produced twice (a child process and its host)
collapses to one when persisted.

## Persistence

On the platform, an `EventRecorder` delivers a run's events in batches to the store, with no
deadline: a batch is whatever accumulated while the previous delivery was in flight. The store
folds the run's state from what it receives, so `run_started` starts the run and `run_completed`
or `run_failed` ends it. A run's verdict is the ingestion of its own terminal event and never
exists without the events before it. Every batch renews the run's heartbeat; a run that cannot
deliver for half the heartbeat timeout is lost, and the reaper fails it.

Why a run did not succeed is recorded on the run itself, as `Run.error`: the fault its runner
reported, or the reason the platform ended it (the reaper, a failed dispatch, an exhausted
quota). A run that failed only because operations failed has none of its own; their reasons are
on their `operation_failed` events.

A run's pod holds no database credentials. With a token the scheduler mints when it dispatches
the run, it loads the run from `GET /api/runs/{run_id}/spec` and delivers its batches to `POST
/api/runs/{run_id}/events`, both on the API's `server.internal_url`; in the scheduler's own
process the run is loaded and its batches go straight through the store.

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

Backfill and hook events are recorded by the platform, not by the core runners. A backfill records
`BACKFILL_STARTED` when it is created and one verdict when it finishes: `BACKFILL_COMPLETED` when
every partition succeeded, `BACKFILL_FAILED` when one failed (or, under fail-fast, at the first
failure). A canceled backfill records no verdict. Every type, with when it fires, is in
[Events](../reference/events.md).

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

Container-based runners run each operation in a child process with its own bus. On the
platform, the child reports its operation's events to the API itself, with its host run's token,
and the host authors the operation's verdict from the child's exit; deterministic ids make the
two verdicts one row. The host's own bus does not see the child's events, so a subscriber on the
host sees the run's and the operations' lifecycle, not the steps inside a child.

## Metrics

The built-in OpenTelemetry metrics are computed from these events by a bus subscriber, so they
cost nothing on the execution path. See [Telemetry](telemetry.md).
