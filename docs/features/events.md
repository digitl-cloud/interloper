# Events

Interloper has a built-in event system for monitoring asset execution, destination I/O, and run
lifecycle. Events flow through a process-wide `EventBus`.

## Subscribing to events

Subscribe and unsubscribe handlers with the `EventBus` class methods:

```py
import interloper as il

def on_event(event: il.Event):
    print(f"[{event.type.value}] {event.metadata}")

il.EventBus.subscribe(on_event)

dag.materialize()

il.EventBus.unsubscribe(on_event)
```

Limit a handler to specific event types by passing `event_types`:

```py
il.EventBus.subscribe(on_event, event_types=[il.EventType.RUN_COMPLETED])
```

You can also receive events without subscribing globally by passing `on_event` to a runner —
see [Runners](runners.md).

## Event types

| Event | Description |
|-------|-------------|
| `ASSET_QUEUED` | Asset queued for execution |
| `ASSET_STARTED` | Asset materialization started |
| `ASSET_COMPLETED` | Asset materialization completed |
| `ASSET_FAILED` | Asset materialization failed |
| `ASSET_CANCELED` | Asset canceled (downstream of a failure) |
| `ASSET_EXEC_STARTED` | Asset function execution started |
| `ASSET_EXEC_COMPLETED` | Asset function execution completed |
| `ASSET_EXEC_FAILED` | Asset function execution failed |
| `DEST_READ_STARTED` | Destination read started (upstream dependency) |
| `DEST_READ_COMPLETED` | Destination read completed |
| `DEST_READ_FAILED` | Destination read failed |
| `DEST_WRITE_STARTED` | Destination write started |
| `DEST_WRITE_COMPLETED` | Destination write completed |
| `DEST_WRITE_FAILED` | Destination write failed |
| `RUN_STARTED` | Runner started a run |
| `RUN_COMPLETED` | Runner completed a run |
| `RUN_FAILED` | Runner run failed |
| `BACKFILL_STARTED` | Backfill started |
| `BACKFILL_COMPLETED` | Backfill completed |
| `BACKFILL_FAILED` | Backfill failed |
| `LOG` | User-emitted log message |

## Event object

```py
event.type        # EventType enum
event.timestamp   # datetime when the event was emitted
event.metadata    # dict with event-specific data
event.id          # unique event id
```

Metadata typically includes `asset_key`, `run_id`, `partition_or_window`, and for failures,
`error` and `traceback`. Events serialize with `event.to_dict()` / `event.to_json()` and parse
back with `Event.from_dict()` / `Event.from_json()`.

## Emitting events

Inside asset functions, use `context.logger` to emit `LOG` events:

```py
@il.asset
def my_asset(context: il.ExecutionContext):
    context.logger.info("Starting data fetch...")
    context.logger.warning("Rate limited, retrying...")
    context.logger.error("Something went wrong")
    context.logger.debug("Detailed debug info")
    return [{"key": "value"}]
```

Each call emits a `LOG` event whose metadata carries the `message`, `level`, and `asset_key`, so
it reaches every subscriber.

To emit a custom event programmatically:

```py
il.EventBus.emit(il.EventType.LOG, metadata={"message": "hello", "level": "INFO"})
```

## Distributed runs

The Docker and Kubernetes runners execute assets in child containers/Jobs and **forward their
events back to the host** over stderr, where they are parsed and re-emitted on the host's
`EventBus`. Subscribers and `on_event` callbacks therefore see a unified event stream regardless
of where assets actually ran.
