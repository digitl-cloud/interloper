# Events

::: interloper.EventType

## Console rendering levels

`ConsoleEventHandler` maps types to logging levels: failures at `ERROR`, `operation_canceled`
at `WARNING`, `operation_queued` and all asset-data and destination I/O events at `DEBUG`,
everything else at `INFO`. `log` events use their own `level`.

## Serialization

::: interloper.Event

::: interloper.EventBus
