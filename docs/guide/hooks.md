# Hooks

A hook reacts to what other components do: it declares **what to watch**, **which outcomes
matter**, and owns a side effect. Like a job's cron, the declaration is inert intent that a
platform scheduler evaluates against finished runs. The core defines the classes and the
contract.

## Declaring a hook

```py
import interloper as il

alert = il.WebhookHook(
    watches=[ingest_job],
    events=["run_failed"],
    url="https://ops.example.com/alerts",
)
```

| Field | Default | Meaning |
|-------|---------|---------|
| `watches` | `[]` | Sources, assets or jobs to observe. |
| `events` | `["run_failed"]` | Run outcomes that fire the hook: `run_completed`, `run_failed`. At least one. |
| `enabled` | `True` | A disabled hook is kept but never fires. |

## Built-in hooks

**`TriggerHook`** runs other components when a watched outcome matches. It is the primitive for
cascading pipelines:

```py
il.TriggerHook(watches=[ingest_job], targets=[transform_job], events=["run_completed"])
```

Its `targets` are sources, assets or jobs. Firing goes through the `trigger` capability the
operator injects into the context, so the hook has no persistence dependency of its own; firing
without one raises `ConfigError`.

**`WebhookHook`** POSTs a fixed JSON document to `url` with a `timeout` (default 10 seconds).
A non-success response raises:

```json
{
  "event_type": "run_failed",
  "component_id": "…",
  "run_id": "…",
  "partition_key": "2026-01-15",
  "hook_id": "…",
  "metadata": {}
}
```

## Custom hooks

Subclass `il.Hook` and implement `fire(context)`. Configuration fields and resource slots work
as on any component, so credentials ride a connection:

```py
class EmailHook(il.Hook):
    recipient: str = il.InputField(description="Address to notify")
    smtp: SmtpConnection

    def fire(self, context: il.HookContext) -> None:
        self.smtp.send(self.recipient, f"{context.event_type} on {context.component_id}")
```

`HookContext` carries:

| Attribute | Meaning |
|-----------|---------|
| `event_type` | The matched outcome. |
| `component_id` | The watched component whose run ended. |
| `run_id` | The run. |
| `partition_key` | The run's partition id, if any. |
| `metadata` | Event details supplied by the operator. |
| `trigger` | A callable creating a run for a component id, when the operator provides it. |

A hook that acts on other components extends the relation vocabulary the way `TriggerHook`
does, adding a `target` relation and a `targets` field; the base hook only observes. Custom hook
classes join the [catalog](catalog.md) through the `interloper.components` entry point like any
component.

`HookState` (`last_fired_at`, `last_run_id`) is the hook's machine-owned state, stamped by the
operator on every firing.

## Scope

Hooks fire on **persisted, terminal runs** evaluated by a scheduler. For observing execution
inside a process (asset lifecycle, destination I/O, log lines), use the
[event bus](events.md).
