# Hooks

Hooks react to what other components do. A hook is a component kind — declarative intent
("when X happens to what I watch, do Y") that the scheduler acts on, exactly like a job's
cron expression. The declaration is inert: the framework carries it, the scheduler evaluates
terminal runs against it, and the hook class owns the side effect.

```py
import interloper as il

hook = il.TriggerHook(
    watches=[upstream_source],
    targets=[downstream_job],
    events=["run_completed"],
)
```

A hook declares three things:

| Field | Meaning |
|-------|---------|
| `watches` | The components to observe — sources, assets, or jobs. A hook watching a source also matches runs of that source's assets. |
| `events` | Which run outcomes matter: `run_completed`, `run_failed` (default: `run_failed`). |
| `enabled` | Disabled hooks are kept but never fired. |

Trigger-style hooks additionally declare `targets` — the components they act on.

## Built-in hooks

**`TriggerHook`** — the cascading-pipelines primitive. When a watched component's run reaches
a matching outcome, it queues a run for every target, propagating the originating run's
partition date so cascading pipelines stay on the same partition:

```py
il.TriggerHook(watches=[ingest_job], targets=[transform_job], events=["run_completed"])
```

**`WebhookHook`** — POSTs a fixed JSON document to a URL, so receivers integrate against one
stable shape:

```py
il.WebhookHook(watches=[ingest_job], events=["run_failed"], url="https://ops.example.com/alerts")
```

```json
{
  "event_type": "run_failed",
  "component_id": "…",
  "run_id": "…",
  "partition_date": "2026-01-01",
  "hook_id": "…",
  "metadata": {"status": "failed"}
}
```

## Custom hooks

Subclass `il.Hook` and implement `fire(context)` — the class owns the side effect, and
credentials ride the existing [resource](resources.md) mechanism (encrypted at rest like any
connection):

```py
import interloper as il

class SlackHook(il.Hook):
    channel: str

    def fire(self, context: il.HookContext) -> None:
        post_message(self.channel, f"{context.event_type} on {context.component_id}")
```

The context carries the event's identity plus the capabilities the scheduler injects:

| Attribute | Meaning |
|-----------|---------|
| `event_type` | The matched outcome (`run_completed`, `run_failed`). |
| `component_id` | The watched component the run targeted. |
| `run_id` | The terminal run. |
| `partition_date` | The run's partition date, if any. |
| `metadata` | Event details (e.g. the run's status). |
| `trigger` | Capability to queue a run for a component id — how `TriggerHook` acts without any persistence dependency. |

A subclass may extend the relation vocabulary — this is how `TriggerHook` adds the `target`
verb. Declarations are **extend-only**: they merge over the anchor's vocabulary (redeclaring
a type replaces its definition; nothing is ever removed), so each class's definition
advertises exactly the relations it acts on.

Like any component kind, custom hook classes become catalog components by declaring them
under the entry-point group — see [Catalog](catalog.md).

## Delivery semantics

The scheduler's hook evaluator sweeps recently-terminal runs on a watermark with an overlap
window. The guarantees:

- **At-most-fired-once.** Every firing is claimed by an event row with a deterministic id, so
  re-evaluated runs (overlap, restarts) never re-fire a hook.
- **Failures are recorded, not retried.** A `fire()` that raises is written to the same claim
  as `hook_failed` and shows up in the events UI.
- **Downtime is not replayed.** The watermark starts at scheduler boot; runs that ended while
  the scheduler was down don't fire hooks on startup.
- **No self-triggering.** A trigger into the hook's own watch set (directly or via a parent
  source) is refused — each such run would re-fire the hook forever. Cycles across *multiple*
  hooks remain the operator's responsibility, like any recursive schedule.

Every firing stamps the hook's machine-owned state (`last_fired_at`, `last_run_id`), which the
app surfaces on the hooks page.

!!! note
    Hooks react to *persisted, terminal runs* in a deployed instance. For observing execution
    in-process (asset lifecycle, destination I/O), use the [event system](events.md).
