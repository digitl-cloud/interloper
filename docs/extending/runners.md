# Runners

A runner walks a DAG: it decides which operations are ready, executes them within a concurrency
budget, records outcomes, and returns a `RunResult`. The core ships in-process and
multi-process runners; container runners in companion packages are built on the same base. This
page describes the contract for writing one.

## The base class

`il.Runner` is `Serializable`, so a runner's configuration is fields on the class. It owns the
public `run()`:

1. Assign a `run_id` when the metadata carries none.
2. Subscribe the `on_event` handler, filtered to this run's events.
3. Open the `interloper.runner.run` span, restoring a parent context from metadata or the
   environment.
4. Delegate to `_run()`.
5. Flush the event bus and unsubscribe.

Subclasses implement `_run(dag, partition_or_window, metadata) -> RunResult`. Shared helpers:

| Helper | Purpose |
|--------|---------|
| `_init_run(dag, scope, metadata)` | Preflight validation, then create the `RunState` and emit `RUN_STARTED` and `OPERATION_QUEUED`. |
| `_finalize_run(error=None)` | Emit the terminal run event and build the `RunResult`. |
| `_reraise_first_failure()` | Re-raise the first failed operation's exception (or `RunnerError` with its message). |
| `_on_start()`, `_on_end()` | Lifecycle hooks around the walk (create and shut down pools). |
| `state` | The `RunState`. |

## RunState

`RunState` tracks per-operation `ExecutionInfo` and answers scheduling questions:

```py
state.ready_operations        # dependencies satisfied, not yet submitted
state.running_operations
state.failed_operations
state.is_run_complete()       # every operation terminal

state.mark_running(operation)
state.mark_completed(operation, effects=result)
state.mark_failed(operation, error, tb=..., effects=..., exception=...)
state.mark_canceled(operation)
```

Completing an operation promotes its dependents to ready; failing one cancels everything
downstream. Non-materializable operations start as `SKIPPED` and count as satisfied
predecessors. Each transition emits the matching `OPERATION_*` event with a deterministic id
(`emit=False` skips the emission for runners whose child process emits it itself). All
mutations happen on the event loop thread, so no locking is needed.

## Two shapes

**In-process, async-native**: extend `AsyncRunner` or copy its loop. It submits ready
operations as asyncio tasks bounded by a semaphore and executes each through
`operation.execute(OperationContext(...))`, consulting `operation.failure(error)` on exceptions.

**Out-of-process**: extend `SyncRunner`, which offloads a blocking, `concurrent.futures`-based
walk to a worker thread. Implement:

| Member | Role |
|--------|------|
| `_capacity` (property) | Maximum operations in flight. |
| `_submit_operation(operation, scope) -> Future` | Hand the operation to your executor (a process pool, a container API) and return a future. Call `state.mark_running` yourself. |
| `_handle_completed(future, operation)` | Interpret the future's result and call `mark_completed` or `mark_failed`. The default treats a raising future as failure. |
| `_handle_flushed(future, operation)` | Same, for futures collected while flushing after a fail-fast break or an abort. |
| `_on_start()`, `_on_end()` | Create and tear down the executor. |

`MultiProcessRunner` is the reference: it serializes the DAG to a `DAGSpec` once, submits
`(operation_id, dag_spec, scope, metadata)` to a process pool, and its worker reconstructs the
DAG, executes the node and returns `(id, success, error, traceback, effects)`. Effects travel
back as plain dicts and are re-wrapped in `OperationResult`.

## Registering

```toml
[project.entry-points."interloper.runners"]
ray = "my_package.runner:RayRunner"
```

`il.Runner.from_settings(settings.runner)` resolves `runner.type` in the `RUNNERS` registry and
constructs the class with `runner.config`, so a registered runner is selectable from
`interloper.yaml` and the CLI without further wiring.

## Events from children

A child process has its own event bus. Have it run under `INTERLOPER_EVENTS_TO_STDERR=true` (the
CLI then subscribes a `StderrEventHandler`), read its stderr on the host, and turn
`@EVENT:` lines back into events with `Event.from_log_line()` and `EventBus.emit_event()`.
Deterministic operation event ids make the host's own terminal events and the child's collapse
into one when persisted. Pass `child_process_env()` into the child for trace continuity.
