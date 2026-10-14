# Runners

A runner is configuration: how many operations run at once, where they execute, and what a
failure does. Each call to `runner.run(dag)` makes a `Run` from it, which walks the DAG once,
records what every operation came to, and returns a `RunResult`. A runner holds nothing per run,
so one runner serves any number of runs, concurrently too. This page describes the contract for
writing one.

## The two classes

A runner names its kind of run, and the run says where an operation executes. A run that gives
every in-process operation a deadline:

```py
import asyncio

import interloper as il
from interloper.runner import InProcessRun


class DeadlineRun(InProcessRun):
    """Every operation in this process, each within the runner's deadline."""

    async def execute(self, operation: il.Operation) -> il.OperationResult:
        try:
            return await asyncio.wait_for(super().execute(operation), self.runner.deadline)
        except asyncio.TimeoutError:
            return il.OperationResult(error=f"Operation '{operation.key}' exceeded {self.runner.deadline}s")


class DeadlineRunner(il.AsyncRunner):
    """An AsyncRunner whose operations each get a deadline."""

    Run = DeadlineRun

    deadline: float = 600.0
```

`Runner` is `Serializable`, so its configuration is fields on the class; a new kind of runner
subclasses `il.Runner`, declares `Run` and the `capacity` property (how many operations a run
executes at once), and inherits `fail_fast`, `reraise` and `on_event`. `Runner.run` does the rest:
it assigns a `run_id` when the metadata carries none, delivers the run's events to `on_event`,
opens the `interloper.runner.run` span (restoring a parent from the metadata or the
environment), makes the run, walks it, closes it, and re-raises when `reraise` asks.

A `Run` implements:

| Member | Role |
|--------|------|
| `execute(operation) -> OperationResult` | Execute one operation where this kind of run executes it, and return what it came to. An operation's own failure is a result with its `error` set, never an exception; an `execute` that raises is a fault of the run itself and fails the whole run. |
| `close()` | Release what the run holds (pools, clients). Optional; a failure here is logged and never masks the run's result. |
| `reports_progress` | Class attribute, `True` by default. Set it to `False` when each operation executes in a child process that reports its own start and retries (see below). |

What a run holds while it lasts (a pool, a client) is its attributes, set up in `__init__`
after `super().__init__(...)`, which checks the run's window against every enabled partitioned
operation before anything is emitted.

## The walk

`Run.walk()` is the same for every kind of run. It emits `RUN_STARTED` and `OPERATION_QUEUED`,
starts every ready operation while fewer than `capacity` are in flight, records each result as it
lands, stops starting new ones after a failure under `fail_fast` (what is in flight always
finishes), cancels whatever never started, and emits `RUN_COMPLETED` or `RUN_FAILED`.

`OperationResult.from_execution(operation, context, on_retry=..., on_restart=...)` is the attempt
loop an in-process run uses: it executes the operation inside its span, retries it under its
`RetryPolicy` while `operation.retryable(error)` agrees, and turns a final failure into the
operation's `failure(error)` result, completed with the traceback (when the operation's class
allows one) and the original exception, so `reraise` can raise it.

## RunState

`run.state` holds every operation's `ExecutionInfo` and moves it through one verb per transition,
each emitting its `OPERATION_*` event with a deterministic id:

```py
state.ready                        # dependencies met, not started
state.completed, state.failed      # in DAG order
state.done                         # every operation terminal
state.context(operation)           # the OperationContext it executes with

state.start(operation)             # RUNNING; OPERATION_STARTED
state.retry(operation, error)      # OPERATION_RETRIED; the next attempt
state.finish(operation, result)    # COMPLETED or FAILED; promotes or cancels its dependents
state.cancel_pending()             # cancels everything not yet terminal
```

The walk calls `start`, `finish` and `cancel_pending`; a run's `execute` only reports retries
(`retry`, then `start` for the next attempt). Non-enabled operations start as `SKIPPED` and count
as satisfied predecessors. `RunState` is not thread-safe: call it from the run's event loop only,
never from a thread an `execute` hands work to.

## Registering

```toml
[project.entry-points."interloper.runners"]
deadline = "my_package.runner:DeadlineRunner"
```

`il.Runner.from_settings(settings.runner)` resolves `runner.type` in the `RUNNERS` registry and
constructs the class with `runner.config`, so a registered runner is selectable from
`interloper.yaml` and the CLI without further wiring.

## Events from children

A run that executes each operation in a child process (the Docker and Kubernetes runners) sets
`reports_progress = False`: the child reports its operation's start, retries and asset steps
itself, and the host records them silently. Have the child run
`interloper run --format inline --run-id=<id> <spec>` under `INTERLOPER_EVENTS_TO_STDERR=true`
(the CLI then subscribes a `StderrEventHandler`), and feed its log through an
`EventLogReader(operation.id, label=...)`, which forwards that operation's `@EVENT:` lines to the
host's event bus. The host still records the verdict and emits the terminal event; deterministic
operation event ids make the host's and the child's collapse into one when persisted. Pass
`child_process_env()` into the child for trace continuity.
