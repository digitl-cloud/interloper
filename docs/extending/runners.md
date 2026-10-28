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
from interloper.runner import AsyncRun


class DeadlineRun(AsyncRun):
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
| `attempt(operation) -> OperationResult` | Make one attempt at an operation where this kind of run executes it, and return what it came to. An attempt's own failure is a result with its `error` and `retryable` set, never an exception; an `attempt` that raises is a fault of the run itself and fails the whole run. |
| `open()` | Acquire what the run holds from outside the process (a client, a connection). Optional; the walk calls it after `RUN_STARTED`, so a failure fails the run with its events, like any other fault of the run. |
| `close()` | Release what the run holds (pools, clients). Optional; a failure here is logged and never masks the run's result. |

What a run holds while it lasts (a pool, a client) is its attributes. Local ones (a pool) are
set up in `__init__` after `super().__init__(...)`, which checks the run's window against every
enabled partitioned operation before anything is emitted; anything that reaches outside the
process (a Docker daemon, a cluster) is acquired in `open`, where a failure is reported.

## The walk

`Run.walk()` is the same for every kind of run. It emits `RUN_STARTED` and `OPERATION_QUEUED`,
starts every ready operation while fewer than `capacity` are in flight, records each result as it
lands, stops starting new ones after a failure under `fail_fast` (what is in flight always
finishes), cancels whatever never started, and emits `RUN_COMPLETED` or `RUN_FAILED`.

The walk retries every kind of run the same way: it makes attempts under the operation's
`RetryPolicy` while each failed attempt is `retryable`, reporting each one. An in-process attempt
is `OperationResult.from_attempt(operation, context)`: it executes the operation inside its span
and turns a failure into the operation's `failure(error)` result, completed with the traceback
(when the operation's class allows one), the original exception, so `reraise` can raise it, and
whether `operation.retryable(error)` thinks another attempt may help.

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

The walk calls every transition; a run's `attempt` calls none. Non-enabled operations start as `SKIPPED` and count
as satisfied predecessors. `RunState` is not thread-safe: call it from the run's event loop only,
never from a thread an `attempt` hands work to.

## Registering

```toml
[project.entry-points."interloper.runners"]
deadline = "my_package.runner:DeadlineRunner"
```

`il.Runner.from_settings(settings.runner)` resolves `runner.type` in the `RUNNERS` registry and
constructs the class with `runner.config`, so a registered runner is selectable from
`interloper.yaml` and the CLI without further wiring.

## Launchers

A runner decides where a run's operations execute; a launcher decides where the run itself
executes. The scheduler's queue claims a run, mints its token, hydrates its target into a DAG
spec, and calls `launch(run, spec, token)` on the configured `Launcher`. A launcher that starts
another process (a container, a pod) runs `Launcher.command(run, spec)` there, `spec` being the
spec's JSON or `@<path>` to a file holding it, with the run's token in `INTERLOPER_RUN_TOKEN` and
the API's `server.internal_url` in `INTERLOPER_SERVER_INTERNAL_URL`: that process holds no
database credentials and reports its events through the API. Register a launcher in the `interloper.launchers` entry-point group and
give it a `from_settings` classmethod, as the Docker and Kubernetes launchers do.

## Events from children

A run that makes each attempt in a child process (the Docker and Kubernetes runners) still owns
its operations' lifecycle: the host reports every start, retry and verdict, and the child only
what happens inside the operation. Have the child run
`interloper run --format inline --run-id=<id> <spec>` with `INTERLOPER_RUN_OPERATION` set to the
operation's id, and, on the platform, the host's `INTERLOPER_RUN_TOKEN` and
`INTERLOPER_SERVER_INTERNAL_URL`: the child then makes one attempt, reports that operation's
asset and destination events to the API, and drops every lifecycle event and its upstreams'
events. A failed attempt leaves its error and whether a retry may help at
`OperationResult.TERMINATION_MESSAGE_PATH`; the host reads it back with
`OperationResult.from_termination_message` once the child exits (Kubernetes surfaces it as the
container's termination message; a Docker host copies it out of the stopped container). A child
that dies without writing one fails its attempt with its exit code, and may be retried. Pass
`child_process_env()` into the child for trace continuity.

Run the child on the host's interloper version. The spec and the flags it receives are written
by the host's version and read by the child's, so a child image older than its host can refuse
a spec field or a flag it does not know yet. Upgrade the runner image together with the host,
and roll them back together.
