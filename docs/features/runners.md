# Runners

Runners orchestrate the execution of assets in a DAG. They handle dependency ordering,
concurrency, and error handling. The execution engine is **async-native**: `runner.run(...)` is
a coroutine, so `await` it inside an event loop or drive it with `asyncio.run(...)`.

## Quick start

The simplest way to run a DAG is `dag.materialize()`, which uses an `AsyncRunner` internally:

```py
import asyncio

dag = il.DAG(source)
result = asyncio.run(dag.materialize())
```

For more control, use a runner directly:

```py
result = asyncio.run(il.SerialRunner().run(dag))
```

All runners take a partition or window as the second argument to `run()`:

```py
result = await il.AsyncRunner().run(dag, il.TimePartition(dt.date(2025, 1, 15)))
```

## SerialRunner

Executes assets one at a time in dependency order. Best for debugging and deterministic
execution. It's an `AsyncRunner` pinned to a single worker.

```py
result = await il.SerialRunner().run(dag)
```

## AsyncRunner

The default. Runs independent assets concurrently on the event loop, bounded by `max_workers`.
Synchronous asset functions are offloaded to threads; `async def` assets run natively.

```py
result = await il.AsyncRunner(max_workers=4).run(dag)
```

Assets are scheduled dynamically: as soon as an asset's dependencies are satisfied, it is
submitted for execution.

## MultiProcessRunner

Executes assets in separate processes. Useful for CPU-bound workloads or to bypass the GIL.

```py
result = await il.MultiProcessRunner(max_workers=4).run(dag)
```

!!! note

    The multi-process runner serializes each asset (and its ancestors) to send it across the
    process boundary, then reconstructs and materializes it in the child. All assets and
    destinations must be serializable. Run it under `if __name__ == "__main__":`.

## Docker & Kubernetes runners

Available via extension packages; each asset (with its ancestors) executes in an isolated
container or Job, streaming events back to the host.

```py
from interloper_docker.runner import DockerRunner

runner = DockerRunner(image="interloper:latest-worker", max_containers=4)
result = await runner.run(dag)
```

```py
from interloper_k8s.runner import KubernetesRunner

runner = KubernetesRunner(image="my-repo/interloper:latest", namespace="data", max_jobs=4)
result = await runner.run(dag)
```

## Options

Common runner options:

| Option | Default | Description |
|--------|---------|-------------|
| `max_workers` | `4` (`1` for `SerialRunner`) | Concurrency (named `max_containers` / `max_jobs` for Docker/K8s) |
| `fail_fast` | `True` (`False` for Docker/K8s) | Stop the run on the first failure vs. continue |
| `reraise` | `False` | Re-raise exceptions vs. capture them in the result |
| `on_event` | `None` | Callback invoked for each lifecycle event |

```py
runner = il.AsyncRunner(max_workers=8, fail_fast=False, on_event=print)
result = await runner.run(dag)
```

## RunResult

Every runner returns a `RunResult`:

```py
result.status            # ExecutionStatus (COMPLETED, FAILED, ...)
result.execution_time    # Total execution time in seconds
result.partition_or_window
result.asset_executions  # dict[str, AssetExecutionInfo] keyed by asset id

result.completed_assets  # list[str] of asset keys that completed
result.failed_assets     # list[str] of asset keys that failed
result.canceled_assets   # list[str] of asset keys canceled downstream of a failure
```

Each `AssetExecutionInfo` carries:

```py
info.asset_key
info.status            # ExecutionStatus
info.start_time        # datetime | None
info.end_time          # datetime | None
info.execution_time    # seconds (end - start) | None
info.error             # error message if failed
info.traceback         # traceback string if failed
```

`ExecutionStatus` members: `QUEUED`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED`,
`CANCELED`.

## Partitioned & backfilled runs

Pass a partition or window to the runner. A `TimePartitionWindow` runs the DAG once per date —
see [Backfilling](backfilling.md):

```py
result = await il.SerialRunner().run(
    dag,
    il.TimePartition(dt.date(2025, 1, 15)),
)
```
