# Execution

Assets execute in a **DAG** driven by a **runner**. The DAG orders operations by dependency; the
runner schedules ready operations, tracks their state, and returns a result. The engine is
async-native; every entry point has a sync form for scripts and notebooks.

## Building a DAG

```py
import interloper as il

dag = il.DAG(shop_source, finance_source, standalone_asset)
```

`DAG(*items)` accepts any **workload**: source or asset instances, definitions (instantiated for
you), and [jobs](jobs.md). Each is flattened into its operations. Construction validates the
graph and raises on duplicates, missing or mismatched dependencies, cycles, and an unpartitioned
asset downstream of a partitioned one (see [Dependencies](dependencies.md#rules-the-dag-enforces)).

```py
dag.operations                    # every node, in insertion order
dag.operation_map                 # id -> node
dag.topological_generations()     # lists of nodes that may run in parallel
dag.get_predecessors(asset.id)    # upstream ids
dag.get_successors(asset.id)      # downstream ids
dag.mini_dag(asset.id)            # one node plus read-only parents
```

Non-materializable nodes stay in the graph as dependencies but never execute and never appear
in the generations.

## Materializing

```py
result = dag.materialize()                                   # default AsyncRunner
result = dag.materialize(il.TimePartition(dt.date(2026, 1, 15)))
result = await dag.materialize_async(partition)
```

For a specific runner or its options:

```py
runner = il.AsyncRunner(max_workers=8, fail_fast=False)
result = il.run(runner.run(dag, partition, metadata={"run_id": "..."}))    # sync code
result = await runner.run(dag, partition)                                  # async code
```

`metadata` is carried onto every event and span of the run; a `run_id` is generated when
absent.

## The sync bridge

`il.run(coro)` drives any framework coroutine to completion from synchronous code. Unlike
`asyncio.run`, it works where a loop is already running (Jupyter) and reuses one persistent
background loop across calls, so loop-bound state such as an `AsyncRESTClient` cached on a
connection stays valid from one call to the next. Ctrl-C cancels the coroutine. Calling it from
code already on that loop raises `RuntimeError`; `await` there instead.

`asset.run()`, `asset.materialize()` and `dag.materialize()` are built on it.

## Runners

| Runner | Concurrency | Where operations run |
|--------|-------------|----------------------|
| `AsyncRunner(max_workers=4)` | asyncio tasks bounded by a semaphore | in-process, on the event loop; sync `data()` offloaded to threads |
| `SerialRunner()` | one at a time | `AsyncRunner` with a single slot |
| `MultiProcessRunner(max_workers=4)` | a process pool | child processes; the DAG is shipped as a spec and reconstructed there |

Companion packages register Docker and Kubernetes runners under the same interface; see
[Ecosystem](../reference/ecosystem.md).

Operations are scheduled dynamically: as soon as every predecessor has completed, an operation
becomes ready and is submitted while a slot is free. A failed operation cancels everything
downstream of it.

`MultiProcessRunner` requires every component in the DAG to be serializable and, like any
process pool, a `if __name__ == "__main__":` guard.

### Options

| Option | `AsyncRunner`, `MultiProcessRunner` | Meaning |
|--------|-------------------------------------|---------|
| `max_workers` | `4` | Concurrency ceiling. `SerialRunner` pins it to 1. |
| `fail_fast` | `True` | Stop submitting after the first failure and cancel everything else, in flight or still queued. `False` runs everything that can still run. |
| `reraise` | `False` | Re-raise the first failed operation's exception after the run is finalized. `False` returns a failed `RunResult` instead. |
| `on_event` | `None` | Callback receiving this run's [events](events.md). Subscribed for the duration of the run only. |

Runners are `Serializable`, so they can be configured from settings:
`il.Runner.from_settings(settings.runner)` resolves `runner.type` in the `RUNNERS` registry and
constructs it with `runner.config`.

## Results

```py
result.status              # ExecutionStatus.COMPLETED or FAILED
result.partition_or_window
result.execution_time      # seconds
result.executions          # id -> ExecutionInfo
result.completed_ids, result.failed_ids, result.canceled_ids
print(result)
# RunResult(status=failed, partition=2026-01-15, completed=2, failed=1, canceled=1, time=1.20s, failed=[...], canceled=[...])
```

`ExecutionInfo` carries `component_id`, `component_key`, `status`, `start_time`, `end_time`,
`execution_time`, `error`, `traceback`, and `effects` (what the operation asked the platform to
persist). `ExecutionStatus` is `QUEUED`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED`
(non-materializable), `CANCELED` (downstream of a failure).

## Failure handling

Asset failures are absorbed into the result: the node is marked failed with its message and
traceback, its dependents are canceled, and the run continues or stops depending on
`fail_fast`. With `reraise=True` the original exception is re-raised once the run is finalized,
so events and results are complete before it surfaces. A failure of the walk machinery itself
(a deadlock, an invalid graph) raises `RunnerError`.

Before anything executes, the runner validates the scope against every materializable
operation: partitioned operations without a scope, windows against operations that forbid them,
and time-partition mismatches fail the whole run up front.

## Running single assets

`asset.run()` and `asset.materialize()` bypass the runner. Pass the DAG when the asset has
dependencies, so upstream data can be read:

```py
dag = il.DAG(source)
source.report.materialize(partition, dag=dag)
```
