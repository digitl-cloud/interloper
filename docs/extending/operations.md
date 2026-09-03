# Operations & workloads

Runners do not know about assets. They execute **operations**: nodes the DAG orders, each with an
`execute()` and a `failure()`. Materializing an asset is one operation; renewing a connection's
credentials is another. A **workload** is anything a run may target: it flattens into operations.
Sources and jobs are workloads only; every operation is trivially the workload of itself.

## The contracts

```py
class Workload(ABC):
    billable: ClassVar[bool] = True          # do runs of this count against a quota?

    def operations(self) -> list[Operation]: ...


class Operation(Workload):
    capture_traceback: ClassVar[bool] = True # attach tracebacks to failure events?

    async def execute(self, context: OperationContext) -> OperationResult: ...
    def failure(self, error: Exception) -> OperationResult: ...
```

`OperationContext` carries the facts an execution is scoped to: `partition_or_window`, the
`dag` (how a node reaches upstream outputs), and the run `metadata`.

`OperationResult` is what an operation hands back for the platform to persist: `config` fields
to merge into the component's stored configuration, `state` fields to stamp onto its
machine-owned state, and an `error` message on failure. Effects are values; the core never holds
a handle to a store.

## The node protocol

Beyond the two methods, an operation exposes what the graph machinery reads. `Operation` gives
plain defaults that make any subclass a valid node; `Asset` overrides them with real fields:

| Member | Default | Asset |
|--------|---------|-------|
| `id`, `kind`, `key`, `qualified_key` | from `Component` | qualified with the source key |
| `materializable` | `True` | field |
| `dependencies` | `{}` | parameter name to upstream id |
| `optional_requires` | `{}` | class contract |
| `source` | `None` | the owning source |
| `partitioning` | `None` | the partition config |
| `effective_partition(scope)` | scope if partitioned else `None` | same |
| `validate_dependencies(nodes)` | no-op | checks `requires` contracts |
| `_event_metadata(metadata, scope)` | component identity | adds `qualified_key`, `source_id` |

## Writing an operation

A component that does work but produces no data is an operation without being an asset. The
core's `Connection` is the example: `execute()` runs `renew()` under a timeout and returns the
rotated credential fields as `config` effects plus the next due time as `state`; `failure()`
turns any exception into a credential-free message and a retry slot, and `capture_traceback` is
off because provider errors embed secrets in URLs.

```py
from interloper.utils import invoke

class Vacuum(il.Component, il.Operation):
    """Compacts a table after its asset ran."""

    table: str = il.InputField()
    warehouse: WarehouseConnection

    async def execute(self, context: il.OperationContext) -> il.OperationResult:
        freed = await invoke(self.warehouse.vacuum, self.table)
        return il.OperationResult(state={"last_vacuum_bytes": freed})

    def failure(self, error: Exception) -> il.OperationResult:
        return il.OperationResult(error=f"Vacuum of {self.table} failed: {type(error).__name__}")
```

`il.DAG(vacuum)` runs it like any node; wiring it after an asset is a `dependencies` entry.
`invoke` calls a sync or async callable uniformly.

## Where effects go

Runners record the returned `OperationResult` on the node's `ExecutionInfo.effects`. In the
core that is where it ends; the platform's run executor reads the effects after the run and
applies them to the stored component. The default `failure()` formats the exception with
pydantic input values stripped, so secrets in validation errors never reach an event.
