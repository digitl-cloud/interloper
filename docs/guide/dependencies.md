# Dependencies

An asset can consume the output of other assets. Inside a DAG, each upstream dependency is
materialized first, read back from its destination, and passed to the downstream asset as a
function argument.

## Inside a source

Name a parameter after a sibling asset and the dependency is inferred:

```py
import interloper as il

@il.source
class Shop(il.Source):
    @il.asset
    def users(self) -> list[dict]:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(self, users: list[dict]) -> list[dict]:
        return [{"count": len(users)}]
```

A parameter with a `None` default is an **optional** dependency:

```py
@il.asset
def report(self, users: list[dict], segments: list[dict] | None = None) -> list[dict]:
    ...
```

Inference records the contract on the asset class as `requires` (`{"users": "shop.users"}`)
and `optional_requires`. Parameters that name a resource slot or the context are never treated
as dependencies.

## Explicit contracts

When the parameter name does not match, or the upstream lives in another source, declare the
mapping on the decorator. Values are asset keys, bare (same source) or qualified
(`source_key.asset_key`):

```py
@il.source
class Finance(il.Source):
    @il.asset(requires={"orders": "shop.orders"}, optional_requires={"fx": "rates.daily_fx"})
    def revenue(self, orders: list[dict], fx: list[dict] | None = None) -> list[dict]:
        ...
```

A cross-source contract is checked, not wired: the source only resolves keys that belong to
itself, and nothing looks `shop.orders` up across sources. Wire the edge by instance id before
building the DAG, and the DAG validates it against the contract:

```py
shop, finance = Shop(...), Finance(...)
finance.revenue.dependencies["orders"] = shop.orders.id
dag = il.DAG(shop, finance)
```

In a spec file the same edge is an `id` on the upstream asset and a `dependencies` entry on the
downstream one; see [Specs](specs.md).

## How wiring works

Each asset instance carries `dependencies`, a mapping from parameter name to the upstream
asset's **instance id**. The source fills it for intra-source contracts at construction; the DAG
checks every entry at build time:

- A mandatory dependency whose id is not in the DAG raises `DependencyNotFoundError`.
- An optional dependency whose id is missing is skipped and the parameter receives `None`.
- A wired upstream whose identity does not match the declared key raises
  `DependencyContractError` (for example, `requires={"orders": "shop.orders"}` wired to an
  asset from another source).

Wiring by hand is possible, for example to connect a standalone asset:

```py
extra = extra_asset(destinations=dest)
source.report.dependencies["data"] = extra.id
dag = il.DAG(source, extra)
```

Persisted dependencies (from a stored spec) are never overwritten by inference.

## Reading upstream data

At run time the downstream asset reads each dependency from the upstream asset's **first
resolved destination**, scoped to the partition the upstream consumes. The read returns whatever
that destination's `read()` yields: rows for the built-in destinations, a DataFrame for
DataFrame-native ones. A failed mandatory read raises `AssetError`; a failed optional read
yields `None`.

Reads emit `dest_read_*` events and an `interloper.destination.read` span.

## Rules the DAG enforces

| Rule | Error |
|------|-------|
| Every operation id is unique | `DAGError` |
| Mandatory dependencies resolve to a node in the DAG | `DependencyNotFoundError` |
| Wired upstreams satisfy the declared contract | `DependencyContractError` |
| No cycles | `CircularDependencyError` |
| A non-partitioned asset never depends on a partitioned one | `DAGError` |

The last rule exists because a partitioned upstream is read for one partition at a time, which
an unpartitioned downstream cannot express.

## Running one asset with its parents

`dag.mini_dag(asset_id)` builds a DAG containing one asset and its immediate parents marked
non-materializable. Only the target executes; parents are read, not rewritten. Sources offer the
same idea through `select`.
