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

Inference records the contract on the asset class as `depends_on` (`{"users": "shop.users"}`);
a `None` default records `il.Dependency(key="shop.segments", optional=True)`. Parameters that
name a resource slot or the context are never treated as dependencies.

## Explicit contracts

When the parameter name does not match, or the upstream lives in another source, declare the
upstream on the decorator. A plain string is a single, non-optional upstream on that key;
`il.Dependency` adds the optional and many-valued cases:

```py
@il.source
class Finance(il.Source):
    @il.asset(depends_on={"orders": "shop.orders", "fx": il.Dependency(key="rates.daily_fx", optional=True)})
    def revenue(self, orders: list[dict], fx: list[dict] | None = None) -> list[dict]:
        ...
```

Keys are bare (`orders`, a sibling), qualified (`shop.orders`, that source type, any instance) or
wildcard (`*.orders`, that asset key from any source). `depends_on` is the contract; the wiring that
satisfies it lives in `upstreams` on the instance (parameter name to a list of upstream ids), and
the platform stores it as `upstream` relations.

The DAG wires a qualified key when exactly one asset in the DAG satisfies it:

```py
dag = il.DAG(Shop(...), Finance(...))     # revenue.upstreams == {"orders": [shop.orders.id]}
```

Two matching assets (two `Shop` instances) raise `DAGError` and ask for explicit wiring by id;
none raise `DependencyNotFoundError` at build time, never a `TypeError` inside `data()`. A `data()`
parameter that is neither the context, a resource nor a declared upstream, and has no default, is
an `AssetError` at build time for the same reason.

In a spec file the same edge is an `id` on the upstream asset and an `upstreams` entry on the
downstream one; see [Specs](specs.md).

## Many upstreams

A slot can bind every asset matching a key. Declare it with `many=True`; `data()` receives a list
of `il.Upstream`, each carrying the upstream asset and its data:

```py
@il.asset(
    depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
    partitioning=il.TimePartitionConfig(column="date"),
)
def campaign_matches(context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]:
    return [
        {"date": context.partition_date, "provider": leg.asset.source.key, "rows": len(leg.data)}
        for leg in campaigns
        if leg.data is not None
    ]
```

The DAG binds every match it holds, and explicit wiring (`upstreams["campaigns"] = [id1, id2]`, or
a list in a spec) is kept as is. `optional=True` allows an empty list. A bound leg arrives with
`data` set to `None`, and a `LOG` warning event names it, when the upstream has nothing
materialized where its destination looks: no table or object for that scope at all. An existing
but empty scope is not that case; it arrives as whatever the destination returns for an empty
read (an empty list or frame), not `None`. `optional` only governs wiring, never this; the asset
decides what a missing leg means. Any other read failure fails the asset. The same rule holds for
a single slot: it receives `None` under the same no-table-or-object condition, whether or not the
slot is optional.

## How wiring works

Each asset instance carries `upstreams`, a mapping from parameter name to the upstream
assets' **instance ids** (a list, one entry for a single slot). The source fills it for
intra-source contracts at construction; the DAG checks every entry at build time:

- A non-optional slot with nothing wired raises `DependencyNotFoundError`.
- An optional slot with nothing wired is skipped: the parameter receives `None` (an empty list
  for a many slot).
- A wired upstream whose identity does not match the declared key raises
  `DependencyContractError` (for example, `depends_on={"orders": "shop.orders"}` wired to an
  asset from another source).

Wiring by hand is possible, for example to connect a standalone asset:

```py
extra = extra_asset(destinations=dest)
source.report.upstreams["data"] = [extra.id]
dag = il.DAG(source, extra)
```

Persisted upstreams (from a stored spec) are never overwritten by inference.

## Reading upstream data

At run time the downstream asset reads each dependency from the destination named by the
upstream's `default_destination_key`, or its first destination, scoped to the partition the
upstream consumes. The read returns whatever that destination's `read()` yields: rows for the
built-in destinations, a DataFrame for DataFrame-native ones. `optional` is a wiring rule, not a
data rule: for every slot, optional or not, single or many, a leg is `None` (a `None` leg for a
many slot, a `None` argument for a single slot) only when the upstream has nothing materialized
where the destination looks — no table or object for that scope at all (`MemoryDestination` and
the file destinations key storage per partition, so a missing partition is exactly this case;
`BigQueryDestination` reaches it only when the table itself does not exist). An existing scope
that simply has no rows for the partition is not this case: it returns whatever the destination
gives back for an empty read (an empty list or frame), never `None`. Either way a `LOG` warning
event names the upstream when the leg is `None`, since data is expected to be occasionally
missing. Any other read failure fails the asset.

Reads emit `dest_read_*` events and an `interloper.destination.read` span.

## Rules the DAG enforces

| Rule | Error |
|------|-------|
| Every operation id is unique | `DAGError` |
| A declared single slot matches at most one asset in the DAG | `DAGError` |
| Every non-default `data()` parameter is the context, a resource or a declared upstream | `AssetError` |
| Non-optional slots are wired to a node in the DAG | `DependencyNotFoundError` |
| Wired upstreams satisfy the declared key | `DependencyContractError` |
| No cycles | `CircularDependencyError` |
| A non-partitioned asset never depends on a partitioned one | `DAGError` |
| Time-partitioned ends of an edge share a granularity | `DAGError` |

The rule that a non-partitioned asset never depends on a partitioned one exists because a
partitioned upstream is read for one partition at a time, which an unpartitioned downstream
cannot express. A run has one partition scope, so both ends of an edge must agree on
granularity, read-only upstreams included.

## Running one asset with its parents

`dag.mini_dag(asset_id)` builds a DAG containing one asset and its immediate parents marked
non-materializable. Only the target executes; parents are read, not rewritten. Sources offer the
same idea through `select`.
