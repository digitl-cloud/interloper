# Dependencies

An asset can consume the output of other assets. Inside a DAG, each upstream is materialized
first, read back from its destination, and handed to the downstream asset as an `il.Upstream`:
the upstream asset plus the data read for the run's scope.

An upstream is one **relation** among the others a component declares (a connection, a config, a
destination). One primitive, `il.Relation`, declares them all; only the ones whose kind is
`asset` become edges in the graph. See [Resources](resources.md) for the rest.

## Inside a source

Annotate a parameter `il.Upstream` and name it after a sibling asset. The source binds it when
it builds its assets:

```py
import interloper as il

@il.source
class Shop(il.Source):
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "id": 1, "total": 9.99}]

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def order_stats(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:
        rows = orders.data or []
        return [{"date": context.partition_date, "count": len(rows)}]
```

The parameter name is the relation name and the bare asset key it expects, so
`orders: il.Upstream` declares `il.Relation("asset", "orders")`, a sibling of the same source
instance. A `None` default makes the relation **optional**, meaning its wiring may be absent, and
declares `on_delete="detach"` with it: a parameter that tolerates a missing leg has no claim on
the upstream, so deleting that upstream is allowed and simply drops the wiring, where a required
upstream refuses the deletion while it is bound.

```py
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def report(
        self,
        context: il.ExecutionContext,
        orders: il.Upstream,
        refunds: il.Upstream | None = None,
    ) -> list[dict]:
        ...
```

`self`, `context`, `source` and `**kwargs` are reserved. Any other parameter that is neither an
`il.Upstream` nor a component class is a `TypeError` at class creation: nothing could ever fill
it.

## Declaring relations explicitly

When the parameter name is not the upstream's key, or the upstream lives in another source,
write the relation on the decorator. `relations=` is keyed by parameter name and wins over the
annotation:

```py
@il.source
class Finance(il.Source):
    currency: str = il.InputField(default="EUR")

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        relations={
            "orders": il.Relation("asset", "shop.orders"),
            "fx": il.Relation("asset", "rates.daily_fx", optional=True),
        },
    )
    def revenue(
        self,
        context: il.ExecutionContext,
        orders: il.Upstream,
        fx: il.Upstream | None,
    ) -> list[dict]:
        ...
```

A class-based asset writes the same relation as a typed class attribute:

```py
class Revenue(il.Asset):
    orders: il.Asset = il.Relation("asset", "shop.orders")
    partitioning = il.TimePartitionConfig(column="date")

    def data(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:
        ...
```

The annotation is for the type checker; the relation itself says what it accepts. Keys come in
four forms:

| Key | Selects |
|-----|---------|
| `orders` | The asset of that key in the declaring asset's own source instance. |
| `shop.orders` | That asset key, in any instance of the source keyed `shop`. |
| `*.orders` | That asset key, in any source at all. |
| `["shop.orders", "wms.orders"]` | Any of the listed keys. |

## Many upstreams

`many=True` binds every matching asset at once, and `data()` receives a `list[il.Upstream]`:

```py
@il.source(tags=["Analytics"])
class CampaignMatcher(il.Source):
    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)},
    )
    def campaign_matches(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
    ) -> list[dict]:
        return [
            {"date": context.partition_date, "source_key": leg.asset.source.key, **row}
            for leg in campaigns
            if leg.data is not None
            for row in leg.data
        ]
```

The DAG binds every `campaigns` asset it holds, so the set of legs is decided by wiring, not by
the class. `list[il.Upstream]` on its own infers the same relation on the parameter's bare key,
which is a many-valued sibling; `relations=` is what reaches outside the source.

## How wiring works

Wiring is always an instance bound to a relation, never an id. It happens in three places:

- **At construction**, for siblings: `Source.sibling_bindings()` says which of an asset's
  relations resolve inside the source, and the source binds them to the instances it just built,
  right after building them.
- **In the DAG**, for everything a bare key cannot reach: a qualified or wildcard key is matched
  against the DAG's other assets. A `many` relation binds every candidate, a single-valued one
  the only candidate there is. Several candidates for a single-valued relation raise `DAGError`
  and ask to be bound by hand.
- **By hand**, at any point:

```py
dest = il.MemoryDestination()
shop = Shop(destinations=[dest])
fin = Finance(destinations=[dest])

dag = il.DAG(shop, fin)          # binds fin.revenue.orders to shop.orders
fin.revenue.bound("orders")      # the shop.orders instance

fin.revenue.bind("orders", shop.orders)     # explicit; accumulates for a many relation
fin.revenue.orders = shop.orders            # assignment replaces atomically
fin.revenue.unbind("orders", shop.orders)   # refused if it would empty a non-optional relation
```

Binding goes through `bind()` everywhere, so an explicit binding is never overwritten by the
DAG, and a hydrated one is never overwritten by construction.

In a spec, a bound upstream is a `{ref: id}` under the relation's name, since an asset always
travels inside its own source's document. See [Specs](specs.md).

## Reading upstream data

Each leg is read from the upstream's `default_destination_key` destination, or its first, scoped
to the partition the run consumes. `il.Upstream` carries two attributes:

| Attribute | Meaning |
|-----------|---------|
| `asset` | The upstream asset the data came from; its `source`, `key` and `id` tell the legs apart. |
| `data` | Whatever that destination's `read()` yields: rows for the built-in destinations, a DataFrame for DataFrame-native ones. |

`data` is `None`, with a `LOG` warning naming the leg, when the upstream has nothing
materialized where the destination looks: no table or object for that scope at all
(`MemoryDestination` and the file destinations key storage per partition, so a missing partition
is exactly this; `BigQueryDestination` reaches it only when the table itself does not exist). An
existing but empty scope is not this case, and comes back as whatever the destination returns
for an empty read, an empty list or frame. `optional` governs wiring only, never data: the asset
decides what a missing leg means. Any other read failure fails the asset.

A bound upstream the run does not hold at all is skipped with its own warning, so its leg simply
does not exist.

Reads emit `dest_read_*` events and an `interloper.destination.read` span.

## Rules the DAG enforces

| Rule | Error |
|------|-------|
| Every operation id is unique | `DAGError` |
| A single-valued relation matches at most one asset in the DAG | `DAGError` |
| Every `data()` parameter is fillable (`context`, `source`, a component class or `il.Upstream`) | `TypeError`, at class creation |
| A non-optional relation is bound, unless it can fill itself | `ConfigError` |
| A bound target is one its relation accepts | `ConfigError` |
| A non-optional relation points at an asset the DAG holds | `ConfigError` |
| No cycles | `CircularDependencyError` |
| A non-partitioned asset never depends on a partitioned one | `DAGError` |
| Time-partitioned ends of an edge share a granularity | `DAGError` |

`validate_relations(nodes)` is the check behind the three `ConfigError` rows. Constructing a
component binds what it is given and checks each binding, but not that every required relation is
bound: components are wired piecewise (a source trickles into its assets, a manifest binds its
references once every component exists), so the check runs where the graph is whole. The DAG runs
it with its own nodes; loading a manifest runs it without `nodes` on each root, which is what lets
a relation only the graph can fill stay unbound until then.

A non-partitioned asset never depends on a partitioned one because a partitioned upstream is
read one partition at a time, which an unpartitioned downstream cannot express. A run has one
partition scope, so both ends of an edge must agree on granularity, read-only upstreams
included.

## Running one asset with its parents

A bound upstream the run does not materialize joins the DAG anyway, as a non-materializable copy
under the same id: it is a live instance with its own destinations, so it can be read without
being run. That is the whole mechanism behind running one asset with its parents:

```py
mini = dag.mini_dag(fin.revenue.id)
[(op.qualified_key, op.materializable) for op in mini.operations]
# [("finance.revenue", True), ("shop.orders", False)]
```

Only the target executes; the parents are read, not rewritten. Partition checks still apply
along those edges. A source offers the same idea over its own assets through `select`.
