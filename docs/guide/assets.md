# Assets

An asset is the unit of data in Interloper: something that **produces data** and can be
**materialized** into destinations. This page covers how to define one, what it can receive,
how to run it, and what happens between the function returning and the data landing.

## Defining an asset

The usual form is a method on a [source](sources.md), decorated with `@il.asset`:

```py
import interloper as il

@il.source
class Shop(il.Source):
    @il.asset
    def users(self) -> list[dict]:
        return fetch_users()
```

A standalone asset is a decorated function:

```py
@il.asset
def users() -> list[dict]:
    return fetch_users()
```

And an asset can be a class, when it needs more than one method:

```py
class Users(il.Asset):
    def data(self, **kwargs) -> list[dict]:
        return fetch_users()
```

All three produce an `Asset` **definition** (a class). Calling it creates an **instance**:

```py
users_asset = users()
users_asset.key        # "users"
```

The key defaults to the snake_cased function or class name and can be overridden with
`key=`. The docstring becomes the asset's description in the catalog.

### Sync or async

The function may be sync or `async def`. Sync functions are offloaded to a worker thread; async
functions are awaited on the event loop. Either way the engine never blocks:

```py
@il.asset
async def events(self, connection: MyConnection) -> list[dict]:
    return await connection.client.get("/events")
```

### What an asset can return

Anything. Tabular data is what destinations and schemas are built for: a `list[dict]`, a
single `dict`, pydantic models or a list of them, a generator of rows, or a pandas DataFrame
when `interloper-pandas` is installed. Non-tabular objects pass through untouched when the asset
declares no schema, which is what a `FileDestination` storing pickles expects.

## Parameters

The engine inspects the function signature and fills each parameter from one of three places.

**`context`**: a parameter named `context` receives an [`ExecutionContext`](#execution-context).

**Resources**: a parameter annotated with a `Resource` subclass (a config, a connection, or
your own) receives a resolved instance. Declaring the slot explicitly with
`resources={"connection": MyConnection}` on the decorator does the same and wins over the
annotation. See [Resources](resources.md) for the resolution cascade.

**Dependencies**: any other parameter is an upstream asset. Inside a source, a parameter named
after a sibling asset is wired automatically; `requires` and `optional_requires` declare the
rest. See [Dependencies](dependencies.md).

```py
@il.asset(requires={"raw": "warehouse.raw_orders"})
def orders(
    self,
    context: il.ExecutionContext,      # the run's context
    connection: ShopConnection,        # a resource, by annotation
    users: list[dict],                 # a sibling asset, by name
    raw: list[dict],                   # a cross-source asset, by requires
) -> list[dict]:
    ...
```

`self`, `source` and `**kwargs` are ignored by the inspection.

## Decorator options

```py
@il.asset(
    key="ads_stats",                                   # override the derived key
    name="Ads statistics",                             # display name
    icon="carbon:chart-line",                          # icon identifier for UIs
    tags=["Report"],                                   # catalog tags
    schema=AdsStats,                                   # output schema
    partitioning=il.TimePartitionConfig(column="date"),
    destinations=[il.CSVDestination],                  # allowed destination classes
    resources={"connection": AdsConnection},           # explicit resource slots
    requires={"campaigns": "ads.campaigns"},           # mandatory upstream assets
    optional_requires={"budget": "finance.budget"},    # optional upstream assets
    materialization_strategy=il.MaterializationStrategy.RECONCILE,
    normalizer=il.Normalizer(flatten_max_level=1),
)
def ads_stats(self, context: il.ExecutionContext, connection: AdsConnection, campaigns, budget=None):
    ...
```

Every option is listed in [Decorator options](../reference/decorators.md). `destinations`
restricts the destination **classes** an asset accepts; instances are supplied at construction.

## Instance configuration

An instance carries the runtime state a definition does not know about:

| Field | Meaning |
|-------|---------|
| `destinations` | Destination instances to write to. A single destination is accepted and wrapped in a list. |
| `dataset` | Namespace (schema, folder) the asset materializes into. Defaults to the source's. |
| `default_destination_key` | With several destinations, the one downstream readers should prefer. Carried for the platform; the core reads upstream data from the first resolved destination. |
| `materializable` | `False` turns the asset into a read-only dependency: it is skipped by runners but its stored output is still readable. |
| `materialization_strategy` | How strictly the data is checked against the schema. |
| `normalizer` | The normalizer applied before conform. |
| `dependencies` | Parameter name to upstream asset **id**. Filled by the source; can be set by hand. |
| `id` | Instance identity, a UUID by default. |
| `resources` | Slot name to resource instance. |

Set them at construction, or derive a reconfigured copy by calling an existing instance:

```py
asset = users(destinations=il.CSVDestination(base_path="./data"), dataset="shop")

read_only = asset(materializable=False)
strict = asset(materialization_strategy=il.MaterializationStrategy.STRICT)
bare = asset(normalizer=None)           # None explicitly clears the normalizer
```

Every keyword of the call means "leave unchanged" when omitted. `resources` merges over the
existing map; `destinations` replaces the list.

Unknown keyword arguments raise `TypeError` rather than being silently dropped.

## Running and materializing

| Call | Effect |
|------|--------|
| `asset.run(partition, dag, metadata)` | Execute, normalize, conform. Return the data. Write nothing. |
| `asset.materialize(partition, dag, metadata)` | Everything `run` does, then write to every destination. Returns the data, or `None` when the asset is not materializable. |
| `await asset.run_async(...)`, `await asset.materialize_async(...)` | The same, for async callers. |

`partition` is required for partitioned assets and ignored (with a warning) for unpartitioned
ones. `dag` is required when the asset has mandatory dependencies, because upstream data is read
through the DAG. `metadata` is a free-form dict (run id, backfill id) carried onto every event
the run emits.

An asset that produces no data skips its destination writes and emits a warning log event
rather than writing an empty table.

## What happens on materialize

1. The partition scope is validated against the asset's partitioning.
2. Resources are resolved and upstream assets are read from their destinations.
3. `data()` runs, wrapped in `asset_data_*` events and a tracing span.
4. The [normalizer](normalization.md) reshapes the result, when one is configured.
5. The result is [conformed](schema.md) to the schema according to the materialization strategy.
   Without a schema, one is inferred so destinations know the column types.
6. Each destination's `write()` is called with an `IOContext`, wrapped in `dest_write_*` events.

Steps 4 and 5 run off the event loop. Every step emits [events](events.md) and
[spans](telemetry.md).

## Execution context

An asset that declares a `context` parameter receives an `ExecutionContext`:

| Accessor | Gives you |
|----------|-----------|
| `context.asset_key` | The asset's key. |
| `context.metadata` | The run's metadata dict (`run_id`, `backfill_id`, anything the caller passed). |
| `context.logger` | A logger whose `debug`, `info`, `warning`, `error` emit `LOG` events attributed to the asset. |
| `context.partition` | The `Partition` this run covers: `.value`, `.id`, `.granularity`, `.bounds`. |
| `context.window` | The `PartitionWindow`, for assets declaring `allow_window=True`. A single partition is presented as a one-partition window. |
| `context.partition_date` | The partition value as a `date`, for daily assets only. |

Each partition accessor raises an `AttributeError` explaining why it is unavailable: the asset
is not partitioned, no partition was given, the run holds a window, or the granularity is not
daily. See [Partitioning](partitioning.md).

## Identity

| Property | Value |
|----------|-------|
| `asset.key` | The class-level key, unique within its source. |
| `asset.qualified_key` | `source_key.asset_key`, unique across sources; the bare key for standalone assets. |
| `asset.identity` | The `(source_key, asset_key)` pair. |
| `asset.table` | The physical table or folder name, derived by the owning source and coerced to a valid identifier. |
| `asset.source` | The owning `Source` instance, or `None`. |
| `Asset.classpath()` | The import path. Source-owned assets use the composite form `module:Source.Asset`. |
| `Asset.definition()` | An `AssetDefinition` with config schema, output schema, partitioning and relations. |

## Row counts per partition

`asset.partition_row_counts()` asks the first destination for row counts grouped by the
partition column. It raises when the asset is not partitioned or has no destination.
