# Assets & Sources

## Standalone assets

The simplest way to define an asset is with the `@asset` decorator on a function:

```py
import interloper as il

@il.asset
def my_asset():
    return [{"key": "value"}]
```

The decorator returns an asset **definition** (a class) -- an immutable blueprint. To create a runtime `Asset` instance, call it:

```py
asset_instance = my_asset()
result = asset_instance.run()
```

Asset functions can be synchronous or asynchronous. Synchronous functions are offloaded to a
worker thread automatically; `async def` functions are awaited natively on the event loop.

### Decorator options

The `@asset` decorator accepts several options:

```py
@il.asset(
    name="custom_name",                 # Human-readable name
    key="custom_key",                    # Override the asset key (defaults to snake_case fn name)
    schema=MySchema,                     # Schema for validation/reconciliation
    partitioning=il.TimePartitionConfig(column="date"),
    normalizer=il.Normalizer(),          # Data normalization
    materialization_strategy=il.MaterializationStrategy.RECONCILE,
    destinations=[il.FileDestination],   # Destination types this asset writes to
    resources={"conn": MyConnection},    # Named resource dependencies (injected)
    requires={"data": "other_source.raw"},        # Explicit upstream mapping
    optional_requires={"extra": "other.maybe"},   # Optional upstream (None if absent)
    tags=["daily", "api"],               # Arbitrary tags
    icon="icon:database",                # Icon identifier (for the UI)
)
def my_asset():
    ...
```

### Running an asset

`run()` executes the asset function and returns the result (normalized and conformed to its
schema if configured) **without** writing to any destination:

```py
result = my_asset().run()
```

### Materializing an asset

`materialize()` executes the asset **and** writes the result to all configured destinations:

```py
asset_instance = my_asset(destination=il.FileDestination("./data"))
asset_instance.materialize()
```

Both `run()` and `materialize()` are plain synchronous calls — they work in scripts, the
REPL, and notebooks. Async code awaits their coroutine counterparts, `run_async()` and
`materialize_async()`.

## Sources

A source groups related assets together. It is a **function** decorated with `@il.source` that
returns the list of asset definitions it contains:

```py
@il.source
def my_source():
    @il.asset
    def asset_a():
        return [{"key": "A"}]

    @il.asset
    def asset_b():
        return [{"key": "B"}]

    return [asset_a, asset_b]
```

!!! note "Class-based sources"

    A class-based form is also supported (`@il.source class MySource(il.Source): ...` with
    `@il.asset`-decorated methods), and is used by some pre-built sources in
    `interloper-assets`. The functional form above is the recommended default for new code.

### Source decorator options

```py
@il.source(
    name="custom_name",
    key="custom_key",
    destinations=[il.FileDestination],            # Shared destination types
    resources={"conn": MyConnection},             # Shared resource dependencies
    dataset="my_dataset",                         # Default dataset for assets
    default_destination_key="primary",            # Which destination assets read from
    normalizer=il.Normalizer(),                   # Applied to all assets without their own
    materialization_strategy=il.MaterializationStrategy.AUTO,
    materializable=True,
    tags=["production"],
)
def my_source():
    ...
```

Configuration set on the source **trickles down** to its assets: an asset that doesn't define
its own `destination`, `dataset`, `normalizer` or strategy inherits the source's.

### Instantiating a source

Call the source definition to create a runtime `Source`:

```py
source = my_source()                                       # Default resources from env
source = my_source(destination=il.FileDestination("./data"))
source = my_source(resources={"conn": MyConnection(...)})  # With explicit resources
```

### Accessing assets

Assets are available as attributes on both the definition and the instance:

```py
# On the definition (returns the asset definition class)
my_source.asset_a

# On the instance (returns the runtime Asset)
source = my_source(destination=il.FileDestination("./data"))
source.asset_a.run()
source.asset_b.materialize()
```

### Reconfiguring a source

Calling an existing source instance returns a reconfigured copy, leaving the original
untouched:

```py
source = my_source(destination=il.FileDestination("./data"))

# A copy whose assets won't be written during materialization
read_only = source(materializable=False)
```

## DAG

A `DAG` (Directed Acyclic Graph) orchestrates the execution of multiple assets, respecting their dependencies:

```py
source = my_source(destination=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize()
```

A DAG can be built from any combination of assets and sources (instances or definitions):

```py
dag = il.DAG(source_a, source_b, standalone_asset)
```

At construction the DAG validates the graph: it rejects duplicate assets, missing dependencies
(`DependencyNotFoundError`), cycles (`CircularDependencyError`), and a non-partitioned asset
depending on a partitioned one.

`dag.materialize(partition_or_window=...)` runs the DAG with the default `AsyncRunner`. For
other execution strategies, see [Runners](runners.md).
