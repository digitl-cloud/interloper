# FAQ

### Why another data framework?

Because the existing ones are either too opinionated, too bloated, or just don't get out of your
way.

* ETL tools force rigid workflows.
* Orchestration frameworks overcomplicate simple jobs.
* DIY pipelines break the moment your schema changes.

Interloper sits in between:

* **Simple when you want it**: write a function, materialize an asset.
* **Powerful when you need it**: define dependencies, automatically reconcile schemas, partition
  and backfill, run anywhere from a single thread to Kubernetes.

In terms of concepts and design, Interloper draws inspiration from modern orchestrators while
staying a lightweight, embeddable library.


### What is the difference between an asset definition and an `Asset`?

The `@asset` decorator produces an immutable **definition** (a class) that describes *what* an
asset does. Calling it creates a runtime `Asset` instance, which carries runtime configuration
like destinations, resources, and dependency wiring.

```py
@il.asset
def my_asset():       # this is the definition
    return "hello"

instance = my_asset()  # this is an Asset instance
```


### Do I need `asyncio` to run assets?

No. `run()`, `materialize()`, and `dag.materialize()` are plain synchronous calls — they work
as-is in scripts, the REPL, and notebook cells. Under the hood the execution engine is
async-native: the sync entrypoints drive it on a persistent background event loop (see
`il.run`). Async code uses the `*_async` counterparts (`run_async()`, `materialize_async()`)
and `await`s them. Synchronous asset functions are offloaded to threads automatically, so you
never have to make your asset `async` just to satisfy the engine.


### What is the difference between `run()` and `materialize()`?

`run()` executes the asset function and returns the result (normalized and conformed to its
schema if configured) **without** writing anywhere. `materialize()` does the same and then writes
the result to all configured destinations.


### How does dependency resolution work?

Within a source, if an asset's parameter name matches a sibling asset's key, the dependency is
resolved automatically. For cross-source dependencies or name mismatches, use `requires` (or
`optional_requires`) on the `@asset` decorator. See [Upstream Assets](features/upstream-assets.md).


### Can I use Interloper without a DAG?

Yes. You can run or materialize individual assets directly:

```py
result = my_asset().run()
my_asset(destination=il.FileDestination("./data")).materialize()
```

A DAG is only needed when you have multiple assets with dependencies.


### What destinations are available?

Built-in (core library): `MemoryDestination` (default), `FileDestination`, `CSVDestination`. Via
`interloper-google-cloud`: `BigQueryDestination`. You can build your own by subclassing
`il.Destination` — or `il.DatabaseDestination` for SQL-style stores. See
[Destinations](features/destinations.md).

> Earlier versions exposed an "IO" concept (`FileIO`, `MemoryIO`, an `interloper-sql` package).
> That has been replaced by Destinations; there is no longer a bundled Postgres/MySQL/SQLite
> backend.


### What's the difference between a Config and a Connection?

Both are [resources](features/resources.md) injected into asset functions and both load from the
environment. Use a [Config](features/config.md) for plain settings and a
[Connection](features/connections.md) for service credentials — connections add secret handling
and an OAuth connect flow.


### How do I run a backfill?

Pass a `TimePartitionWindow` to a runner (or `dag.materialize()`). There's no separate backfiller
object. See [Backfilling](features/backfilling.md).
