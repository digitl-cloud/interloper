# FAQ

### Why another data framework?

Because the existing ones are either too opinionated, too heavy, or in the way.

* ETL tools force rigid workflows.
* Orchestrators overcomplicate simple jobs.
* Hand-rolled pipelines break the moment a schema changes.

Interloper sits in between: write a function, materialize an asset; when you need it, declare
dependencies, enforce schemas, partition and backfill, and run the same code from a notebook to
a cluster.

### What is the difference between an asset definition and an asset instance?

`@il.asset` produces a **definition**, a class describing what the asset does. Calling it
produces an **instance**, which carries runtime configuration: destinations, resources,
dataset, dependency wiring.

```py
@il.asset
def my_asset():          # the definition
    return "hello"

instance = my_asset()    # an Asset instance
```

The same applies to sources: `OpenMeteo` is the definition, `OpenMeteo(latitude=...)` an
instance.

### Do I need `asyncio` to run assets?

No. `run()`, `materialize()` and `dag.materialize()` are synchronous calls that work in scripts,
the REPL and notebooks. The engine is async-native underneath and the sync entry points drive it
on a persistent background loop through `il.run`. Async code uses the `*_async` counterparts and
awaits them. Sync asset functions are offloaded to worker threads automatically, so an asset never
has to be `async` just to satisfy the engine.

### What is the difference between `run()` and `materialize()`?

`run()` executes the asset, normalizes and conforms the result to its schema, and returns it
without writing anywhere. `materialize()` does the same and then writes the result to every
configured destination.

### Should I write sources as classes or functions?

Classes. `@il.source class MySource(il.Source)` with `@il.asset` methods is the form every
shipped connector uses: configuration fields live on the class, assets read them through
`self`, and type checkers understand the result. The functional form
(`@il.source def my_source(): return [a, b]`) still works and is documented in
[Sources](guide/sources.md#functional-form).

### How does dependency resolution work?

Within a source, a parameter named after a sibling asset is a dependency. For a different name
or a cross-source dependency, declare `requires={"param": "source_key.asset_key"}` on the
asset. Optional dependencies use `optional_requires` and receive `None` when the upstream is
missing. See [Dependencies](guide/dependencies.md).

### Can I use Interloper without a DAG?

Yes. Run or materialize individual assets directly:

```py
result = my_asset().run()
my_asset(destinations=il.CSVDestination(base_path="./data")).materialize()
```

A DAG is needed once assets depend on each other, because the DAG is what resolves upstream data.

### Which destinations exist?

The core ships `CSVDestination`, `FileDestination` (pickle) and `MemoryDestination`.
Companion packages add BigQuery and Google Cloud Storage. Your own takes a `read()` and a
`write()` method; database-style stores subclass `DatabaseDestination` and implement a handful
of row operations. See [Destinations](guide/destinations.md).

### What is the difference between a Config and a Connection?

Both are [resources](guide/resources.md) injected by type annotation and loaded from the
environment. A `Config` holds plain settings. A `Connection` holds credentials and clients, and
adds a health check hook, credential renewal, and the OAuth sign-in machinery. See
[Connections](guide/connections.md).

### How do I run a backfill?

Iterate a `TimePartitionWindow` and materialize the DAG once per partition, newest first. An
asset that declares `allow_window=True` can take the whole window as one run instead. See
[Backfilling](guide/backfilling.md).

### Where do jobs, hooks and the scheduler fit?

`Job`, `CronJob` and the hook classes are core components: they declare *what* to run and
*when*, or *what to react to*. Acting on those declarations is the job of the platform packages
(`interloper-scheduler`, `interloper-api`, `interloper-app`), which are outside this site. The
core pages [Jobs](guide/jobs.md) and [Hooks](guide/hooks.md) document the declarations.
