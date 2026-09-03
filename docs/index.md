# Getting started

## Installation

```sh
pip install interloper-core
```

Or with [uv](https://docs.astral.sh/uv/):

```sh
uv add interloper-core
```

Interloper requires Python 3.10 or newer. The core package depends only on pydantic,
pydantic-settings, httpx, PyYAML and the OpenTelemetry API. Optional extras and companion
packages are listed in [Ecosystem](reference/ecosystem.md).

## Your first asset

An asset is a function that produces data. Decorate it with `@il.asset`:

```py
import interloper as il

@il.asset
def greetings():
    return [
        {"name": "Alice", "message": "Hello"},
        {"name": "Bob", "message": "Hi"},
    ]
```

The decorator turns the function into an asset **definition**, a class. Calling the definition
creates an asset **instance** you can run:

```py
result = greetings().run()
# [{'name': 'Alice', 'message': 'Hello'}, {'name': 'Bob', 'message': 'Hi'}]
```

`run()` executes the function and returns its data without writing anywhere. It is a plain
synchronous call that works in scripts, the REPL and notebooks. Async code awaits `run_async()`
instead.

## Materialize it

Materializing an asset runs it **and** writes the result to its destinations. Give the instance a
destination:

```py
asset = greetings(destinations=il.CSVDestination(base_path="./data"))
asset.materialize()
# ./data/greetings/data.csv
```

Three destinations ship with the core: `CSVDestination`, `FileDestination` (pickle) and
`MemoryDestination` (in-process, for tests). Others come from companion packages, and
[writing your own](guide/destinations.md#custom-destinations) takes two methods.

## Group assets in a source

A source groups assets that belong together. Declare it as a class with `@il.asset` methods.
The source instance arrives as `self`, so assets can share configuration and helpers:

```py
@il.source
class Shop(il.Source):
    currency: str = il.InputField(default="EUR", description="Reporting currency")

    @il.asset
    def users(self) -> list[dict]:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def orders(self) -> list[dict]:
        return [{"id": 1, "user_id": 1, "total": 99.90, "currency": self.currency}]

    @il.asset
    def order_count(self, orders: list[dict]) -> list[dict]:
        return [{"count": len(orders)}]
```

Two things happened here without any wiring:

- `currency` became a configurable field of the source, with a default, loadable from the
  environment or set at construction: `Shop(currency="USD")`.
- `order_count` declares a parameter named `orders`, which matches a sibling asset. That is a
  **dependency**: when the DAG runs, `orders` is materialized first, read back from its
  destination, and passed in.

Assets are reachable as attributes on the instance:

```py
shop = Shop(destinations=il.CSVDestination(base_path="./data"))
shop.users.run()
shop.orders.materialize()
```

## Build a DAG and materialize everything

A DAG orders the assets by dependency and runs them:

```py
dag = il.DAG(shop)
result = dag.materialize()
print(result)
# RunResult(status=completed, partition=None, completed=3, failed=0, canceled=0, time=0.02s)
```

`dag.materialize()` uses the default `AsyncRunner`, which runs independent assets concurrently.
Pick another runner, or tune concurrency and failure handling, as described in
[Execution](guide/execution.md).

## Run from the command line

Anything importable can be run with the CLI. Save the source as `shop.py`, make its directory
importable, and print the plan:

```sh
PYTHONPATH=. interloper run shop.Shop --dry-run
```

An import path instantiates the class with its defaults, so a source whose assets depend on each
other needs destinations to read them back from. Those come from a spec file:

```yaml
# shop.yaml
path: shop.Shop
init:
  destinations:
    - path: interloper.destination.csv.CSVDestination
      init: { base_path: ./data }
```

```sh
PYTHONPATH=. interloper run -f shop.yaml
```

See [CLI](guide/cli.md) and [Specs and serialization](guide/specs.md).

## Next steps

- Follow the [Tutorial](tutorial.md) to build a real source against an HTTP API with
  configuration, credentials, schema and partitioning.
- Read [Assets](guide/assets.md) and [Sources](guide/sources.md) for every option the two
  decorators accept.
