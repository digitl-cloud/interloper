# Getting started

## Installation

```sh
pip install interloper
```

Or with [uv](https://docs.astral.sh/uv/):

```sh
uv add interloper
```

### Optional packages

```sh
pip install interloper-pandas         # pandas DataFrame normalizer/adapter
pip install interloper-google-cloud   # BigQuery destination
pip install interloper-assets         # Pre-built source definitions
pip install interloper-docker         # Docker runner
pip install interloper-k8s            # Kubernetes runner
```

## Quick example

### Define an asset

An asset is a function that produces data. Decorate it with `@asset`:

```py
import interloper as il

@il.asset
def greetings():
    return [
        {"name": "Alice", "message": "Hello"},
        {"name": "Bob", "message": "Hi"},
    ]
```

### Run it

`run()` executes the asset function and returns the result — a plain synchronous call that
works in scripts, the REPL, and notebooks (the engine is async-native under the hood; async
code awaits `run_async()` instead):

```py
result = greetings().run()
print(result)
# [{'name': 'Alice', 'message': 'Hello'}, {'name': 'Bob', 'message': 'Hi'}]
```

### Materialize it

Add a destination and materialize -- this runs the asset **and** writes the result:

```py
greetings_asset = greetings(destination=il.FileDestination("./data"))
greetings_asset.materialize()
# Data is written to ./data/greetings/data.pkl
```

### Group assets in a source

A source groups related assets. It is a **function** decorated with `@il.source` that
returns the list of asset definitions it contains:

```py
@il.source
def my_source():
    @il.asset
    def users():
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def orders():
        return [{"id": 1, "user_id": 1, "total": 99.90}]

    return [users, orders]
```

### Build a DAG and materialize everything

```py
source = my_source(destination=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize()
```

## Next steps

- Follow the [Tutorial](tutorial.md) for a hands-on walkthrough
- Explore [Features](features/assets-sources.md) for in-depth documentation
