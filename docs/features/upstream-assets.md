# Upstream Assets

Interloper supports asset dependencies -- an asset can consume data produced by another asset.
When an asset runs inside a DAG, each upstream dependency is read back from its destination and
passed in as a function argument.

## Automatic dependency resolution

Within a source, dependencies are resolved **automatically** by matching parameter names to
sibling asset keys:

```py
@il.source
def my_source():
    @il.asset
    def users():
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(users):  # 'users' matches the asset above
        return [{"count": len(users)}]

    return [users, user_count]
```

When the DAG executes, `users` runs first, its result is written to its destination, then read
back and passed as the `users` argument to `user_count`.

```py
source = my_source(destinations=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize()
```

## Explicit dependency mapping

When a parameter name doesn't match the upstream asset key, use `requires` in the `@asset`
decorator to declare the mapping (parameter name → upstream asset key, bare or qualified):

```py
@il.source
def my_source():
    @il.asset
    def raw_data():
        return [{"value": 42}]

    @il.asset(requires={"data": "raw_data"})
    def processed(data):  # 'data' doesn't match 'raw_data'
        return [{"result": data[0]["value"] * 2}]

    return [raw_data, processed]
```

Use `optional_requires` the same way for dependencies that may be absent — the parameter is set
to `None` when the upstream asset isn't part of the DAG.

## Cross-source dependencies

Assets from different sources can depend on each other as long as they are in the same DAG. Use
a qualified key in `requires`:

```py
@il.source
def source_a():
    @il.asset
    def data():
        return [{"id": 1}]
    return [data]

@il.source
def source_b():
    @il.asset(requires={"data": "source_a.data"})
    def report(data):
        return [{"count": len(data)}]
    return [report]

dag = il.DAG(source_a(...), source_b(...))
dag.materialize()
```

## Runtime dependency overrides

Each runtime asset carries a `dependencies` dict mapping a parameter name to an upstream asset's
**instance id**. You can wire dependencies imperatively — useful for connecting a standalone
asset to a source asset:

```py
@il.asset
def extra():
    return "x"

extra_asset = extra(destinations=dest)
source.report.dependencies["data"] = extra_asset.id

dag = il.DAG(source, extra_asset)
```

## Dependency resolution order

The DAG resolves each parameter in this order:

1. **Explicit mapping** via `asset.dependencies` / `requires`
2. **Same-source implicit** -- parameter name matches a sibling asset key
3. **Renamed-asset alias** -- parameter name matches the original key of a renamed asset
4. **Standalone implicit** -- parameter name matches a standalone asset key

Resources (connections, configs) injected by type annotation are resolved separately and are
never treated as upstream assets.
