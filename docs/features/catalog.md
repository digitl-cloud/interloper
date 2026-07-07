# Catalog

A **catalog** is a registry of component definitions — sources, assets, connections,
destinations, and configs — keyed by their component key. It's how Interloper introspects what's
available without instantiating or executing anything: the API, the web UI, and the declarative
[declarative workloads](cli.md) all read catalog metadata.

## Building a catalog

From a list of source/asset classes:

```py
import interloper as il
from interloper_assets import DemoSource

catalog = il.Catalog.from_assets([DemoSource])
```

From a list of import paths (resources and destinations referenced by the components are
discovered automatically):

```py
catalog = il.Catalog.from_paths([
    "interloper_assets.facebook_ads.source.FacebookAds",
    "interloper_assets.google_ads.source.GoogleAds",
])
```

From application settings (the `catalog` list in [`interloper.yaml`](cli.md#interloperyaml),
falling back to entry-point discovery):

```py
catalog = il.Catalog.from_settings()
```

Or discover everything registered under the `interloper.components` entry-point group:

```py
catalog = il.Catalog.discover()
```

## Working with a catalog

```py
catalog.components            # dict[str, ComponentDefinition]
catalog.get("facebook_ads")   # a ComponentDefinition, or None
catalog.to_paths()            # sorted list of component import paths
catalog.dump()                # JSON-serializable dict of all definitions
```

Each entry is a `ComponentDefinition` carrying the component's metadata — its key, name, tags,
config schema, declared resources and destinations, and (for assets) schema and partitioning.
This is the same metadata the API serves and spec reconstruction resolves component `key`
references against.
