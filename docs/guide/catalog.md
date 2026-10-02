# Catalog

A catalog is the set of component **definitions** available to a process: sources, connections,
configs, destinations, jobs and hooks, keyed by component key. It answers "what exists and how is
it configured" without instantiating or executing anything, and it is what `key` references in
[specs](specs.md) resolve against.

## Building a catalog

```py
import interloper as il

il.Catalog.discover()                      # everything installed packages declare
il.Catalog.from_settings()                 # what this deployment enables
il.Catalog.from_paths(["my_package.sources.Shop", "my_package.sources.Finance"])
il.Catalog.from_assets([Shop, Finance])
```

**Discovery** reads the `interloper.components` entry-point group of every installed package.
Each entry names a component class, or a module whose public attributes are scanned for
component classes. The scan is cached for the process.

**Settings** narrow the universe: when `catalog` in `interloper.yaml` (or
`INTERLOPER_CATALOG`) lists import paths, the catalog holds those components, everything they
depend on, and the framework's own components. An empty list means everything installed.

**Dependencies come along.** Enabling a source pulls in the target class of every relation it
declares and, through its assets, theirs, transitively. A catalog therefore never carries a
relation whose target class it cannot resolve. Framework components (`cron_job`, `trigger_hook`,
`webhook_hook`) are present in every catalog.

Paths that fail to import are skipped with a warning. A component whose kind has no registered
anchor raises `ConfigError`; see [Entry points](../extending/entry-points.md).

## Registering components

Declare your components in your package's `pyproject.toml`:

```toml
[project.entry-points."interloper.components"]
my_package = "my_package.sources"          # a module: every component class in it
```

or one entry per class. Nothing else is needed: installation is registration.

## Reading a catalog

```py
catalog.components                              # key -> ComponentDefinition
catalog.get("facebook_ads")                     # a SourceDefinition, or None
catalog.get("ads_stats", parent_key="facebook_ads")   # a source-owned asset's definition
catalog.vocabulary("hook", "trigger_hook")      # relation name -> Relation
catalog.to_paths()                              # sorted import paths, for another process
catalog.dump()                                  # JSON-serializable definitions
```

Assets are not top-level entries: they belong to their source and are reached through
`SourceDefinition.assets` or `get(key, parent_key=...)`.

## Definitions

Every component class describes itself through `definition()`:

| Field | Content |
|-------|---------|
| `kind`, `key`, `path` | Identity and import path. |
| `name`, `icon`, `description`, `tags` | Display metadata; the description is the docstring. |
| `maturity` | `deprecated`, `alpha`, `beta` or `stable` (the default); see [Maturity](#maturity). |
| `config_schema` | JSON Schema of the user-facing [configuration fields](fields.md). |
| `state_schema` | JSON Schema of the machine-owned state model, when the kind has one. |
| `relations` | Every declared relation by name, with its `kind`, `key`, `many`, `optional` and `on_delete`. |

Kind-specific definitions add to this: `SourceDefinition.assets`, `AssetDefinition.asset_schema`
and `partitioning`, `ResourceDefinition.provider`, `checkable`, `renewable`. This metadata is
what UIs render forms and pickers from.

## Maturity

A component declares how far it is from being relied on in production:

```py
@il.destination(maturity="beta", tags=["Cloud"])
class WarehouseDestination(il.Destination): ...
```

| Value | Meaning |
|-------|---------|
| `alpha` | Early work, not yet proven against a live service. Behaviour, configuration and output may still change. |
| `beta` | Works and is being proven in use. Changes are still possible. |
| `stable` | Relied on. The default for a component that declares nothing. |
| `deprecated` | Still works but is being phased out; avoid it for new setups. It will be removed in a future release. |

The value is `il.Maturity` or its string, set through the decorator or as a class attribute
(`maturity = "alpha"`); anything else is a `TypeError` where it is written. It is
informational: a component behaves the same at every level, and the app shows a Deprecated, Alpha
or Beta badge wherever the component is offered or listed.

An asset's definition carries the less mature of its own and its source's maturity, so an alpha
source makes every one of its assets alpha, while a stable source can still hold a beta asset.
`deprecated` counts as the least mature: a deprecated source deprecates all its assets.

## Resolving keys

`il.Source.resolve_key("facebook_ads", catalog)` turns a catalog key into the class, enforcing
the kind. Unknown keys raise `CatalogKeyError`. Spec reconstruction uses the same path for
every `key` reference.
