# Component model

Everything a developer defines in Interloper is a **component**: assets, sources, destinations,
connections, configs, jobs, hooks. Components share one base with two layers. `Serializable` is
anything that is "a class plus its configuration"; `Component` adds kind, identity and relations
and makes the object a catalog citizen. This page is for people writing new component classes
or new kinds.

## Serializable

A pydantic model with:

- **`key`**: class-level, snake_cased from the class name unless declared.
- **Strict construction**: unknown keyword arguments raise `TypeError`.
- **Specs**: `to_spec()`, `from_spec()`, `from_spec_file()`, `classpath()`, `resolve_path()`.
  See [Specs and serialization](../guide/specs.md).
- **`config_schema()`**: the JSON Schema of user-facing fields, with framework fields and the
  class's `internal_fields` stripped.
- **`build_class(decorated, classvars=..., fields=...)`**: the factory behind every decorator.
  It builds a subclass from a decorated class, stamping class-level attributes and overriding
  field defaults through the pydantic metaclass so `model_fields` stays correct. Decorators
  build classes; they never mutate finalized ones.

Runners, normalizers and schemas are `Serializable` without being components.

## Component

On top of `Serializable`:

| Attribute | Level | Meaning |
|-----------|-------|---------|
| `kind` | class | The component category (`source`, `asset`, `connection`, …). Set automatically for direct children of `Component`; inherited below that. |
| `id` | instance | A UUID by default; the identity persisted relations point at. |
| `name`, `icon` | class | Display metadata; `name` defaults to a label built from the class name. |
| `resource_types` | class | Slot name to resource class. Filled from typed annotations and `ResourceRef` descriptors. |
| `resources` | instance | Slot name to resource instance. |
| `relation_types` | class | The relation vocabulary (below). |
| `sensitive` | class | Whether stored configuration must be encrypted. `True` for resources. |
| `state_model` | class | A pydantic model of machine-owned state (job timestamps, renewal times). Its JSON Schema becomes `state_schema` in the definition. |
| `internal_fields` | class | Fields hidden from the config schema. |

`definition()` returns a `ComponentDefinition` (kind, key, path, name, icon, description, tags,
`config_schema`, `state_schema`, `relations`). Subclasses return richer definitions of their own
(`SourceDefinition`, `AssetDefinition`, `ResourceDefinition`, `DestinationDefinition`).

### Kinds and anchors

Each kind has an **anchor**: the base-most class declaring it (`Connection` for every
connection). `Component.anchor()` resolves it, and the `KINDS` registry maps kind names to
anchors. Anchors are declared through the `interloper.kinds` entry-point group; the core
declares `source`, `asset`, `destination`, `resource`, `connection`, `config`, `job` and
`hook`. A new kind is a new anchor class and one entry-point line:

```py
class Report(il.Component):
    """A rendered document built from assets."""

    relation_types = {
        "input": il.RelationDefinition(kinds=["asset"], field="inputs"),
    }
    inputs: list[il.Asset] = []
```

```toml
[project.entry-points."interloper.kinds"]
report = "my_package.report:Report"
```

A catalog containing a component of an unregistered kind raises `ConfigError`.

### Relations

A relation type describes how instances of a kind point at other components. It is declared in
`relation_types` and is what the platform validates when it stores an edge, and what UIs render
pickers from:

| `RelationDefinition` field | Meaning |
|---------------------------|---------|
| `kinds` | Component kinds the relation may point at. |
| `field` | The instance field carrying the relation: a `list` for unslotted types, a `dict[slot, ...]` for slotted ones. Must exist on the class. |
| `slotted` | Whether each relation fills a named slot (resource slots, dependency parameters). |
| `inline` | Whether the field holds component instances (default) or bare ids resolved at run time (asset dependencies). |
| `keys` | Allowed destination keys, as picker metadata. |
| `slots` | The slots a concrete class declares (`RelationSlot(key, required)`). |
| `on_delete` | What deleting the relation's target does to the referrer: `block` (default, for consumption relations) or `detach` (for orchestration pointers such as a job's targets or a hook's watches). |
| `on_unbind` | What explicitly unbinding a bound required slot does: `detach` (default) or `block` (asset dependencies). |

Declarations are **extend-only**: a subclass's `relation_types` merges over its parent's, so
`TriggerHook` adds `target` without losing `watch` and `resource`. `relation_definitions()`
returns the vocabulary enriched with the class's own slots: resource slots from
`resource_types`, dependency slots from `requires`, allowed destination keys from
`destination_types`.

A relation whose `field` does not exist on the class raises `ValueError` when the definition is
built.

### Discriminator

One configuration field may carry `discriminator=True`. `discriminator_field()`,
`discriminator` and `instance_name()` expose it; sources use it for per-instance table names.
Two marked fields raise `TypeError`.

## Writing a decorator

A decorator for a new kind wraps `build_class`:

```py
def report(cls=None, /, *, key=None, name=None, tags=None):
    classvars = {k: v for k, v in {"key": key, "name": name, "tags": tags}.items() if v is not None}
    if cls is not None:
        return Report.build_class(cls, classvars=classvars)
    return lambda cls: Report.build_class(cls, classvars=classvars)
```

`classvars` are stamped as class attributes; `fields` override defaults of existing pydantic
fields and must name fields the receiving class has.

## Definitions in the catalog

Every class reachable through the `interloper.components` entry point becomes a catalog entry
through its `definition()`. Nothing is inferred or registered at import time: installation is
registration, and the catalog contains exactly what was declared. See
[Catalog](../guide/catalog.md) and [Entry points](entry-points.md).
