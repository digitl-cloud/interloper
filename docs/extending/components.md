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
| `relations` | class | Relation name to `Relation`, the links this class declares (below). |
| `parent` | instance | The component that owns this one, `None` when it stands alone. A source owns its assets. |
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

    inputs: list[il.Asset] = il.Relation("asset", many=True, optional=True)
```

```toml
[project.entry-points."interloper.kinds"]
report = "my_package.report:Report"
```

A catalog containing a component of an unregistered kind raises `ConfigError`.

### Relations

A relation is one declared link from a component to the components that may fill it. One class,
`il.Relation`, declares every link on every kind: a connection an asset injects, the
destinations a source writes to, a job's targets, an upstream asset.

| Field | Meaning |
|-------|---------|
| `kind` | The component kind, or kinds, the relation may point at. |
| `key` | The keys it narrows to: an exact key, `source.asset`, `*.asset`, a list, or `""` for any key of those kinds. |
| `many` | Whether it binds several components at once. |
| `optional` | Whether it may stay unbound. Says nothing about data. |
| `default` | A zero-argument factory producing the value an unbound relation resolves to. |
| `on_delete` | What deleting the target does to the referrer: `block` (default, for consumption relations) or `detach` (for orchestration pointers such as a job's targets or a hook's watches). |
| `name` | The relation's name, stamped from the attribute it is declared under. |
| `target` | The class the relation was declared from, when declared from one. It is what a fallback is built from. |

`il.Relation(PostgresConnection)` is shorthand for
`il.Relation(kind="connection", key="postgres_connection")` with the class kept as `target`.
`accepts(kind, identity, owner=...)` is the one place a candidate is judged against a
declaration, and `ComponentIdentity.satisfies` the one place a declared key is compared to a
concrete component. An asset's key is source-local, so a bare key is scoped to the owner's
source; every other kind is keyed globally by its catalog key.

**Three declaration forms**, in increasing precedence, all merged at class creation:

```py
class Widget(il.Component):
    connection: PostgresConnection                                   # an annotation naming a class
    config: WidgetConfig = il.Relation(WidgetConfig, optional=True)  # a typed Relation attribute
    relations = {"cache": il.Relation(Cache)}                        # what the decorators emit
```

The map merges over every base's, so a subclass entry replaces the inherited one of the same
name and nothing an ancestor declared is lost. Each entry is copied, stamped with its name and
installed under it: a `Relation` is its own descriptor, so `Widget.connection` is the
declaration and `widget.connection` what is bound to it. An annotated relation is dropped from
the class's annotations before pydantic collects its fields, so a relation is never also a
field. The two collectors read the annotations against different namespaces (the declaring
module here, the full defining scope in pydantic), so an annotation naming a component class
that ends up a plain field, and one that resolves nowhere at all, are both `TypeError` at class
definition rather than a component silently missing a relation.

**Operations**, all of them on `Component`:

| Method | What it does |
|--------|--------------|
| `bind(name, *targets)` | Writes bindings, together with `unbind` and attribute assignment (which routes through the same checks, see below). Checks `accepts` for each target; `many` accumulates and collapses duplicates, single-valued replaces what it holds and refuses more than one target at a time. |
| `unbind(name, *targets)` | Detaches. Refused when it would empty a non-optional relation. |
| `bound(name)` | What is explicitly bound: a list for `many`, the single component or `None`. |
| `resolve(name)` | What a reader gets: `bound(name)`, else the relation's fallback for a single-valued relation, an empty list for a `many` one. Fallbacks are never bound. |
| `trickle(child)` | Fills a child's unbound relations from this component's own bindings, by name, keeping only what the child's relation accepts. Never overrides an explicit binding. |
| `validate_relations(nodes=None)` | Unbound non-optional without a fallback, several targets on a single-valued relation, a target the relation does not accept, and (with `nodes`) a non-optional asset target absent from the run. |

**Extension point**: `on_rebind(name)` is called once after every write to a binding, whichever
way it was written (constructor kwarg, `bind`, `unbind`, attribute assignment, a parent's
`trickle`), with the new binding already in place. The base does nothing. A kind that cascades its
wiring overrides it; this is the whole of how a source's connection reaches its assets:

```py
class Source(Component):
    def on_rebind(self, name: str) -> None:
        for asset in self.assets:
            self.trickle(asset)
        for destination in self.destinations:
            self.trickle(destination)
```

Bind on other components from inside the hook, never on `self`: that would re-enter it.

A relation name is also a constructor keyword and an assignable attribute; assignment goes
through `Relation.__set__`, which shares `bind`'s single write path: the replacement is checked
before the existing binding is touched (so a rejected assignment leaves the previous one exactly
as it was), duplicates collapse, clearing a non-optional relation raises `ConfigError`, and
whatever the owner cascades into its children is cascaded again.

`definition().relations` exports each relation for the catalog and the UI: `kind`, `key`,
`many`, `optional`, `on_delete`.

### Discriminator

One configuration field may carry `discriminator=True`. `discriminator` and `instance_name()`
expose it; sources use it for per-instance table names. Two marked fields raise `TypeError`.

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
