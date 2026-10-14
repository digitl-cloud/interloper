# Core phase 4: Component, Serializable and decorators

Date: 2026-10-14. Status: approved in conversation, implementation pending.

Phase 4 of the interloper-core consolidation programme (five stacked phases: scope, contexts and
observation; Asset and Source; the runner family; Component, Serializable and decorators; the
surface sweep). Phases 1 to 3 settled the execution spine, how assets and sources relate, and how a
DAG is run. This phase settles how a component class is built, what a component belongs to, and
what flows between components that do not belong to each other.

Scope: `interloper.component` (`base`, `decorator`), `interloper.serializable`, the kind
decorators (`asset`, `view`, `reference`, `source`, `connection`, `config`, `destination`,
`schema`, and `interloper_sql`'s `ibis_view`), `Source` and `Job` where they cascade, and the docs
and skills that describe them. Built on phase 3 (PR #460).

The guiding constraint is the author's view. A developer writing a component must be able to hold
the whole model in one sentence: *a component declares fields and relations; an asset belongs to
its source and reads from it whatever it leaves unset; nothing else belongs to anything.* Every
change below either serves that sentence or removes something that contradicts it. No new concept
reaches an author.

---

## 1. Problem

**Decorators repeat themselves.** Eight public decorators each write the same bare or parenthesised
form (a nested `wrapper` closure and three overloads), delegate to `decorate`, which routes the
settings, and then to a per-kind `_build_*` function. The class-target kinds (`connection`,
`config`, `destination`) have identical build functions, `declare(X.build_class(...), relations)`.
`view`, `reference` and `ibis_view` each wrap `build_asset_class` in a cast. `schema` bypasses the
engine. `connection` carries a special case for `oauth`.

**A class is built four ways.** `Serializable.build_class` has two branches: a decorated subclass
gets `create_model` when there are field defaults and is otherwise stamped in place; a plain class
is rebuilt with `type()`. `build_asset_class` and `_build_source_from_fn` each assemble a namespace
(annotations from the fields, module, qualname, docstring) by hand. `declare()` collects relations a
second time, because the class was created before the decorator could add any. "Decorators build
classes, they never mutate finalized ones" is stated, and not quite true.

**Ownership is discovered.** `_owned_by_field` reads every field of a component on every call
(inherited read-throughs included) and keeps whatever component it finds whose `parent` is self.
Only `Source.model_post_init` ever sets a `parent`, so the one relationship in interloper that is
part-of is a Source detail `Component` reverse-engineers. A source given another source's asset
silently takes it.

**Bindings are pushed into components nothing owns.** `Source.on_rebind` trickles the source's
bindings into its destinations; `Job.on_rebind` trickles the job's into its targets and its
destinations. The push writes into shared components (a destination or target used twice keeps the
first push), turns a lent binding into one indistinguishable from a set one (a target's spec then
carries the job's destinations as its own), and can only run at bind time, so `on_rebind` and
`trickle` exist on every component for two cascades. A destination taking its credentials from
whichever source writes through it contradicts the author's model outright.

**Reconstruction knows about components in the base layer.** `Spec.reconstruct` nests a `load`
closure and branches on `issubclass(cls, Component)`, a special case the `Serializable` layer
should not hold.

---

## 2. Decisions

- One `Decorator` class replaces `decorate`, `declare`, the eight closures and the per-kind build
  functions. Every decorator, `schema` included, is an instance of it.
- One build path: `build_class` always creates a new class with one `type(...)` call, everything in
  its namespace. A decorated class that already extends its anchor becomes the base of the new
  class (one same-named level in its MRO), keeping its key and kind through `rebuild=True`.
- The routing of decorator settings lives on the anchor class (`Serializable` sorts ClassVars and
  field defaults, `Component` adds relations), so `Decorator` holds no logic of its own.
- Ownership stays the business of the one kind that owns: `Source` overrides `_owned_by_field`;
  `Component` owns nothing. No `owns` declaration. A source refuses an asset another source owns.
- `trickle`, `on_rebind` and both cascades into components that are not owned are removed.
- A job's default destinations survive as the one cascade into components it does not own, applied
  where a job runs: `Job.operations()` runs each target that resolves none as a copy carrying them.
- The source-to-destination cascade is removed outright: a destination configures its own
  connection.
- `Serializable.from_init` is the polymorphic step of reconstruction; `Component` overrides it.

---

## 3. One decorator, one build path

### 3.1 `ClassBody`

`serializable/body.py` holds the one value every decorator turns its target into:

```python
@dataclass(frozen=True)
class ClassBody:
    """What a decorated function or class contributes to the class a decorator builds."""

    name: str
    module: str
    qualname: str
    doc: str | None
    members: dict[str, Any]
    annotations: dict[str, Any]
    base: type[Serializable] | None = None

    @classmethod
    def from_class(cls, target: type, anchor: type[Serializable]) -> ClassBody: ...
```

- `from_class` reads a class. When the class already extends the anchor, the body is empty and
  `base` is the class itself: the built class subclasses it. Otherwise the body carries the class's
  members (dunders excluded) and own annotations, and the anchor is the base.
- A function target has no generic reading: the anchor says what a function means, through
  `Serializable.body_from_function(fn) -> ClassBody`. The base refuses (`TypeError: <anchor>
  decorates a class, not a function`). `Asset` overrides it with today's `data()` generation (a
  method asset passes `self.source` as `self`; sync and async keep their own wrapper, two closures
  instead of four, since the generated `data` must be a real function for `inspect` to see it as
  async), so `View`, `Reference` and `IbisView` inherit it. `Source` overrides it with today's
  function form (call the function for its asset classes, turn its annotated parameters into
  fields).
- `ClassBody.of(target, anchor)` dispatches: a class to `from_class`, a function to
  `anchor.body_from_function`.

### 3.2 `build_class`

```python
class Serializable(BaseModel):
    @classmethod
    def build_class(cls, body: ClassBody, **settings: Any) -> type[Self]:
        """Build a new class from a body and a decorator's settings."""
```

- `settings` is sorted by `_sort_settings(settings) -> dict[str, Any]` into the namespace entries it
  becomes: a public ClassVar (any name some class in the MRO annotates `ClassVar`, neither private
  nor reserved) stays a value and is annotated `ClassVar`; a field default becomes a deep copy of the
  field's `FieldInfo` with the new default (title, description and `json_schema_extra` kept) and is
  annotated with the field's type. A name both a ClassVar and a field, or neither, is a `TypeError`.
  When a subclass of `cls` declares the refused name (`oauth` on `OAuthConnection`), the error names
  it: `Connection does not accept 'oauth'; it is declared by OAuthConnection, subclass it`. This
  replaces the `connection` decorator's special case.
- `Component._sort_settings` extends it with the `relations=` channel, read exactly as `_relations`
  reads it today (a `Relation`, a component class, or a list of component classes narrowing a
  relation the anchor declares) and written into the namespace's `relations` dict, which
  `Component._collect` merges at class creation. Relations are collected once; `declare()` goes.
- The class is `type(body.name, (body.base or cls,), namespace, rebuild=body.base is not None)`,
  with `__module__`, `__qualname__` and `__doc__` from the body. `__init_subclass__` validates the
  ClassVars where they are written (`_validate_classvars`, already called there), so the extra call
  after building goes.
- `_classvars`, `_validate_classvars` and `_RESERVED_CLASSVARS` move from `Component` to
  `Serializable`, so `schema` routes and validates like every other kind.
- The receiver is the class the routing introspects: `(body.base or anchor).build_class(...)`, so a
  decorated `OAuthConnection` subclass accepts `oauth`.

`create_model`, the in-place ClassVar stamping and `_override_defaults` as a separate static helper
(it becomes part of `_sort_settings`) go.

### 3.3 `Decorator`

`component/decorator.py`:

```python
class Decorator(Generic[SerializableT]):
    """A decorator building a class of one kind: bare, or with settings."""

    def __init__(self, anchor: type[SerializableT], **settings: Any) -> None: ...

    @overload
    def __call__(self, target: type[SubclassT], /) -> type[SubclassT]: ...
    @overload
    def __call__(self, target: type | Callable[..., Any], /) -> type[SerializableT]: ...
    @overload
    def __call__(self, /, *, relations: dict[str, Any] | None = ..., **overrides: Any) -> Decorator[SerializableT]: ...
```

Called with a target, it builds `ClassBody.of(target, anchor)` through `build_class` with its
settings. Called with settings, it returns a new `Decorator` carrying them, so nothing nests. The
overloads keep today's typing: a decorated subclass keeps its own type (a probe confirmed `ty`
reveals `<class 'Amazon'>` for `@connection(name=...) class Amazon(OAuthConnection)`), a plain class
or a function is `type[<anchor>]`.

The public decorators become instances with an attribute docstring (the docs reference renders
attribute docstrings; the plan checks it with the docs build):

```python
asset = Decorator(Asset)
"""Create an Asset subclass from a decorated function. ..."""
```

`asset`, `view`, `source`, `connection`, `config`, `destination`, `schema` and `ibis_view` are
instances. `reference(*, table, **overrides)` stays a function, since it renames `table` to
`table_name` and has no bare form: `return Decorator(Reference, table_name=table, **overrides)`.

Every decorator keeps its exact call signature, its settings and their validation. A decorator's
settings stay keyword-only.

### 3.4 Removed

`decorate`, `declare`, `_route`, `_relations` (moved), `build_asset_class`, `_build_view_class`,
`_build_reference_class`, `_build_ibis_view_class`, `_build_source`, `_build_source_from_fn`,
`_build_source_from_class`, `_build_connection`, `_build_config`, `_build_destination`, the
`ConnectionT`/`ConfigT`/`DestinationT` TypeVars, the `connection` decorator's `oauth` check.

---

## 4. Ownership

- `Component._owned_by_field()` returns `{}`; `Component.owned` is built from it. `Source`
  overrides `_owned_by_field()` to return `{"assets": list(self.assets)}`. Nothing scans fields.
- `Source.model_post_init` wires its assets as today, and refuses one that belongs elsewhere:
  `ConfigError: asset '<key>' belongs to source '<other>'; give this source a copy (asset())`. This
  mirrors phase 2's class-level rule.
- A copy (`Component.__call__`) re-parents the owned children it copies, as today. A copy of an
  owned asset (`asset(enabled=False)`, the DAG's read-only upstream) keeps its `parent` and reads
  through it without being in the source's list, as today.
- The wire format is unchanged: owned components travel in the owner's spec as a key-to-init map,
  read back by `Source`'s `assets` validator.

---

## 5. Cascades

### 5.1 Removed

`Component.trickle`, `Component.on_rebind` and both overrides. `_replace_binding` no longer calls a
hook. What an owned asset reads through its source is phase 2's pull (`_targets`, `inherited`) and
does not change.

The source-to-destination cascade is removed: a destination configures its own connection. Every
manifest in the workspace and in `interloper-manifests`, and every platform destination (whose
`connection` relation is required), already does.

### 5.2 A job's default destinations

`Job.operations()` runs each target as this job runs it:

```python
def operations(self) -> list[Operation]:
    """The targets' operations, each target run with this job's bindings where it resolves none."""
    return [operation for target in self.targets for operation in self._run_as(target).operations()]

def _run_as(self, target: Source | Asset) -> Source | Asset:
    """The target as this job runs it: a copy carrying the job's bindings it lacks, or itself."""
```

For every relation the job binds other than `targets`, a target declaring a relation of that name
that resolves nothing (`_targets(name)` empty, so a source's own destinations and an owned asset's
source-provided ones win) receives the job's targets it accepts, every one for a `many` relation and
the first otherwise. The target is returned itself when it lacks nothing, else as a copy
(`target(**lent)`: same ids, the source's assets copied and re-parented). The target object is never
modified, and two jobs sharing a target each run it with their own destinations.

`job.targets` reads the targets as configured. The job's destinations show where they matter: in the
operations, the DAG and its spec (a container child receives the target with the destinations bound).

A job subclass declaring a further relation cascades it the same way, into targets only.

---

## 6. Reconstruction

- `Serializable.from_init(init, context) -> Self`: build an instance from a loaded init. The base is
  `cls(**init)`. `Component.from_init` holds the references back (`context.hold`), builds, and
  registers the instance (`context.add`): today's `issubclass(cls, Component)` branch, moved to its
  owner.
- `Spec.reconstruct` resolves the class, loads the init through `Spec._load(value, catalog,
  context)` (today's `load` closure), and calls `cls.from_init(...)`. It no longer imports
  `Component`, except where it resolves a catalog `key` (`Component.resolve_key`), unchanged.
- `SerializationContext.add` reads `instance.owned`.

---

## 7. Behaviour changes

Listed in the upgrade skill's row for this phase:

- A destination no longer receives a source's connection; bind it on the destination.
- A job no longer writes its destinations into its targets: `job.targets[0].destinations` is the
  target's own. The run is unchanged.
- `Component.trickle` and `Component.on_rebind` are removed.
- `decorate`, `declare` and `build_asset_class` are removed; `Serializable.build_class` takes a
  `ClassBody` and keyword settings; `Component._classvars` is `Serializable._classvars`.
- The public decorators are `Decorator` instances (their call forms are unchanged).
- A source given an asset another source owns raises `ConfigError`.
- A decorated class that already extends its anchor is a subclass of the class written: same name,
  key, kind and classpath, one more MRO level.

Unchanged: every decorator's call signature and settings, class keys, kinds and classpaths, catalog
definitions, the spec wire format (including owned assets as a key-to-init map), relation semantics
for owned assets, and what any run executes.

---

## 8. Out of scope, follow-ups

- `SerializationContext` stays in `serializable/`: moving it to the component package would give an
  author nothing and keep the same lazy import, in the other direction.
- `Component` itself is not split by concern (identity, relations, reconfiguration): phase 5 may,
  once this phase has removed `trickle` and `on_rebind`.
- Assigning `source.assets` after construction stays unsupported, as today.

---

## 9. Testing

Mirror layout, red then green per unit.

- `tests/serializable/test_body.py`: `from_class` for a plain class (members, annotations, the anchor
  as base) and for a subclass of the anchor (empty body, the class as base); `of` dispatches; a
  function on a class-only anchor is refused.
- `tests/serializable/test_base.py`: `build_class` sorts ClassVars and field defaults, keeps
  `FieldInfo` metadata, refuses an unknown or ambiguous name, names the subclass declaring a refused
  one; a subclass keeps an explicit class-body key (the `..._360_connection` case) and its kind; the
  decorated class is never modified; `from_init` on a plain `Serializable`.
- `tests/component/test_decorator.py`: bare and parenthesised forms; settings carried by a configured
  decorator; relations in every form collected once; `ty`-level typing pinned with
  `typing.assert_type` in a test module.
- `tests/component/test_base.py`: `owned` is empty on a plain component; `from_init` holds references
  and registers.
- `tests/asset/test_decorator.py`, `tests/source/test_decorator.py` and the per-kind decorator tests:
  unchanged behaviour through the new path (method assets, async assets, function-form sources).
- `tests/source/test_base.py`: `owned` is the assets; another source's asset is refused; a copy
  re-parents; a destination no longer receives the source's connection.
- `tests/job/test_base.py`: a target without destinations runs with the job's through
  `operations()`; the target object is unchanged; a target's own and an owned asset's
  source-provided destinations win; two jobs sharing a target each run it with their own.
- The docs build renders every decorator's docstring.
- Live: the demo job and the campaign-matcher manifest through `interloper run --dry-run`, and a
  demo run through the scheduler.
