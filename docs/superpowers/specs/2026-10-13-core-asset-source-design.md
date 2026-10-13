# Core phase 2: Asset and Source

Date: 2026-10-13. Status: approved in conversation, implementation pending.

Phase 2 of the interloper-core consolidation programme (five stacked phases: scope, contexts and
observation; Asset and Source; the runner family; Component, Serializable and decorators; the
surface sweep). Phase 1 settled the execution spine. This phase settles how a source and its
assets relate at definition time: how an asset gets what it leaves unset, how an asset is
reached, how `Asset` is divided by concern, and how a component is reconfigured.

Scope: `interloper.component` (a new `inherited` concern, `Component.__call__`, the relation read
path, `to_spec`), `interloper.resource.fields` (one flag), `interloper.serializable` (one read),
`interloper.asset` (`base`, a new `output` module, `upstream`, `view`, `reference`, the
decorators), `interloper.source` (`base`, `decorator`), `interloper.operation` (`retry`), and the
consumers those seams reach: the interloper-sql `IbisView`, the db store's hydration and child
sync, the API's row counts, interloper-assets, the docs and the plugin skills. Built on phase 1 as
rebased onto main 0.111.0 (views, references, `Source.table`, `Source.format`).

---

## 1. Problem

A source copies five settings into each asset when it is built (`Source._resolve`: `dataset`,
`default_destination_key`, `normalizer`, `retry`, `materialization_strategy`) and pushes its
relation bindings into each asset (`on_rebind` calling `trickle`). Copies go stale, and every
consumer that cares works around it. Each row below was reproduced against the branch:

| # | Behaviour | Cause |
|---|---|---|
| 1 | An asset declaring `materialization_strategy=RECONCILE` under a `STRICT` source becomes `strict` | The copy cannot tell the default from an explicit value |
| 2 | `source(default_destination_key="warehouse")`: the copy has it, its assets keep `''` | Copied once, at construction |
| 3 | `source(normalizer=N)`: the copy has it, its assets keep none | Same |
| 4 | `source.retry = RetryPolicy(...)` after construction never reaches the assets | Same |
| 5 | `source.dataset = "other"` after construction never reaches the assets | Same |
| 6 | `source.to_spec()` writes every copied value into each asset's payload, freezing it | Same |

The workarounds: `Source.__call__` re-points `dataset` by comparing values and unbinds what it
had pushed; `to_spec` strips pushed bindings back out of child payloads through
`Component._trickled`; `_resolve` also formats `{field}` templates into each asset's dataset and
skips the table check for references.

Reaching an asset takes four mechanisms: an `AssetRef` descriptor replacing each asset written in
a source body, `Source.__getattr__` for assets given in `asset_types`, `Source.asset_def` for
definitions, and `_source_type` stamped in three places (the source's subclass hook and both
branches of `@il.source`). Statically, `Shop().orders` types as `type[Asset]`, which is why the
tests carry `# ty: ignore[unresolved-attribute]` on body-declared asset access.

`asset/base.py` is 955 lines and mixes seven concerns: reading the `data()` signature,
reconfiguration, the entry points, input assembly, upstream reads, output shaping, and destination
IO. Output shaping stores the run's schema on the instance (`_effective_schema`), written by
conform and read back by the write: two concurrent runs of one instance race on it. `View`,
`IbisView` and `Reference` extend `Asset` by overriding its private helpers (`_read_upstreams`,
`_normalize_and_conform`, `materialize_async`) and add their own (`_definition`,
`_view_destination`), so moving a helper silently breaks a kind.

`Asset.__call__` and `Source.__call__` each hand-list their keyword arguments with `None` meaning
"unchanged", plus a sentinel for `normalizer` and the opposite rule for relations.

---

## 2. Decisions

- **Effective on read.** An inherited setting reads its effective value: `asset.dataset` is the
  asset's own value, else what its source hands it. The field stores the override only, so specs
  and configs carry overrides only. Same pattern relations already use.
- **One owner seam.** What an owned component inherits is decided by its owner, in one method;
  `Source` overrides it for `dataset` alone, to fill templates. Location stays named by the source:
  `Source.table` for the table, `Source.inherited` for the dataset.
- **Asset classes are their own descriptor**, through their metaclass, as a `Relation` is.
- **`Asset` is divided by owner**: the signature is read once, output is a value, an upstream reads
  itself, and the asset kinds vary through three named hooks.
- **One reconfiguration** on `Component`: naming something changes it, leaving it out keeps it.
- Removed: `AssetRef`, `Source.__getattr__`, `Source.asset_def`, `Source.select`,
  `Source._resolve`, `Component._trickled`, `Asset._effective_schema`, `Asset._destinations`,
  `Asset._validate_destination`, `View._view_destination`, `View._definition`.
- Persisted contracts unchanged: the spec YAML shape (`assets` as key to init, `{ref: id}`), the
  definition JSON shape, event type strings, `x-*` extras (one added), the CLI.

---

## 3. One cascade rule

An owned component reads what it leaves unset from its owner, settings and relations alike.
Nothing is copied down.

### 3.1 Settings

The house field factories (`InputField`, `SelectField`, and the others built on `_extra`) take
`inherit=True`, rendered as `x-inherit`, beside `discriminator=True` and `section=`. A field so
marked defaults to `None`, meaning unset, and reads through a descriptor `Component` installs for
every marked field once Pydantic has built the class:

```py
class Asset(Operation):
    dataset: str | None = InputField(default=None, inherit=True, ...)
    default_destination_key: str | None = InputField(default=None, inherit=True, ...)
    normalizer: Normalizer | None = Field(default=None, json_schema_extra={"x-inherit": True})
    materialization_strategy: MaterializationStrategy | None = SelectField(default=None, inherit=True, ...)

class Operation(Component, Workload):
    retry: RetryPolicy | None = Field(default=None, json_schema_extra={"x-inherit": True, ...})
    enabled: bool = Field(default=True, json_schema_extra={"x-inherit": True, ...})
```

The read, in `component/inherited.py`:

1. The stored value, a blank string counting as unset (persisted forms save `""`).
2. With an owner: `owner.inherited(child, name, own)`, whatever the stored value was.
3. Without one: the stored value, `None` when unset.

`Component.inherited(child, name, own)` returns `own` when set, else the owner's own effective
value under that name when the owner declares it, else `None`. Every inherited setting already
reads `None` as its neutral value (the destination's default dataset, the first destination, no
normalizer, no retry, and reconcile, since conform only tests `is STRICT`), so `inherit=True` needs
no default of its own. `enabled` is the one inherited field with a non-`None` default: it is never
unset, so its read always reaches the owner with the stored value, and the owner decides.

The descriptor never affects storage: Pydantic validates into and serializes from `__dict__`,
equality and copies compare stored values, and `model_fields_set` is untouched. Reading a marked
field on the class returns `None`.

Bug 1 is gone by construction: an asset's strategy defaults to `None`, so an explicit `RECONCILE`
stays `RECONCILE`. Bugs 2 to 5 are gone because nothing is copied.

### 3.2 Templates and the source's location

`Source` overrides `inherited` for `dataset` and `enabled`:

```py
def inherited(self, child: Component, name: str, own: Any) -> Any:
    if name == "dataset":
        return self.format(own or self.dataset or self.key)
    if name == "enabled":
        return bool(own) and self.enabled
    return super().inherited(child, name, own)
```

`Source` gains `enabled: bool = True`, listed in `internal_fields` so the config form is unchanged:
an asset runs only when it and its source are both enabled. `source(enabled=False)`, the documented
way to put a read-only source in a run, keeps working through `Component.__call__`, and replaces
`select=[]` wherever a test made every asset of a source read-only.

So an asset's dataset is its own or its source's, a `{field}` template filled with the source's
fields either way, and the source key when neither is set. `Source.dataset` becomes
`InputField(default=None, ...)` and stops being rewritten to the key at construction. Its
`validate_key` check stays in `model_post_init`, now on `self.dataset or self.key`, so the error a
caller sees is unchanged.

The table check main introduced ("the result is validated when the source resolves its assets")
stays a construction-time check: `Source.model_post_init` validates `asset.table` for every asset
that is not a `Reference`, as `_resolve` does today.

### 3.3 Relations

`bound(name)` keeps meaning "what this component holds itself", and it is what specs, the db store
and `validate_relations` count as owned.

- The attribute read (`Relation.__get__`) returns the own binding, else the owner's targets under
  the same name that this relation accepts (every accepted target for a `many` relation, the
  first otherwise), the same filter `trickle` applies today.
- `resolve(name)` is that, else the relation's fallback.
- `validate_relations` counts a relation filled through the owner as filled.
- `Source.on_rebind` stops trickling into its assets; it keeps trickling into its destinations.
  `Job.on_rebind` is unchanged. Both are pushes into components that are not owned, and belong
  with `Component` in phase 4.

### 3.4 Removed

`Source._resolve` (its parent pointer and table check move to `model_post_init`),
`Component._trickled` and its use in `to_spec`, the dataset re-point and the unbinding in
`Source.__call__`.

### 3.5 Specs and persistence

`Serializable._fields_init` reads stored values (`__dict__`), not attributes, so a spec carries
overrides only (bug 6). Platform asset rows are created with only their key and hold overrides
already: no migration. A spec written by an earlier version carries the copied values, which now
read as explicit overrides with the same values; they differ only if the source changes later,
which is the staleness this phase removes.

---

## 4. Reaching an asset

An asset class is its own descriptor. `AssetMeta`, the metaclass of `Asset` (a subclass of
Pydantic's model metaclass), defines `__get__`:

- read on a class: the asset class (`Shop.orders`), which is what composite import paths
  (`module:Shop.orders`) walk;
- read on a `Source` instance: that instance's live asset of that key (`Shop().orders`);
- read on any other instance: the asset class, plain Python behaviour (a test class holding
  `asset_cls = users`, another component).

Its overloads type the instance read as `Asset`, so `ty` resolves `Shop().orders.connection`.
`Source` lists `AssetMeta` in `ignored_types`.

Collection happens once, in `Source.__init_subclass__`: the asset classes written in the body and
the ones in `asset_types` (which the function form of `@il.source` fills) form `asset_types`; a
listed asset is installed as a class attribute under its key; and each asset class's owning source
is set there, so neither `@il.source` builder sets it. A key colliding with another attribute of
the source, and an asset class already owned by another source, are `TypeError`s at definition.
A standalone asset is untouched: it owns no source, reads no owner, and keeps its dotted path.

Body-declared assets are the typed form; an asset given only in `asset_types` works at runtime and
is invisible to `ty`. The sources guide says so.

Removed: `AssetRef`, `Source.__getattr__`, the descriptor swap in `_collect_asset_types`,
`Source.asset_def` (no callers), `Source.select` and `_apply_select` (platform rows narrow a source
through the `assets` override map, which stays the only way). `source.assets`, `asset_types`, the
`assets` spec map and `sibling_bindings` are unchanged.

---

## 5. Asset by concern

### 5.1 The signature, read once

`Asset._collect` already walks `data()` to infer relations. It also records the parameters the
call fills, in order, as `_parameters: ClassVar[tuple[str, ...]]`: `context`, `source`, and the
relation names. `_build_kwargs` iterates it instead of calling `inspect.signature` on every run
with its own reserved names. `_RESERVED_PARAMETERS` stays the one list. A subclass inheriting
`data()` inherits the record.

### 5.2 Output is a value

`asset/output.py`:

```py
@dataclass(frozen=True)
class Output:
    data: Any
    schema: type[Schema] | None

    @classmethod
    def from_result(cls, result: Any, asset: Asset) -> Output: ...
```

`from_result` is today's unwrap, normalize, then conform or infer, with the same spans. It replaces
`_unwrap`, `_normalize_and_conform`, `_conform` and `_infer_schema`; `_effective_schema` is gone,
and so is the race.

### 5.3 Upstreams read themselves

```py
@classmethod
async def read(cls, upstream: Asset, reader: Asset, window: PartitionWindow | None, metadata: dict[str, Any]) -> Upstream: ...

@classmethod
def reference(cls, upstream: Asset, destination: Destination) -> Upstream: ...
```

`read` narrows the window to the upstream's, opens the `DEST_READ` step on the reader's metadata,
and turns `DataNotFoundError` into `data=None` with the warning event; any other failure raises
`AssetError` as today. `reference` is `Upstream(upstream, destination.reference(upstream))`.
`Asset` keeps the short loop that looks each bound upstream up in the DAG and skips one absent from
it, with its warning.

### 5.4 Three hooks for the kinds

Materializing is one pipeline in `Asset`: upstreams, then `data()`, then output, then landing.
The kinds vary through three hooks, and override nothing else:

| Hook | `Asset` | `View` | `IbisView` |
|---|---|---|---|
| `async _upstream(target, window, metadata) -> Upstream` | `Upstream.read(...)` when `reads_upstream_data`, else `Upstream.reference(target, self.default_destination)` | inherited: the class flag selects the reference | wraps the reference as a typed Ibis table |
| `_output(result) -> Output` | `Output.from_result(result, self)` | `Output(result, None)` | checks the expression, wraps it in `IbisDefinition` |
| `async _land(output, window, metadata)` | one `DEST_WRITE` step per destination, skipping an empty output with its warning | `create_view` in a `DEST_WRITE` step on `"view '<key>'"` | inherited |

`run_async` returns `output.data`; `materialize_async` lands the output and returns its data.
`reads_upstream_data` is the one fact both the DAG and `_upstream` read. `View.materialize_async`,
`_read_upstreams`, `_normalize_and_conform`, `_definition` and `_view_destination` go;
`View.validate_relations` keeps its one-destination and dialect checks, reading
`self.destinations`. `Reference` keeps its one override, `materialize_async`, returning `None`:
it never runs. A view's `run()` now returns what `_output` produced, so an `IbisView` returns its
`IbisDefinition` (its `expression` holds what `data()` returned).

### 5.5 Destinations

`_read_destination` becomes the public property `default_destination` (the destination whose key
is `default_destination_key`, else the first; `AssetError` when there is none), read by
`Upstream.read`, `_upstream` and `partition_row_counts`. `_destinations` and
`_validate_destination` are removed: every binding goes through `_replace_binding`, which checks
acceptance, and hydration binds through the constructor.

Kept on `Asset`: relation inference, `classpath`, `definition`, reconfiguration (section 6), the
entry points and `execute` (their shape is phase 3's), input assembly, the window check, the hooks.

---

## 6. One reconfiguration

`Asset.__call__` and `Source.__call__` become one `Component.__call__(**changes)`:

- A field name sets the field, validated against its type through Pydantic's assignment
  validator. On an inherited field, `None` means "inherit again".
- A relation name rebinds through the write path: one component, a list, or `None` to clear.
- Any other name is a `TypeError`.

The copy keeps its own bindings (a new dict, the same targets) and its owner; the components it
owns are deep-copied and pointed at the copy.

```py
asset(enabled=False)
asset(dataset="sandbox", normalizer=None)
source(dataset="raw_v2", connection=other_connection)
```

Changes: `None` no longer means "unchanged" (no source code passes it). `source(enabled=False)`
keeps its meaning, now as a field of the source its assets read through (section 3.2).

---

## 7. Consumers

- **interloper-sql**: `IbisView` moves from `_read_upstreams` and `_definition` to `_upstream` and
  `_output`.
- **interloper-db**: hydration still builds the `assets` map, from rows that hold overrides only;
  the `(dataset, table)` collision check reads effective values through the descriptor;
  `asset_types` and `sibling_bindings` unchanged. Tests using `select` change.
- **interloper-api**: `partition_row_counts` unchanged.
- **interloper-app**: an asset's `materialization_strategy` renders empty, meaning inherit; the
  source's nullable select already renders that way. No app change.
- **interloper-assets**: no source change; the `# ty: ignore[unresolved-attribute]` on
  body-declared asset access go.
- **Docs**: `guide/sources.md` (the cascade, templates, the body form as the typed form, no
  `select`), `guide/assets.md` (reconfiguration, `Output`, `default_destination`), the views
  guide where it names the removed hooks, the extending pages, the plugin skills that name
  `select`, `asset_def` or `__call__`, and the upgrade table row.

---

## 8. Errors

- Definition time, `TypeError`: an asset key colliding with a source attribute; an asset class
  owned by a second source; the signature errors as today.
- Reconfiguration: `TypeError` for an unknown name; Pydantic `ValidationError` for a wrong value.
- Relations: `ConfigError`, from the same write path.
- `Source.dataset` failing `validate_key`: the same `ValueError` as today, from `model_post_init`.

---

## 9. Out of scope, follow-ups

- Trickling into non-owned components (a source's destinations, a job's targets): phase 4.
- The entry point family (`run`, `run_async`, `materialize`, `materialize_async`, `execute`) and the
  operation lifecycle: phase 3.
- `Source.table`'s reference branch stays as main designed it.

---

## 10. Testing

Mirror layout, red then green per unit.

- `tests/component/test_inherited.py`: own value; the owner's; `None` standalone; blank string as
  unset; the owner seam called with the stored value; storage, equality, copy and dump see stored
  values only; the class read.
- `tests/resource/test_fields.py`: `inherit=True` renders `x-inherit`.
- `tests/component/test_base.py`: relation read-through (single, many, accepts filter);
  `resolve` falling back after the owner; `validate_relations` through the owner; `to_spec` with
  overrides only; `__call__` (fields, validation, inherited `None`, relations, unknown names, owned
  components copied and re-pointed).
- `tests/source/test_base.py`: the six rows of section 1 as regression tests; a disabled source
  disabling its assets, an asset disabled alone; templates through
  `inherited`; the construction-time dataset and table checks; `AssetMeta` on the
  class, on a source instance, on another holder, and for a listed asset; both definition errors;
  `select`, `asset_def`, `__getattr__` and `AssetRef` gone.
- `tests/asset/test_base.py`: `_parameters`; `default_destination`; two concurrent runs of one
  instance producing different schemas; `run_async` and `materialize_async` over the hooks.
- `tests/asset/test_output.py`, `tests/asset/test_upstream.py`: the moved logic.
- `tests/asset/test_view.py`, `tests/asset/test_reference.py`: the hooks; the write step's events.
- interloper-sql `tests/view/test_base.py`: `IbisView` over `_upstream` and `_output`.
- interloper-db: a hydration round trip with overrides only; the collision check through the
  descriptor.
