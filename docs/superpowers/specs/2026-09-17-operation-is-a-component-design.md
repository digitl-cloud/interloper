# An operation is a component

Date: 2026-09-17. Status: approved design, implementation not started.

Scope: remove the stand-in attribute block from `Operation` by making it what every implementor
already is. Small, self-contained, and a prerequisite that simplifies
`2026-09-17-retry-design.md`: with this landed, `retry` is an ordinary field declared once on
`Operation` instead of a node-protocol declaration plus a component field.

---

## 1. Problem

`Operation` is a plain ABC that declares, under `if TYPE_CHECKING`, attributes it does not own:
`id`, `kind`, `key`, `relations`, `materializable`, `source`, `partitioning`, `to_spec()` and
`bound()`. Below the block it supplies runtime stand-ins for four of them (`materializable = True`,
`relations = {}`, `source = None`, `partitioning = None`).

That block is not a protocol. It is a promise that `Operation` will only ever be mixed into a
`Component`, written in the form `ty` accepts. The promise is already kept everywhere:

- `Asset(Component, Operation)`
- `Connection(Resource, Operation)`, where `Resource(BaseSettings, Component)`

and the platform depends on it. Events carry `component_id`/`component_kind`/`component_key` per
operation, the `executions` view is keyed by `(run_id, component_id)`, and
`RunExecutor._prior_successes` matches by component row id. There is no non-component operation
anywhere in the system, so the indirection buys a generality nothing uses and costs a block of
attributes that drift.

A `typing.Protocol` does not fix this. It expresses what the DAG and the runner *read*, which is the
consumer side, and carries neither defaults nor behaviour. `Operation` would keep its ABC for
`effective_partition`, `upstream_relations`, `failure` and `_event_metadata`, and keep the same
stand-ins. Two names for one concept, same duplication.

---

## 2. Decision

| Topic | Decision |
|---|---|
| Contract | `class Operation(Component, Workload)`. `class Asset(Operation)`. `Connection(Resource, Operation)` is unchanged in form. |
| The block | The whole `if TYPE_CHECKING` declaration and the `relations`/`source` stand-ins are deleted. `id`, `kind`, `key`, `relations`, `to_spec()` and `bound()` are inherited and real. |
| `Workload` | Unchanged. It stays a plain ABC: two members, no stand-ins, and implemented by things that are not operations (`Source`, `Job`). |
| Kinds | `Asset` declares `kind: ClassVar[str] = "asset"` explicitly, as `Connection` already declares its own. Auto-derivation stops reaching it once `Component` is no longer a direct base. |
| Opting out of a kind | `Operation` declares `kind: ClassVar[str] = ""` in its own body, which the existing derivation already honours (`"kind" not in cls.__dict__`). `__init_subclass__` is untouched: abstractness is not the signal, because `Destination` is an abstract class that is itself a kind. |
| Fields that move | `materializable` moves from `Asset` up to `Operation` as the same pydantic field. `partitioning` stays a `ClassVar` defaulting to `None`, declared once on `Operation`. |
| Compatibility | None required. Core and its consumers ship together. |

---

## 3. What moves

`interloper/operation/base.py`

```py
class Operation(Component, Workload):
    kind: ClassVar[str] = ""
    partitioning: ClassVar[PartitionConfig | None] = None
    capture_traceback: ClassVar[bool] = True

    materializable: bool = Field(default=True, json_schema_extra={"x-hidden": True})

    def operations(self) -> list[Operation]: ...
    def effective_partition(self, partition_or_window): ...
    def upstream_relations(self) -> dict[str, Relation]: ...
    def _validate_time_partitioning(self, partitioning, partition_or_window) -> None: ...
    def _event_metadata(self, metadata, partition_or_window=None) -> dict[str, Any]: ...

    @abstractmethod
    async def execute(self, context: OperationContext) -> OperationResult: ...
    def failure(self, error: Exception) -> OperationResult: ...
```

`Asset` drops its `materializable` and `partitioning` declarations and gains an explicit `kind`.
Everything else on `Asset` stays where it is.

`Operation.qualified_key` goes too. It returned the bare `key` as a stand-in for the qualified form
`Component.qualified_key` already builds from a component's identity. Under the old base order
(`Asset(Component, Operation)`) `Component`'s won; under `Asset(Operation)` the stand-in would win and
silently unqualify every asset key. Deleting it is the point of the refactor, and `Connection`, which
has no owner, reads the same either way.

---

## 4. Consequences to handle

**Kind derivation.** `__init_subclass__` derives `kind` only when `Component` is a direct base
(`any(base is Component for base in cls.__bases__)`) and the class does not declare one itself. Two
effects:

- `Operation` becomes a direct child and would derive `kind = "operation"`, a kind that does not
  exist. It declares `kind: ClassVar[str] = ""` instead, which the derivation already reads as an
  opt-out. Guarding on abstractness was tried and is wrong: `Destination` is an abstract class that
  is itself a kind, so skipping derivation for abstract classes unregisters `destination` and breaks
  every relation declared against it.
- `Asset` stops being a direct child and would silently inherit whatever `Operation` carries. It
  declares `kind: ClassVar[str] = "asset"` instead, which is what `Connection` already does. Future
  operation kinds declare theirs the same way.

`KINDS` itself is entry-point driven, so nothing about registration changes.

**MRO.** `Connection(Resource, Operation)` reaches `Component` through both bases. C3 linearizes it
as `Connection, Resource, BaseSettings, Operation, Component, ..., Workload, object`, which is valid,
but a `BaseSettings` and `BaseModel` diamond is where pydantic is occasionally awkward. This is the
one risk worth proving first, with a throwaway subclass, before touching anything else.

**Fields land on `Connection`.** Any pydantic field declared on `Operation` is collected for
`Connection` as well as `Asset`, so it enters the connection's config schema, its stored config and
its form. `materializable` therefore carries `x-hidden`, and every later field on `Operation` has to
make the same decision deliberately. This is the real cost of the change and the reason to keep
`Operation`'s field surface minimal.

**`source` collapses into `parent`.** `Asset.source` is literally `cast("Source | None",
self.parent)`, so a `source` on `Operation` is a second name for `Component.parent` living on a
contract where it is false for every implementor but one, with a `None` that is a null object rather
than an answer. The same shape as `qualified_key`, and it goes the same way.

Both generic readers want ownership, not source-ness. `DAG.to_spec` reads it to decide what to
serialize (no owner means the node's own spec, an owner means the owner's spec once, deduped), which
is the ownership rule `Component` already defines. `RunState._operation_event_metadata` stamps the
owner's id. Both now read `operation.parent`.

`Asset.source` stays: a typed accessor narrowing `parent` to `Source` on the one class where that
holds, and part of the authoring surface, since `@il.asset` injects `self.source` into `data()`. A
domain alias on the class where it is true is not the smell; hoisting it onto a contract where it is
not is.

**The event key follows.** All three producers of the owner's id (the node lifecycle metadata, the
asset-level metadata and the component log emitter) wrote `source_id`, and the telemetry attribute
was `interloper.source.id`. That named a guarantee the model does not make, so the key is `parent_id`
and the attribute `interloper.parent.id`. Renaming only the generic producer would have left two keys
for one value, which is worse than the name being loose.

The same line divides the plumbing: `EventLogger`, which is component-generic, takes `parent_id`,
while `ExecutionContext`, which is asset-specific, keeps `source_id` and fills it. Events written
before this carry the old key; they are history and are not rewritten, so anything reading the event
stream over a period spanning it has to accept both.

---

## 5. Testing

- `tests/operation/test_base.py`: a concrete `Operation` subclass has a real `id`, `key` and
  `relations`, serializes through `to_spec()`, and reports `operations() == [self]`.
- `tests/asset/test_base.py`: `Asset.kind == "asset"`, `materializable` still defaults to `True` and
  is still honoured by `Source.select`.
- `tests/connection/test_base.py`: `Connection.kind == "connection"`, it constructs from the
  environment as before, and `materializable` is hidden from its public schema.
- The existing DAG and runner suites are the regression surface for node-protocol reads and should
  pass untouched.

---

## 6. Follow-ups, recorded

- **`DAG.to_spec` duplicates the owner rule** that `Component` serialization already implements.
- **Explicit kinds everywhere.** With `Asset` and `Connection` declaring theirs, the auto-derivation
  in `__init_subclass__` serves only `Source`, `Job`, `Hook`, `Destination` and `Config`. Dropping it
  entirely in favour of an explicit declaration per anchor would remove a piece of magic, and is a
  separate, mechanical change.
