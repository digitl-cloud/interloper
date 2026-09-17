# Operation is a Component Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Operation` a real `Component` so it stops declaring stand-in attributes it does not own.

**Architecture:** `Operation` becomes `Component, Workload`, and `Asset` becomes a plain subclass of
`Operation`. The `if TYPE_CHECKING` block and the `relations`/`source` runtime stand-ins are deleted,
because `id`, `kind`, `key`, `relations`, `to_spec()` and `bound()` are then inherited and real.
`materializable` moves up from `Asset` to `Operation`. Kinds need two explicit declarations:
`Operation` declares `kind = ""` so the derivation skips it, and `Asset` declares `kind = "asset"`
because `Component` is no longer one of its direct bases.

**Tech Stack:** Python 3.10+, pydantic v2, pydantic-settings, pytest, ruff, ty, uv workspace.

Spec: `docs/superpowers/specs/2026-09-17-operation-is-a-component-design.md`.

## Global Constraints

- Line length 120, ruff-formatted. Type-checked with `ty`.
- Google-style docstrings on every module, class, function and method, with every applicable section
  (`Args:`, `Returns:`, `Raises:`). See `.claude/rules/python-style.md`.
- Comment sparingly. Never attribute-level comments on pydantic fields.
- Tests mirror the package layout one to one. A test for `src/interloper/<pkg>/<module>.py` lives in
  `tests/<pkg>/test_<module>.py`. Never add a standalone `test_<feature>.py`.
- Run checks from the repo root: `uv run ruff check`, `uv run ty check`, `uv run pytest`.
- A fresh worktree needs `uv sync --all-packages --all-extras` first, or `ty` and `pytest` fail on
  optional imports (suds, facebook_business, OTel exporters).
- **Do not commit without Guillaume asking.** The commit step in each task records the intended
  message; run it only when he says so.

---

### Task 1: Operation becomes a Component

**Files:**
- Modify: `packages/interloper-core/src/interloper/operation/base.py:88-245`
- Modify: `packages/interloper-core/src/interloper/asset/base.py:102-150`
- Test: `packages/interloper-core/tests/operation/test_base.py`
- Test: `packages/interloper-core/tests/asset/test_base.py`
- Test: `packages/interloper-core/tests/connection/test_base.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `class Operation(Component, Workload)` with a real `id`, `kind`, `key`, `relations`,
  `to_spec()` and `bound()`, plus the field `materializable: bool` (default `True`, hidden from the
  public schema). `class Asset(Operation)` with `kind: ClassVar[str] = "asset"`. The retry plans
  declare `Operation.retry` on this class.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-core/tests/operation/test_base.py`:

```python
def test_operation_is_a_component_with_real_identity():
    class Thing(Operation):
        kind: ClassVar[str] = "thing"

        async def execute(self, context: OperationContext) -> OperationResult:
            return OperationResult()

    thing = Thing()
    assert isinstance(thing, Component)
    assert thing.kind == "thing"
    assert thing.key == "thing"
    assert thing.relations == {}
    assert UUID(thing.id)
    assert thing.to_spec() is not None
    assert thing.operations() == [thing]


def test_operation_declares_no_kind_of_its_own():
    assert Operation.kind == ""
    assert "destination" in il.KINDS


def test_operation_is_materializable_by_default():
    class Thing(Operation):
        kind: ClassVar[str] = "thing"

        async def execute(self, context: OperationContext) -> OperationResult:
            return OperationResult()

    assert Thing().materializable is True
    assert Thing(materializable=False).materializable is False
```

Add to `packages/interloper-core/tests/asset/test_base.py`:

```python
def test_asset_kind_is_declared_explicitly():
    assert Asset.__dict__["kind"] == "asset"
```

Add to `packages/interloper-core/tests/connection/test_base.py`:

```python
def test_connection_keeps_its_kind_and_hides_materializable():
    assert Connection.kind == "connection"
    assert Connection.model_fields["materializable"].json_schema_extra == {"x-hidden": True}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/operation/test_base.py packages/interloper-core/tests/asset/test_base.py::test_asset_kind_is_declared_explicitly packages/interloper-core/tests/connection/test_base.py::test_connection_keeps_its_kind_and_hides_materializable -v`
Expected: FAIL. `Operation` cannot be instantiated as a component, `Asset.__dict__` has a derived
rather than declared `kind`, and `materializable` is not a field on `Connection`.

- [ ] **Step 3: Rewrite the Operation class header**

In `packages/interloper-core/src/interloper/operation/base.py`, change the imports to bring in
`Component` (`from interloper.component.base import Component`) and `Field` from pydantic, drop the
now-unneeded `TYPE_CHECKING` imports of `Relation`, `SerializationContext` and `Spec`, and replace
the class header and the whole node-protocol block:

```python
class Operation(Component, Workload):
    """A unit of work: the node a DAG orders and a runner drives.

    An operation is a component: the DAG, the runner and the platform all
    address it by its component identity, and its events, executions and
    retries are keyed by its row id. What it adds to a component is the
    execution contract (:meth:`execute`, :meth:`failure`) and the node
    attributes the graph machinery reads, which ``Asset`` (the
    graph-structured, partitioned operation) narrows with its own fields.

    ``capture_traceback`` controls whether a failed execution's traceback
    is attached to its failure event; off for operations whose raw errors
    embed secrets (credential exchanges carry them in URLs).
    """

    kind: ClassVar[str] = ""
    partitioning: ClassVar[PartitionConfig | None] = None
    capture_traceback: ClassVar[bool] = True

    materializable: bool = Field(default=True, json_schema_extra={"x-hidden": True})
```

`kind = ""` is the opt-out the derivation in `Component.__init_subclass__` already honours: it only
derives a kind for a direct child that declares none. Do not instead make the derivation skip
abstract classes; `Destination` is abstract and *is* a kind, so that unregisters `destination` and
breaks every relation declared against it.

Delete the entire `if TYPE_CHECKING:` block (the `id`/`kind`/`key`/`relations`/`materializable`/
`source`/`partitioning` declarations and the `to_spec`/`bound` stubs) and the four runtime stand-ins
below it (`materializable = True`, `relations = {}`, `source = None`, `partitioning = None`), keeping
`source = None` only if step 5 shows a read that `Component.parent` does not already serve.

Everything from `qualified_key` downwards is unchanged.

- [ ] **Step 4: Rewrite the Asset class header**

In `packages/interloper-core/src/interloper/asset/base.py`, change the header and remove the two
declarations that moved up:

```python
class Asset(Operation):
    ...
    kind: ClassVar[str] = "asset"
```

Delete `materializable: bool = Field(default=True)` from the `# State` block, and delete
`partitioning: ClassVar[PartitionConfig | None] = None` from the `# Definition` block. Keep the
`Component` import only if it is still used elsewhere in the module; remove it otherwise.

- [ ] **Step 5: Resolve `source`**

Run: `uv run grep -rn "\.source" packages/interloper-core/src/interloper/dag packages/interloper-core/src/interloper/runner`

For each hit, confirm whether it reads an operation's owning source. If every hit is served by
`Asset.source` (assets) and nothing reads `source` on a non-asset operation, delete the `source =
None` stand-in. If any read reaches a `Connection`, keep `source = None` on `Operation` and record it
in the spec's follow-ups instead of changing behaviour here.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/operation packages/interloper-core/tests/asset packages/interloper-core/tests/connection -v`
Expected: PASS.

If `Connection` fails to build its model, the `BaseSettings` plus `BaseModel` diamond is the cause:
`Connection(Resource, Operation)` reaches `Component` through both bases. The MRO linearizes as
`Connection, Resource, BaseSettings, Operation, Component, ..., Workload, object`. Report the exact
pydantic error rather than working around it; this is the one risk the spec flagged as worth proving
before going further.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-core/src/interloper/operation/base.py packages/interloper-core/src/interloper/asset/base.py packages/interloper-core/tests
git commit -m "refactor(core)!: make an operation a component

By Digitl"
```

---

### Task 2: Sweep the workspace

**Files:**
- Modify: whatever the checks surface, across `packages/`.

**Interfaces:**
- Consumes: Task 1.
- Produces: a green workspace. No new API.

- [ ] **Step 1: Run the Python checks**

Run: `uv run ruff check && uv run ty check`
Expected: clean. Likely fallout: imports of `Component` that `asset/base.py` and other modules no
longer need, and `ty` errors where a caller narrowed an `Operation` to reach a component attribute
that is now inherited (those narrowings can be deleted).

- [ ] **Step 2: Run the full test suite**

Run: `uv run pytest`
Expected: PASS. Watch `packages/interloper-core/tests/dag`, `tests/runner` and
`packages/interloper-db/tests/store/test_hydration.py`, which are the regression surface for node
attribute reads and for reconstructing an operation from a spec.

- [ ] **Step 3: Check the catalog still builds**

Run: `uv run pytest packages/interloper-assets -q`
Expected: PASS. This is the broadest set of real `Asset` subclasses and decorator-built classes, so
it is what proves the inheritance change holds for the shipped definitions.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: fix fallout from operation becoming a component

By Digitl"
```
