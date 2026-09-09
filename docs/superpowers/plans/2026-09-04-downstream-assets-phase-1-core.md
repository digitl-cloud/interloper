# Many-valued upstreams, phase 1 (core) Implementation Plan

> **SUPERSEDED 2026-09-07** by `2026-09-07-relation-model-phase-*.md` (design `2026-09-07-relation-model-design.md`). Kept for the record; phase 1 here was executed as PR #321 and is being reworked.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One upstream vocabulary and one contract. The asset-to-asset relation becomes `upstream` with the wiring field `upstreams: dict[str, list[str]]`; `depends_on: dict[str, str | il.Dependency]` declares what an asset needs (replacing `requires` and `optional_requires`); `il.Dependency` (the renamed `RelationSlot`, with `optional` and `many`) is both the declaration and the slot the catalog publishes. On top: the DAG resolves declared keys, one contract check fails unbound slots and incomplete signatures at build time, equal granularity is enforced across edges, every slot is read through one path with `None` for a leg without data, and `default_destination_key` is honoured on read.

**Architecture:** `Dependency` lives in `component/base.py` where `RelationSlot` lived, so resource slots and upstream slots share it. `Asset.declared_upstreams()` is the single reader of `depends_on`; `Asset.sibling_upstreams()` is the single sibling-wiring rule (used by the source at construction and by the store at creation); `Asset.validate_upstreams(nodes)` is the single contract check (signature, cardinality, identity). `upstreams` is always a list, so nothing shapes or flattens it. The leg object `Upstream` gets its own module `asset/upstream.py`.

**Tech Stack:** Python 3.10+, pydantic v2, Alembic (one migration), pytest, ruff, ty; a mechanical TypeScript rename keeps the app in step. Spec: `docs/superpowers/specs/2026-09-04-downstream-assets-design.md` (Part 2, sections 2.2 to 2.5).

## Global Constraints

- Branch `feat/many-upstreams-core` from `main`; rebase only, never merge; push with `--force-with-lease`.
- Conventional Commits; the renames are breaking, so their commits and the PR title use `feat!:`. Commit messages end with `By Digitl`. Commit only when Guillaume has asked for commits in the session; otherwise leave the work staged.
- Run Python from the repo root with `uv run --frozen ...` (never bare `uv run`, it rewrites `uv.lock`). Frontend commands from `packages/interloper-app/app/`.
- ruff line length 120; `uv run --frozen ty check` must pass; docstrings carry full Google sections (Args, Returns, Raises) on every function, private ones included.
- Tests mirror modules one to one: `tests/asset/test_upstream.py` for `asset/upstream.py`, existing files for existing modules. No standalone `test_<feature>.py`.
- Migration `017_upstream_relation.py` in `packages/interloper-db/src/interloper_db/migrations/versions/`, `revision = "017"`, `down_revision = "016"`, module docstring explaining the why.
- Comment sparingly; never restate the code. No em-dashes anywhere (code, docs, commit messages).
- `packages/interloper-core/src/interloper/` is the root of every core path below unless a path starts with `packages/`; `packages/interloper-core/tests/` for core tests.

---

### Task 1: The `upstream` relation and the `upstreams` field (mechanical rename)

A rename across every layer, done first so the feature tasks build on the final names. No behaviour changes in this task.

**Files:**
- Modify: `asset/base.py:163-171` (relation type, `internal_fields`), `asset/base.py:194` (field), every `dependencies` reader in `asset/base.py`, `dag/base.py`, `source/base.py`, `operation/base.py`, and the `"dependency"` literal wherever it names the relation type
- Modify: `packages/interloper-db/src/interloper_db/store/components.py` (`_wire_intra_deps`), `packages/interloper-db/src/interloper_db/store/relations.py` (`"dependency"` literals), `packages/interloper-toolkit/src/interloper_toolkit/lineage.py` (`type="dependency"`), `packages/interloper-scheduler/src/interloper_scheduler/executor.py:177-186`
- Create: `packages/interloper-db/src/interloper_db/migrations/versions/017_upstream_relation.py`
- Modify: `packages/interloper-db/src/interloper_db/models/components.py:158-166` (index)
- Modify (app): every `'dependency'` relation literal and `relations.dependency` access in `packages/interloper-app/app/app` (`stores/components.ts`, `composables/graphModel.ts`, `composables/warnings.ts`, `composables/collection.ts`, `composables/graph.ts`, `pages/graph.vue`, `components/sources/Wizard.vue`, `components/sources/AssetSelect.vue`, `types/catalog.ts` helpers `dependencySlots` and `requiredDependencies`)
- Modify: docs and skills that name the relation (`docs/guide/dependencies.md`, `docs/guide/specs.md`, `docs/extending/components.md`, `docs/extending/operations.md`, `plugins/interloper/skills/interloper-manifest/SKILL.md`)
- Test: every test that uses `dependencies=` or `type="dependency"` (core `tests/dag`, `tests/asset`, `tests/source`; db `tests/store/*`; toolkit; scheduler)

**Interfaces:**
- Produces: `Asset.relation_types["upstream"] = RelationDefinition(kinds=["asset"], field="upstreams", slotted=True, inline=False, on_unbind="block")`; the `dependency` type no longer exists.
- Produces: `Asset.upstreams: dict[str, str]` (list shape comes in Task 4); `Operation.upstreams` in the node protocol.
- Produces: `Source._resolve_upstreams` (was `_resolve_deps`), `Source._infer_upstreams` (was `_infer_all_requires`), `DAG._check_upstreams` (was `_check_requires`), store `_wire_intra_upstreams` (was `_wire_intra_deps`).
- Produces: app helpers `upstreamSlots(defn)` and `requiredUpstreams(defn)` (were `dependencySlots`, `requiredDependencies`); store getter `componentsStore.upstreams` (was `.dependencies`).
- Produces: persisted rows with `type = 'upstream'`.

- [ ] **Step 1: Write the failing test**

Append to `tests/asset/test_base.py`:

```python
def test_upstream_relation_replaces_dependency():
    relations = il.Asset.relation_types
    assert "dependency" not in relations
    assert relations["upstream"].field == "upstreams"
    assert relations["upstream"].kinds == ["asset"]
    assert relations["upstream"].inline is False
    assert FakeAsset(upstreams={"x": "id-1"}).upstreams == {"x": "id-1"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k upstream_relation_replaces`
Expected: FAIL with `KeyError: 'upstream'`.

- [ ] **Step 3: Rename in core**

In `asset/base.py`: the relation entry becomes `"upstream": RelationDefinition(kinds=["asset"], field="upstreams", slotted=True, inline=False, on_unbind="block")`; `internal_fields` lists `"upstreams"`; the field is `upstreams: dict[str, str] = Field(default_factory=dict)`; the `__call__` keyword and its Args entry become `upstreams`; every `self.dependencies` becomes `self.upstreams`. Rename `validate_dependencies` to `validate_upstreams`. Update the warnings filter at line 58 to `'Field name "(materializable|upstreams)" in "Asset"'`.

In `operation/base.py`: the node protocol attribute and default become `upstreams`; `validate_dependencies` becomes `validate_upstreams`.

In `dag/base.py`: `operation.dependencies` becomes `operation.upstreams`; `_check_requires` becomes `_check_upstreams` and calls `validate_upstreams`; the `mini_dag` comment "Wired dependencies are assets by relation schema" becomes "Wired upstreams are assets by relation schema".

In `source/base.py`: `_resolve_deps` becomes `_resolve_upstreams` (and its call), `_infer_all_requires` becomes `_infer_upstreams`; `asset.dependencies` becomes `asset.upstreams`.

Run `grep -rn "dependencies\b" packages/interloper-core/src/interloper` and rename every remaining reference to the wiring field; leave the words "dependency" and "dependencies" in prose where they mean the concept (docstrings about "dependency resolution" may stay, docstrings about the field must say `upstreams`).

- [ ] **Step 4: Rename in the store, toolkit and scheduler, and write the migration**

`packages/interloper-db/src/interloper_db/store/components.py`: `_wire_intra_deps` becomes `_wire_intra_upstreams`, `_add_relation(session, child, ..., "dependency", param_name)` becomes `"upstream"`, the `select(...).where(ComponentRelation.type == "dependency")` becomes `"upstream"`.

`packages/interloper-db/src/interloper_db/store/relations.py`: `if relation_type != "dependency":` becomes `"upstream"`; docstrings that say "dependency slot" become "upstream slot".

`packages/interloper-toolkit/src/interloper_toolkit/lineage.py`: every `type="dependency"` becomes `type="upstream"` (the tool names `get_upstream`, `get_downstream`, `cross_source_dependencies` stay).

`packages/interloper-scheduler/src/interloper_scheduler/executor.py:180`: `operation.dependencies.values()` becomes `operation.upstreams.values()`.

`packages/interloper-db/src/interloper_db/models/components.py`, replace the `Index(...)` block:

```python
        # Resource slots are single-valued by schema. Upstream slots are
        # single-valued only when their class says so, which the store
        # enforces from the slot contract (a many-valued slot fans in).
        Index(
            "uq_component_relations_slot",
            "src_id",
            "type",
            "slot",
            unique=True,
            postgresql_where=text("type = 'resource'"),
            sqlite_where=text("type = 'resource'"),
        ),
```

`017_upstream_relation.py`:

```python
"""Rename the asset-to-asset relation to ``upstream`` and let its slots hold several edges.

The relation type ``dependency`` named the role too loosely (every relation
is a dependency of sorts); ``upstream`` says which way the edge points and
matches the vocabulary the app, the lineage tools and the executor already
use. Persisted rows follow the rename.

A many-valued upstream slot binds every matching upstream, so the per-slot
uniqueness that made re-binding repoint an edge can no longer be a schema
rule for that type. Single-valued upstream slots keep repointing in
``RelationStore``, which knows the slot contract; resources stay unique by
schema.

Revision ID: 017
Revises: 016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "017"
down_revision: str | None = "016"
branch_labels: str | None = None
depends_on: str | None = None

_INDEX = "uq_component_relations_slot"
_TABLE = "component_relations"
_COLUMNS = ["src_id", "type", "slot"]


def upgrade() -> None:
    op.execute("UPDATE component_relations SET type = 'upstream' WHERE type = 'dependency'")
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(_INDEX, _TABLE, _COLUMNS, unique=True, postgresql_where=sa.text("type = 'resource'"))


def downgrade() -> None:
    # Fails if a slot holds several upstream edges; remove the extra legs first.
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX, _TABLE, _COLUMNS, unique=True, postgresql_where=sa.text("type IN ('resource', 'dependency')")
    )
    op.execute("UPDATE component_relations SET type = 'dependency' WHERE type = 'upstream'")
```

- [ ] **Step 5: Rename in the app**

From `packages/interloper-app/app/app`: `grep -rn "'dependency'\|\.dependency\b\|dependencySlots\|requiredDependencies" .` and rename: the relation literal to `'upstream'`, `relations.dependency` to `relations.upstream`, `dependencySlots` to `upstreamSlots`, `requiredDependencies` to `requiredUpstreams`, the store getter `dependencies` (which filters `relations` by type) to `upstreams` and its consumers. The `types/catalog.ts` docstrings that say "`dependency` (param → upstream asset key ...)" say `upstream`. Run `pnpm run lint && pnpm exec nuxt typecheck`.

- [ ] **Step 6: Rename in tests, docs and skills**

Tests: every `dependencies=` keyword and `.dependencies` access on assets becomes `upstreams`; every `type="dependency"` becomes `type="upstream"`; `validate_dependencies` call sites become `validate_upstreams`; the hydration test's `_wire_intra_deps` reference, if any, becomes `_wire_intra_upstreams`.

Docs: in `docs/guide/dependencies.md`, `docs/guide/specs.md`, `docs/guide/assets.md`, `docs/extending/components.md`, `docs/extending/operations.md` the field is `upstreams` and the relation type `upstream` (the guide's title and the word "dependency" for the concept stay); the spec example becomes `assets: {revenue: {upstreams: {orders: shop-orders}}}`. The manifest skill's YAML and prose follow.

- [ ] **Step 7: Run everything**

Run: `uv run --frozen pytest packages/interloper-core packages/interloper-db packages/interloper-toolkit packages/interloper-scheduler -q && uv run --frozen ty check` and, from `packages/interloper-app/app/`, `pnpm run lint && pnpm exec nuxt typecheck`.
Expected: PASS. `grep -rn "\"dependency\"\|'dependency'\|dependencies=" packages/*/src packages/interloper-app/app/app` returns nothing.

- [ ] **Step 8: Stage (commit only if asked)**

```bash
git add packages docs plugins
git commit -m "feat!: rename the asset-to-asset relation to upstream and its wiring field to upstreams

The relation type dependency becomes upstream (a role name, like target and
watch) and Asset.dependencies becomes Asset.upstreams, across core, store,
toolkit, scheduler, app, docs and specs. Migration 017 renames persisted
rows and narrows the per-slot unique index to resources.

By Digitl"
```

---

### Task 2: `Dependency` replaces `RelationSlot`; `Upstream` leg object

**Files:**
- Modify: `component/base.py:55-63` (`RelationSlot` becomes `Dependency`), `component/base.py:89`, `component/base.py:104`, `component/base.py:532`
- Modify: `component/__init__.py:8,16`, `__init__.py:10,147` (exports)
- Modify: `asset/base.py:16,352-354` (imports and the two `RelationSlot(...)` calls), `packages/interloper-db/src/interloper_db/store/relations.py:268,341`
- Modify: `docs/extending/components.md:84`
- Modify (app): `packages/interloper-app/app/app/types/catalog.ts:2-6,85`, `stores/components.ts:188`, `components/sources/AssetSelect.vue:57`
- Create: `asset/upstream.py`
- Test: `tests/asset/test_upstream.py` (create), `tests/component/test_base.py`

**Interfaces:**
- Produces: `interloper.component.base.Dependency(key: str = "", optional: bool = False, many: bool = False)`, pydantic model, exported as `il.Dependency`. `RelationSlot` and `il.RelationSlot` no longer exist.
- Produces: `RelationDefinition.slots: dict[str, Dependency]`; the catalog JSON field `required` becomes `optional` (inverted), `many` is added.
- Produces: `interloper.asset.upstream.Upstream(asset: Asset, data: Any)`, frozen dataclass, exported as `il.Upstream`; `data` is `None` for a bound leg with no data for the partition.

- [ ] **Step 1: Write the failing tests**

`tests/asset/test_upstream.py`:

```python
"""Tests for ``interloper.asset.upstream``."""

from __future__ import annotations

from typing import Any

import interloper as il
from interloper.asset.upstream import Upstream


def test_upstream_carries_asset_and_data():
    class Leg(il.Asset):
        """Fixture asset."""

    leg = Leg()
    upstream = Upstream(asset=leg, data=[{"id": 1}])
    assert upstream.asset is leg
    assert upstream.data == [{"id": 1}]
    assert il.Upstream is Upstream


def test_upstream_data_may_be_none():
    class Leg(il.Asset):
        """Fixture asset."""

    missing: Any = Upstream(asset=Leg(), data=None)
    assert missing.data is None
```

Append to `tests/component/test_base.py`:

```python
def test_dependency_defaults_and_flags():
    dependency = il.Dependency(key="*.campaigns")
    assert (dependency.optional, dependency.many) == (False, False)
    fan_in = il.Dependency(key="*.campaigns", optional=True, many=True)
    assert (fan_in.optional, fan_in.many) == (True, True)
    assert not hasattr(il, "RelationSlot")


def test_relation_definition_slots_are_dependencies():
    definition = il.RelationDefinition(kinds=["asset"], field="upstreams", slotted=True)
    enriched = definition.model_copy(update={"slots": {"x": il.Dependency(key="a")}})
    assert enriched.model_dump()["slots"]["x"] == {"key": "a", "optional": False, "many": False}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_upstream.py packages/interloper-core/tests/component/test_base.py -q`
Expected: FAIL with `ModuleNotFoundError: interloper.asset.upstream` and `AttributeError: module 'interloper' has no attribute 'Dependency'`.

- [ ] **Step 3: Rename the slot class**

`component/base.py`, replace the `RelationSlot` class:

```python
class Dependency(BaseModel):
    """One declared dependency of a component on another: a slot on a slotted relation type.

    The same object declares a slot on a class (an asset's ``depends_on``
    values, a resource slot derived from ``resource_types``) and publishes it
    in the class's definition. ``key`` names the expected component key
    (``""`` accepts any component of the relation's kinds; upstream slots use
    the asset key grammar, bare, ``source.asset`` or ``*.asset``);
    ``optional`` marks a slot that may stay unbound; ``many`` marks a slot
    that binds several components at once (a fan-in).
    """

    key: str = ""
    optional: bool = False
    many: bool = False
```

Then, in the same file: `slots: dict[str, Dependency] = Field(default_factory=dict)` (line 104); the `RelationDefinition` docstring sentence at line 89 becomes "Optional slots (``Dependency.optional=True``) detach regardless of this default."; line 532 becomes `name: Dependency(key=resource_type.key) for name, resource_type in cls.resource_types.items()`.

`component/__init__.py` and `__init__.py`: replace `RelationSlot` with `Dependency` in the import and in `__all__` (keep the lists sorted).

`asset/base.py:16`: import `Dependency` instead of `RelationSlot`; lines 352-354 become `Dependency(key=key)` and `Dependency(key=key, optional=True)` (Task 3 rewrites this block anyway).

`packages/interloper-db/src/interloper_db/store/relations.py:268`: the `slot_def` parameter type becomes `il.Dependency`; line 341: "optional (``Dependency.optional=True``) upstream". Search the store for `.required` on slot objects (`_relation_detaches`, `_blocked_unbinds`) and flip them: `slot is not None and slot.optional` and `not slot_def.optional`.

`docs/extending/components.md:84`: "The slots a concrete class declares (`Dependency(key, optional, many)`); `many` marks a slot that binds several components."

- [ ] **Step 4: Create the leg module and export it**

`asset/upstream.py`:

```python
"""The leg object handed to ``data()`` for many-valued upstream slots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True)
class Upstream:
    """One leg of a many-valued slot as handed to ``data()``.

    Attributes:
        asset: The upstream asset the data was read from; its ``source``,
            ``identity`` and ``id`` tell the legs apart.
        data: The data read from the upstream's destination for the run's
            partition, in that destination's read representation. ``None``
            when the upstream holds no data for that partition; a ``LOG``
            warning names the leg.
    """

    asset: Asset
    data: Any
```

`asset/__init__.py`:

```python
"""Assets: the core data-producing component, its execution context, and its decorator."""

from interloper.asset.base import Asset, AssetDefinition, AssetIdentity
from interloper.asset.context import ExecutionContext
from interloper.asset.decorator import asset
from interloper.asset.upstream import Upstream

__all__ = ["Asset", "AssetDefinition", "AssetIdentity", "ExecutionContext", "Upstream", "asset"]
```

`__init__.py` line 3 gains `Upstream` in the import and `"Upstream"` in `__all__`.

- [ ] **Step 5: Keep the app reading the renamed flag**

In `packages/interloper-app/app/app/types/catalog.ts` replace the interface (the JSON now carries `optional` and `many`):

```ts
/** A declared dependency: a slot on a slotted relation type. Mirrors `interloper.component.base.Dependency`. */
export interface Dependency {
    /** Expected dst component key. `''` accepts any component of the relation's kinds. */
    key: string
    /** Whether the slot may stay unbound. */
    optional: boolean
    /** Whether the slot binds several components (a fan-in). */
    many: boolean
}
```

and update `slots: Record<string, Dependency>`, `upstreamSlots(...): Record<string, Dependency>`, and the filter in `requiredUpstreams` to `.filter(([, s]) => !s.optional)`. `stores/components.ts:188` becomes `return !!slot && slot.optional`. `components/sources/AssetSelect.vue:57` becomes `const isOptional = slot.optional`. Run `pnpm run lint && pnpm exec nuxt typecheck` from `packages/interloper-app/app/`.

- [ ] **Step 6: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_upstream.py packages/interloper-core/tests/component -q && uv run --frozen pytest packages/interloper-core packages/interloper-db -q`
Expected: PASS. (`grep -rn RelationSlot packages docs` returns nothing.)

- [ ] **Step 7: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper packages/interloper-core/tests packages/interloper-db/src/interloper_db/store/relations.py docs/extending/components.md packages/interloper-app/app/app/types/catalog.ts packages/interloper-app/app/app/stores/components.ts packages/interloper-app/app/app/components/sources/AssetSelect.vue
git commit -m "feat(core)!: rename RelationSlot to Dependency with optional and many flags

The slot a class declares and the slot the catalog publishes are one class.
The catalog JSON carries optional (inverted from required) and many.

By Digitl"
```

---

### Task 3: `AssetIdentity.satisfies` with the source wildcard

**Files:**
- Modify: `asset/base.py:61-101` (`AssetIdentity`)
- Test: `tests/asset/test_base.py` (class `TestAssetIdentity`, around line 837)

**Interfaces:**
- Produces: `ANY_SOURCE = "*"` module constant in `asset/base.py`.
- Produces: `AssetIdentity.satisfies(self, declared_key: str, *, own_source_key: str | None = None) -> bool`.

- [ ] **Step 1: Write the failing tests**

Append to `TestAssetIdentity` in `tests/asset/test_base.py`:

```python
    def test_satisfies_bare_key_means_same_source(self):
        assert AssetIdentity("shop", "orders").satisfies("orders", own_source_key="shop")
        assert not AssetIdentity("warehouse", "orders").satisfies("orders", own_source_key="shop")

    def test_satisfies_qualified_key_names_the_source(self):
        assert AssetIdentity("shop", "orders").satisfies("shop.orders", own_source_key="finance")
        assert not AssetIdentity("warehouse", "orders").satisfies("shop.orders", own_source_key="finance")

    def test_satisfies_wildcard_accepts_any_source_including_none(self):
        assert AssetIdentity("facebook_ads", "campaigns").satisfies("*.campaigns")
        assert AssetIdentity("tiktok_ads", "campaigns").satisfies("*.campaigns")
        assert AssetIdentity(None, "campaigns").satisfies("*.campaigns")
        assert not AssetIdentity("facebook_ads", "ads").satisfies("*.campaigns")
```

Extend the file's import to `from interloper.asset.base import AssetDefinition, AssetIdentity` if `AssetIdentity` is not imported yet.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k satisfies`
Expected: FAIL with `AttributeError: 'AssetIdentity' object has no attribute 'satisfies'`.

- [ ] **Step 3: Implement**

In `asset/base.py`, add after `_UNSET = object()`:

```python
ANY_SOURCE = "*"
```

Add to `AssetIdentity`, after `resolve`:

```python
    def satisfies(self, declared_key: str, *, own_source_key: str | None = None) -> bool:
        """Whether this identity is an acceptable upstream for a declared key.

        A bare key expects an asset of the declaring source, a qualified key
        an asset of the named source type, and ``*.asset`` an asset of that
        key from any source, standalone assets included.

        Args:
            declared_key: The upstream key as written on the declaring asset.
            own_source_key: Key of the source declaring the upstream, used to
                scope a bare key. ``None`` for a standalone asset.

        Returns:
            True when the asset key matches and the source constraint holds.
        """
        expected = AssetIdentity.resolve(declared_key, own_source_key=own_source_key)
        if self.asset_key != expected.asset_key:
            return False
        return expected.source_key == ANY_SOURCE or self.source_key == expected.source_key
```

Update the `AssetDefinition` docstring's key list (asset/base.py:116-121): rewrite the two existing bullets with a comma and a colon instead of dashes, replace "``requires`` / ``optional_requires``" with "``depends_on``", and add:

```
    - **Wildcard key**, ``"*.campaigns"``: that asset key from any source.
      Used by many-valued slots (``Dependency(many=True)``) to fan in across providers.
```

Also update the `AssetIdentity` class docstring (asset/base.py:66-67): "``depends_on`` entries and upstream slot keys".

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k "AssetIdentity"`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper/asset/base.py packages/interloper-core/tests/asset/test_base.py
git commit -m "feat(core): match upstream keys through AssetIdentity.satisfies with a source wildcard

By Digitl"
```

---

### Task 4: `depends_on` replaces `requires` and `optional_requires`; `upstreams` is always a list

This task touches every reader of the two old fields in one go, core and store, so the workspace never has a half-migrated state.

**Files:**
- Modify: `asset/base.py:172-173` (class attributes), `asset/base.py:194` (field), `asset/base.py:339-361` (`relation_definitions`), `asset/base.py:438-467` (`validate_upstreams`), `asset/base.py:653-720` (`_build_kwargs`)
- Modify: `asset/decorator.py` (`depends_on` keyword replaces `requires` and `optional_requires`)
- Modify: `operation/base.py:113-129` (node protocol)
- Modify: `source/base.py:383-415` (`_infer_upstreams`), `source/base.py:532-556` (`_resolve_upstreams`)
- Modify: `dag/base.py:132-139` (optional check in `_build_graph`)
- Modify: `packages/interloper-db/src/interloper_db/store/components.py:793-830` (`_wire_intra_upstreams`)
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/executor.py:177-186` (`_resolve_upstream` walks lists)
- Test: `tests/asset/test_base.py`, `tests/asset/test_decorator.py:102,117`, `tests/dag/test_base.py:48-56`, `tests/source/test_base.py:270`, `packages/interloper-db/tests/store/test_components.py:215,268`, `packages/interloper-db/tests/store/test_relations.py:48,54,70`

**Interfaces:**
- Produces: `Asset.depends_on: ClassVar[dict[str, str | Dependency]]`; `Asset.requires` and `Asset.optional_requires` no longer exist.
- Produces: `Asset.declared_upstreams() -> dict[str, Dependency]` (classmethod) and `Operation.declared_upstreams(self) -> dict[str, Dependency]` (default `{}`).
- Produces: `Asset.sibling_upstreams(source_key: str, sibling_keys: Iterable[str]) -> dict[str, str]` (classmethod): parameter name to sibling asset key for every declared key that resolves to the declaring source.
- Produces: `Asset.upstreams: dict[str, list[str]]`; a bare string on input is wrapped by validator `_coerce_upstreams`.
- Produces: `@il.asset(depends_on=...)`; the `requires` and `optional_requires` keywords are gone.
- Produces: `Source._infer_upstreams` writes `Dependency(key=qualified, optional=True)` for a `None`-default sibling parameter.

- [ ] **Step 1: Write the failing tests**

Append to `tests/asset/test_base.py` at module level and new classes:

```python
class FakeFanIn(il.Asset):
    """Asset with one many-valued slot and one optional single slot."""

    depends_on: ClassVar[dict[str, Any]] = {
        "campaigns": il.Dependency(key="*.campaigns", many=True),
        "rules": il.Dependency(key="rules", optional=True),
    }

    def data(self, campaigns: list[il.Upstream], rules: Any = None) -> Any:  # pragma: no cover
        return None


class TestDeclaredUpstreams:
    def test_reads_strings_and_declarations(self):
        declared = FakeFanIn.declared_upstreams()
        assert declared["campaigns"] == il.Dependency(key="*.campaigns", many=True)
        assert declared["rules"] == il.Dependency(key="rules", optional=True)

    def test_plain_string_is_a_single_non_optional_slot(self):
        class Single(il.Asset):
            """Fixture."""

            depends_on: ClassVar[dict[str, Any]] = {"orders": "shop.orders"}

        assert Single.declared_upstreams()["orders"] == il.Dependency(key="shop.orders")

    def test_definition_publishes_the_same_objects(self):
        relation = FakeFanIn.definition().relations["upstream"]
        assert relation.slots == FakeFanIn.declared_upstreams()

    def test_decorator_accepts_declarations(self):
        @il.asset(depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)})
        def fan_in(campaigns: list[il.Upstream]) -> Any:  # pragma: no cover
            return None

        assert fan_in.declared_upstreams()["campaigns"].many is True

    def test_old_names_are_gone(self):
        assert not hasattr(il.Asset, "requires")
        assert not hasattr(il.Asset, "optional_requires")

    def test_sibling_upstreams_resolves_bare_and_own_qualified_keys_only(self):
        class Downstream(il.Asset):
            """Fixture."""

            depends_on: ClassVar[dict[str, Any]] = {
                "a": "a",
                "b": "shop.b",
                "c": "warehouse.c",
                "d": il.Dependency(key="*.d", many=True),
                "e": il.Dependency(key="e", optional=True),
            }

        assert Downstream.sibling_upstreams("shop", ["a", "b", "c", "d", "e"]) == {"a": "a", "b": "b", "e": "e"}


class TestUpstreamsShape:
    def test_bare_string_is_wrapped(self):
        assert FakeFanIn(upstreams={"rules": "id-9"}).upstreams == {"rules": ["id-9"]}

    def test_lists_are_kept(self):
        assert FakeFanIn(upstreams={"campaigns": ["id-1", "id-2"]}).upstreams["campaigns"] == ["id-1", "id-2"]

    def test_spec_round_trip_emits_lists(self):
        asset = FakeFanIn(upstreams={"campaigns": ["id-1", "id-2"], "rules": "id-9"})
        spec = asset.to_spec()
        assert spec.init["upstreams"] == {"campaigns": ["id-1", "id-2"], "rules": ["id-9"]}
        assert il.Asset.from_spec(spec).upstreams == {"campaigns": ["id-1", "id-2"], "rules": ["id-9"]}
```

Update the existing fixtures (same semantics, new spelling):

- `tests/asset/test_base.py:198`: `optional_requires: ClassVar[dict[str, str]] = {"extra": "other_source.extras"}` becomes `depends_on: ClassVar[dict[str, Any]] = {"extra": il.Dependency(key="other_source.extras", optional=True)}`; line 915 likewise with `"producer": il.Dependency(key="producer", optional=True)`. Every other `requires: ClassVar[...] = {...}` in the file becomes `depends_on: ClassVar[dict[str, Any]] = {...}` with the same string values.
- `tests/asset/test_decorator.py:102`: `requires={...}, optional_requires={"maybe": "another"}` becomes one `depends_on={..., "maybe": il.Dependency(key="another", optional=True)}`; line 117 becomes `assert declared.declared_upstreams()["maybe"] == il.Dependency(key="another", optional=True)`; every other `requires=` keyword in the file becomes `depends_on=`.
- `tests/dag/test_base.py:48-56`: `FakeAssetRequiringFake` declares `depends_on: ClassVar[dict[str, Any]] = {"upstream": "fake_asset"}`; `FakeAssetOptionallyRequiringFake` declares `depends_on: ClassVar[dict[str, Any]] = {"upstream": il.Dependency(key="fake_asset", optional=True)}` with a docstring saying "optional ``depends_on`` contract".
- `tests/source/test_base.py:270`: `assert "fake_a" in second_cls.optional_requires` becomes `assert second_cls.declared_upstreams()["fake_a"].optional is True`; any `requires` assertions in that file switch to `declared_upstreams()`.
- `packages/interloper-db/tests/store/test_components.py:215,268` and `packages/interloper-db/tests/store/test_relations.py:48,54,70`: `requires: ClassVar[dict[str, str]] = {...}` becomes `depends_on: ClassVar[dict[str, Any]] = {...}`; `optional_requires: ClassVar[dict[str, str]] = {"x": "key"}` becomes `depends_on: ClassVar[dict[str, Any]] = {"x": il.Dependency(key="key", optional=True)}`.
- Any existing test asserting `asset.upstreams == {"x": "<id>"}` (string shape) asserts `{"x": ["<id>"]}` instead.

Finish with `grep -rn "requires\b\|optional_requires" packages/*/tests --include='*.py' | grep -v "requires_"` and convert whatever is left that refers to the asset contract.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k "DeclaredUpstreams or UpstreamsShape"`
Expected: FAIL with `AttributeError: type object 'FakeFanIn' has no attribute 'declared_upstreams'`.

- [ ] **Step 3: Implement in `asset/base.py`**

Replace the two class attributes with one:

```python
    depends_on: ClassVar[dict[str, str | Dependency]] = {}
```

The field:

```python
    upstreams: dict[str, list[str]] = Field(default_factory=dict)
```

with a validator after `_validate_destinations`:

```python
    @field_validator("upstreams", mode="before")
    @classmethod
    def _coerce_upstreams(cls, value: Any) -> Any:
        """Accept a bare id where a list of ids is expected.

        Hand wiring and older specs write ``{"orders": "<id>"}``; the wiring
        is always a list, so a lone string is wrapped.

        Args:
            value: The raw ``upstreams`` input, parameter name to id or ids.

        Returns:
            The input with every string value wrapped in a one-element list.
        """
        if not isinstance(value, dict):
            return value
        return {name: [ids] if isinstance(ids, str) else ids for name, ids in value.items()}
```

Add the classmethods right before `relation_definitions`:

```python
    @classmethod
    def declared_upstreams(cls) -> dict[str, Dependency]:
        """The asset's upstream contract, one :class:`Dependency` per parameter.

        The single reading of ``depends_on``: a plain string is a
        non-optional single slot on that key, a :class:`Dependency` is taken
        as declared.

        Returns:
            Parameter name to declaration.
        """
        return {
            parameter: declared if isinstance(declared, Dependency) else Dependency(key=declared)
            for parameter, declared in cls.depends_on.items()
        }

    @classmethod
    def sibling_upstreams(cls, source_key: str, sibling_keys: Iterable[str]) -> dict[str, str]:
        """The declared upstreams that resolve to a sibling of the declaring source.

        The one sibling-wiring rule, shared by the source at construction and
        by the platform store at creation: a declared key whose source is the
        declaring source (bare, or qualified with its own key) and whose asset
        key names a sibling other than the asset itself.

        Args:
            source_key: Key of the source the asset belongs to.
            sibling_keys: Keys of the source's assets, the asset itself included.

        Returns:
            Parameter name to sibling asset key.
        """
        siblings = set(sibling_keys)
        wiring: dict[str, str] = {}
        for parameter, dependency in cls.declared_upstreams().items():
            if not dependency.key:
                continue
            expected = AssetIdentity.resolve(dependency.key, own_source_key=source_key)
            if expected.source_key == source_key and expected.asset_key in siblings and expected.asset_key != cls.key:
                wiring[parameter] = expected.asset_key
        return wiring
```

Import `Iterable` from `collections.abc` (the module already imports `Mapping` from there). Replace the slot construction in `relation_definitions`:

```python
        if "upstream" in relations:
            relations["upstream"] = relations["upstream"].model_copy(update={"slots": cls.declared_upstreams()})
```

and its docstring's first lines with: `"""Enrich the vocabulary with upstream slots and destination keys.` / `Upstream slots are :meth:`declared_upstreams`."""` (keep the Returns section).

Rewrite the body of `validate_upstreams` (Task 6 makes it the full contract check; here it only stops reading `requires`):

```python
        own_source_key = self._source.key if self._source is not None else None
        declared = self.declared_upstreams()
        for parameter_name, upstream_ids in self.upstreams.items():
            dependency = declared.get(parameter_name)
            if dependency is None or not dependency.key:
                continue
            for upstream_id in upstream_ids:
                if upstream_id not in nodes:
                    continue
                upstream = cast(Asset, nodes[upstream_id])
                if not upstream.identity.satisfies(dependency.key, own_source_key=own_source_key):
                    raise DependencyContractError(
                        f"Asset '{self.key}' parameter '{parameter_name}' depends on "
                        f"'{dependency.key}' but is wired to '{upstream.identity}'."
                    )
```

In `_build_kwargs`, replace `optional_names = set(self.optional_requires)` with `declared = self.declared_upstreams()`, read `self.upstreams[parameter_name][0]` where it read `self.dependencies[parameter_name]`, and replace every `parameter_name in optional_names` with `(declared.get(parameter_name) is not None and declared[parameter_name].optional)` (Task 7 rewrites this method around one read path).

- [ ] **Step 4: Implement in `asset/decorator.py`**

Replace the `requires` and `optional_requires` parameters (both overloads and the implementation) with one `depends_on: dict[str, str | Dependency]` (`= ...` in the overload, `| None = None` in the implementation); set `classvars["depends_on"] = depends_on` when given; import `from interloper.component import Dependency`; Args entry:

```
        depends_on: Upstream assets, keyed by ``data()`` parameter name. A
            value is an asset key (bare, qualified or ``*.asset``) for a
            non-optional single slot, or a
            :class:`~interloper.component.base.Dependency` for an optional or
            many-valued slot.
```

- [ ] **Step 5: Implement in `operation/base.py`**

Delete `optional_requires: ClassVar[Mapping[str, str]]` from the `TYPE_CHECKING` block and `optional_requires = {}  # noqa: RUF012` from the defaults; drop the now-unused `Mapping` import if nothing else uses it. The `upstreams` protocol attribute becomes `dict[str, list[str]]`. Import `Dependency` under `TYPE_CHECKING` from `interloper.component.base` and add after `effective_partition`:

```python
    def declared_upstreams(self) -> dict[str, Dependency]:
        """The node's declared upstream contract.

        The default declares nothing; ``Asset`` returns its ``depends_on``.

        Returns:
            Parameter name to declaration.
        """
        return {}
```

`Asset` inherits the classmethod from Step 3, which shadows this default by MRO (`Asset(Component, Operation)`); calling it on an instance works because classmethods bind on instances. If `ty` rejects a classmethod overriding an instance method, keep the `Operation` default and add on `Asset` an instance forwarder to a renamed classmethod; prefer the plain classmethod if `ty` accepts it.

- [ ] **Step 6: Implement in `source/base.py`**

Rewrite `_infer_upstreams`:

```python
    @classmethod
    def _infer_upstreams(cls) -> None:
        """Populate ``depends_on`` on asset classes from sibling parameter names.

        A parameter named after a sibling asset declares an upstream on it;
        a ``None`` default makes that upstream optional.
        """
        sibling_keys: set[str] = {a.key for a in cls.asset_types}
        for asset_cls in cls.asset_types:
            if not hasattr(asset_cls, "data"):
                continue
            signature = inspect.signature(asset_cls.data)
            inferred: dict[str, str | Dependency] = {}
            for parameter_name, parameter in signature.parameters.items():
                if parameter_name in ("self", "context", "source", "kwargs"):
                    continue
                if parameter_name in asset_cls.resource_types or parameter_name in asset_cls.depends_on:
                    continue
                if parameter_name in sibling_keys and parameter_name != asset_cls.key:
                    qualified = str(AssetIdentity(cls.key, parameter_name))
                    inferred[parameter_name] = (
                        Dependency(key=qualified, optional=True) if parameter.default is None else qualified
                    )
            if inferred:
                asset_cls.depends_on = {**asset_cls.depends_on, **inferred}
```

Import `Dependency` from `interloper.component`. Rewrite `_resolve_upstreams`:

```python
    def _resolve_upstreams(self, asset: Asset, siblings: dict[str, Asset]) -> None:
        """Wire intra-source upstreams for a single asset.

        Applies :meth:`~interloper.asset.base.Asset.sibling_upstreams` to the
        source's assets. Qualified keys naming another source and wildcard
        keys are left to the DAG, which sees every node.

        Pre-existing ``upstreams`` entries (e.g. hydrated from persisted
        relations) are never overwritten.

        Args:
            asset: The asset whose ``upstreams`` map is wired in place.
            siblings: The source's assets keyed by asset key, including *asset*
                itself.
        """
        for parameter_name, sibling_key in type(asset).sibling_upstreams(self.key, siblings).items():
            if parameter_name in asset.upstreams:
                continue
            asset.upstreams[parameter_name] = [siblings[sibling_key].id]
```

- [ ] **Step 7: Implement in `dag/base.py` and the store**

`dag/base.py:132-139`: replace `if parameter_name in operation.optional_requires:` with

```python
                    dependency = operation.declared_upstreams().get(parameter_name)
                    if dependency is not None and dependency.optional:
```

and iterate the list: `for parameter_name, upstream_ids in operation.upstreams.items(): for upstream_id in upstream_ids: ...` (Task 6 rewrites this loop fully).

`packages/interloper-db/src/interloper_db/store/components.py`, in `_wire_intra_upstreams`, replace

```python
            all_requires = {**asset_type.requires, **asset_type.optional_requires}
            for param_name, declared_key in all_requires.items():
                expected = AssetIdentity.resolve(declared_key, own_source_key=source_key)
                if expected.source_key != source_key or expected.asset_key == asset_key:
                    continue
                if expected.asset_key not in children_by_key or (child.id, param_name) in bound:
                    continue
                _add_relation(session, child, children_by_key[expected.asset_key], "upstream", param_name)
```

with

```python
            for param_name, sibling_key in asset_type.sibling_upstreams(source_key, children_by_key).items():
                if (child.id, param_name) in bound:
                    continue
                _add_relation(session, child, children_by_key[sibling_key], "upstream", param_name)
```

(`children_by_key` holds only enabled children, so a sibling excluded from the source yields no edge, as before.) Drop the now-unused `AssetIdentity` import if nothing else in the module uses it.

`packages/interloper-scheduler/src/interloper_scheduler/executor.py`, in `_resolve_upstream`, the inner loop walks the lists (the field is a list from this task on):

```python
            for operation in frontier:
                for upstream_ids in operation.upstreams.values():
                    for dependency_id in upstream_ids:
                        if dependency_id in visited:
                            continue
                        visited.add(dependency_id)
                        upstream = cast(il.Asset, self._store.components.load(UUID(dependency_id)))
                        upstream.materializable = False
                        operations.append(upstream)
                        next_frontier.append(upstream)
```

- [ ] **Step 8: Run the affected suites**

Run: `uv run --frozen pytest packages/interloper-core packages/interloper-db packages/interloper-scheduler -q && uv run --frozen ty check`
Expected: PASS, no type errors. `grep -rn "optional_requires\|\.requires\b\|requires=" packages/*/src` must return only the toolkit and agent lines (they read a dumped catalog key and are rewritten in phase 2).

- [ ] **Step 9: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper packages/interloper-core/tests packages/interloper-db/src/interloper_db/store/components.py packages/interloper-db/tests/store packages/interloper-scheduler/src/interloper_scheduler/executor.py
git commit -m "feat(core)!: declare upstreams with depends_on and wire them as lists

depends_on holds asset keys and il.Dependency values (optional, many) and
replaces requires and optional_requires; upstreams is always a list. Every
reader goes through Asset.declared_upstreams(), and sibling wiring through
Asset.sibling_upstreams() in both the source and the store.

By Digitl"
```

---

### Task 5: Sibling wiring and optional inference under `depends_on` (tests)

Task 4 already rewrote `_resolve_upstreams` and `_infer_upstreams`; this task pins the behaviour.

**Files:**
- Test: `tests/source/test_base.py`

- [ ] **Step 1: Write the tests**

```python
class FakeFanInSource(il.Source):
    """Source owning a many-valued slot and a sibling; the sibling is wired, the wildcard is not."""

    class Campaigns(il.Asset):
        """Sibling that happens to match the wildcard."""

    class Matches(il.Asset):
        """Fan-in asset."""

        depends_on: ClassVar[dict[str, Any]] = {
            "campaigns": il.Dependency(key="*.campaigns", many=True),
            "sibling": "campaigns",
        }

        def data(self, campaigns: list[il.Upstream], sibling: Any) -> Any:  # pragma: no cover
            return None


def test_resolve_upstreams_wires_bare_siblings_and_leaves_wildcards_to_the_dag():
    source = FakeFanInSource()
    assert source.matches.upstreams == {"sibling": [source.campaigns.id]}


def test_none_default_sibling_parameter_is_an_optional_upstream():
    class Pair(il.Source):
        """Source with an optional sibling upstream."""

        class First(il.Asset):
            """Upstream."""

        class Second(il.Asset):
            """Downstream with an optional sibling parameter."""

            def data(self, first: Any = None) -> Any:  # pragma: no cover
                return None

    declared = Pair.Second.declared_upstreams()["first"]  # ty: ignore[unresolved-attribute]
    assert declared == il.Dependency(key="pair.first", optional=True)
```

- [ ] **Step 2: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/source -q`
Expected: PASS.

- [ ] **Step 3: Stage (commit only if asked)**

```bash
git add packages/interloper-core/tests/source/test_base.py
git commit -m "test(core): pin sibling wiring and optional inference under depends_on

By Digitl"
```

---

### Task 6: DAG resolution and the single contract check

**Files:**
- Modify: `dag/base.py:78-142` (`__init__`, `_build_graph`), `dag/base.py:152-162` (`_check_upstreams`)
- Modify: `asset/base.py` (`validate_upstreams` becomes the full contract check), `operation/base.py` (`validate_upstreams` default docstring)
- Test: `tests/dag/test_base.py`, `tests/asset/test_base.py`

**Interfaces:**
- Produces: `DAG._resolve_declared(self) -> None`.
- Produces: `Asset.validate_upstreams(self, nodes: Mapping[str, Operation]) -> None` checking, in order, signature completeness (`AssetError`), cardinality (`DependencyNotFoundError`) and identity (`DependencyContractError`).
- Behaviour: after construction, `finance.revenue.upstreams == {"orders": [shop.orders.id]}` for a qualified key with one match; many slots bind every match; a bare key only binds within the same source instance; an unbound non-optional slot and an undeclared non-default parameter both raise at build; two single-slot candidates raise `DAGError`.

- [ ] **Step 1: Write the failing tests**

Add fixtures at module level in `tests/dag/test_base.py` (after the existing small fixtures):

```python
class FakeShop(il.Source):
    """Upstream source with an ``orders`` asset."""

    class Orders(il.Asset):
        """Orders."""


class FakeFinance(il.Source):
    """Downstream source depending on ``fake_shop.orders`` by qualified key."""

    class Revenue(il.Asset):
        """Revenue."""

        depends_on: ClassVar[dict[str, Any]] = {"orders": "fake_shop.orders"}

        def data(self, orders: Any) -> Any:  # pragma: no cover
            return None


class FakeProviderA(il.Source):
    """First provider with a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""


class FakeProviderB(il.Source):
    """Second provider with a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""


class FakeMatcher(il.Asset):
    """Standalone fan-in over every ``campaigns`` asset."""

    depends_on: ClassVar[dict[str, Any]] = {"campaigns": il.Dependency(key="*.campaigns", many=True)}

    def data(self, campaigns: list[il.Upstream]) -> Any:  # pragma: no cover
        return None


class FakeSloppy(il.Asset):
    """Asset whose ``data()`` takes a parameter nothing declares."""

    def data(self, ordres: Any) -> Any:  # pragma: no cover
        return None


class FakePairSource(il.Source):
    """Source whose second asset depends on its first by bare key."""

    class First(il.Asset):
        """Upstream."""

    class Second(il.Asset):
        """Downstream."""

        depends_on: ClassVar[dict[str, Any]] = {"first": "first"}

        def data(self, first: Any) -> Any:  # pragma: no cover
            return None
```

Add a test class:

```python
class TestDeclaredResolution:
    def test_qualified_key_resolves_to_the_single_match(self):
        shop, finance = FakeShop(), FakeFinance()
        dag = DAG(shop, finance)
        assert finance.revenue.upstreams == {"orders": [shop.orders.id]}
        assert dag.predecessors[finance.revenue.id] == [shop.orders.id]

    def test_qualified_key_with_two_matches_is_ambiguous(self):
        shop_one, shop_two, finance = FakeShop(), FakeShop(), FakeFinance()
        with pytest.raises(DAGError, match="2 matching assets"):
            DAG(shop_one, shop_two, finance)

    def test_unbound_slot_fails_at_build(self):
        finance = FakeFinance()
        with pytest.raises(DependencyNotFoundError, match="nothing is wired"):
            DAG(finance)

    def test_unbound_slot_is_ignored_on_read_only_nodes(self):
        finance = FakeFinance(materializable=False)
        DAG(finance)  # no raise

    def test_many_slot_binds_every_match(self):
        a, b, matcher = FakeProviderA(), FakeProviderB(), FakeMatcher()
        dag = DAG(a, b, matcher)
        assert set(matcher.upstreams["campaigns"]) == {a.campaigns.id, b.campaigns.id}
        assert set(dag.predecessors[matcher.id]) == {a.campaigns.id, b.campaigns.id}
        assert [op.key for op in dag.topological_generations()[-1]] == ["fake_matcher"]

    def test_many_slot_without_match_fails_unless_optional(self):
        with pytest.raises(DependencyNotFoundError, match="nothing is wired"):
            DAG(FakeMatcher())

    def test_many_slot_keeps_explicit_wiring(self):
        a, b, matcher = FakeProviderA(), FakeProviderB(), FakeMatcher()
        matcher.upstreams["campaigns"] = [a.campaigns.id]
        dag = DAG(a, b, matcher)
        assert dag.predecessors[matcher.id] == [a.campaigns.id]

    def test_wired_leg_of_wrong_identity_is_a_contract_error(self):
        a, matcher = FakeProviderA(), FakeMatcher()
        other = FakeOtherAsset()
        matcher.upstreams["campaigns"] = [a.campaigns.id, other.id]
        with pytest.raises(DependencyContractError):
            DAG(a, other, matcher)

    def test_bare_key_resolves_within_the_source_instance(self):
        one, two = FakePairSource(), FakePairSource()
        one.second.upstreams.clear()  # simulate a sibling wiring lost before build
        dag = DAG(one, two)
        assert one.second.upstreams == {"first": [one.first.id]}
        assert dag.predecessors[one.second.id] == [one.first.id]

    def test_undeclared_non_default_parameter_fails_at_build(self):
        with pytest.raises(AssetError, match="neither the context, a resource, nor a declared upstream"):
            DAG(FakeSloppy())
```

Import `AssetError` from `interloper.errors` at the top of the test module.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/dag/test_base.py -q -k DeclaredResolution`
Expected: FAIL (upstreams stay empty; no errors raised).

- [ ] **Step 3: Implement the resolution pass in `dag/base.py`**

In `_build_graph`, after the duplicate-id check and the `successors` initialisation, insert `self._resolve_declared()` and rewrite the edge loop:

```python
        for operation in self.operations:
            self.successors[operation.id] = []

        self._resolve_declared()

        for operation in self.operations:
            if not operation.materializable:
                continue

            self.predecessors[operation.id] = []
            declared = operation.declared_upstreams()
            for parameter_name, upstream_ids in operation.upstreams.items():
                dependency = declared.get(parameter_name)
                optional = dependency is not None and dependency.optional
                for upstream_id in upstream_ids:
                    if upstream_id not in self.operation_map:
                        if optional:
                            continue
                        raise DependencyNotFoundError(
                            f"'{operation.key}' upstream '{parameter_name}' points to id '{upstream_id}' "
                            f"which is not in the DAG."
                        )
                    self.predecessors[operation.id].append(upstream_id)
                    self.successors[upstream_id].append(operation.id)
```

Add the method after `_build_graph`:

```python
    def _resolve_declared(self) -> None:
        """Wire declared upstream keys that nothing has wired yet.

        For every live asset and every unwired declaration, the candidates
        are the other assets in the DAG whose identity satisfies the key; a
        bare key is further restricted to the asset's own source instance. A
        many-valued slot binds every candidate; a single slot binds exactly
        one. Wiring writes the asset's ``upstreams`` in place, the same way a
        source wires its siblings, so specs and the CLI need no extra step
        for cross-source contracts.

        Raises:
            DAGError: If a single slot has several candidates; the caller
                must wire it explicitly.
        """
        assets = [operation for operation in self.operations if isinstance(operation, Asset)]
        for asset in assets:
            if not asset.materializable:
                continue
            own_source_key = asset.source.key if asset.source is not None else None
            for parameter_name, dependency in asset.declared_upstreams().items():
                if asset.upstreams.get(parameter_name) or not dependency.key:
                    continue
                bare = "." not in dependency.key
                candidates = [
                    candidate
                    for candidate in assets
                    if candidate is not asset
                    and candidate.identity.satisfies(dependency.key, own_source_key=own_source_key)
                    and (not bare or candidate.source is asset.source)
                ]
                if not candidates:
                    continue
                if dependency.many:
                    asset.upstreams[parameter_name] = [candidate.id for candidate in candidates]
                elif len(candidates) > 1:
                    listed = ", ".join(f"{candidate.qualified_key}#{candidate.id[:8]}" for candidate in candidates)
                    raise DAGError(
                        f"'{asset.qualified_key}' parameter '{parameter_name}' depends on '{dependency.key}' and "
                        f"the DAG holds {len(candidates)} matching assets ({listed}); wire "
                        f"upstreams['{parameter_name}'] explicitly."
                    )
                else:
                    asset.upstreams[parameter_name] = [candidates[0].id]
```

Reduce `_check_upstreams` to the one call:

```python
    def _check_upstreams(self) -> None:
        """Let every live node validate its contract (see ``Asset.validate_upstreams``)."""
        for operation in self.operations:
            if operation.materializable:
                operation.validate_upstreams(self.operation_map)
```

- [ ] **Step 4: Make `validate_upstreams` the full contract check in `asset/base.py`**

```python
    def validate_upstreams(self, nodes: Mapping[str, Operation]) -> None:
        """Check the asset's contract against its signature and its wiring.

        Three checks, in order: every ``data()`` parameter without a default
        is the context, a resource, or a declared upstream (an undeclared one
        would surface as a ``TypeError`` inside ``data()`` at run time);
        every non-optional slot has at least one wired upstream present in
        *nodes*; every wired upstream present in *nodes* satisfies its slot
        key (bare keys expect the asset's own source, see
        :meth:`AssetIdentity.satisfies`). Called once per live node at DAG
        construction; upstream ids absent from *nodes* are ignored here,
        graph construction already rejected the non-optional ones.

        Args:
            nodes: Every node in the DAG, keyed by id.

        Raises:
            AssetError: If ``data()`` takes an undeclared parameter without a default.
            DependencyNotFoundError: If a non-optional slot has nothing wired in *nodes*.
            DependencyContractError: If a wired upstream violates its slot key.
        """
        declared = self.declared_upstreams()
        for parameter_name, parameter in inspect.signature(self.data).parameters.items():
            if parameter_name in ("self", "context", "source", "kwargs") or parameter_name in self.resource_types:
                continue
            if parameter_name not in declared and parameter.default is inspect.Parameter.empty:
                raise AssetError(
                    f"{type(self).__name__}.data(): parameter '{parameter_name}' is neither the context, a resource, "
                    f"nor a declared upstream; add it to depends_on or give it a default."
                )

        own_source_key = self._source.key if self._source is not None else None
        for parameter_name, dependency in declared.items():
            present = [upstream_id for upstream_id in self.upstreams.get(parameter_name, []) if upstream_id in nodes]
            if not dependency.optional and not present:
                raise DependencyNotFoundError(
                    f"'{self.qualified_key}' depends on '{dependency.key}' for parameter '{parameter_name}' "
                    f"but nothing is wired in the DAG."
                )
            if not dependency.key:
                continue
            for upstream_id in present:
                upstream = cast(Asset, nodes[upstream_id])
                if not upstream.identity.satisfies(dependency.key, own_source_key=own_source_key):
                    raise DependencyContractError(
                        f"Asset '{self.key}' parameter '{parameter_name}' depends on "
                        f"'{dependency.key}' but is wired to '{upstream.identity}'."
                    )
```

Import `DependencyNotFoundError` from `interloper.errors` in `asset/base.py`. Update the `Operation.validate_upstreams` default docstring: "The default has nothing to validate; ``Asset`` checks its signature, its cardinality and its wired identities here."

- [ ] **Step 5: Run the whole core suite**

Run: `uv run --frozen pytest packages/interloper-core -q`
Expected: PASS. If a pre-existing test builds a DAG around an asset whose non-optional slot is deliberately unbound, or whose `data()` takes an undeclared non-default parameter, and expects success, fix the fixture (wire it, declare it, or give the parameter a default); do not weaken the new checks. The demo source's `b(self, context, a, x: str | None = None)` passes as is.

- [ ] **Step 6: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper/dag/base.py packages/interloper-core/src/interloper/asset/base.py packages/interloper-core/src/interloper/operation/base.py packages/interloper-core/tests/dag/test_base.py
git commit -m "feat(core): resolve declared upstreams in the DAG and check the whole contract at build

By Digitl"
```

---

### Task 7: One read path, `list[Upstream]` for many slots, `None` for missing legs

**Files:**
- Modify: `asset/base.py:653-720` (`_build_kwargs`), new `_read_upstreams`
- Test: `tests/asset/test_base.py`

**Interfaces:**
- Produces: `Asset._read_upstreams(self, parameter_name: str, upstream_ids: list[str], dag: DAG, partition_or_window, metadata) -> list[Upstream]`.
- Behaviour: every declared slot is read through `_read_upstreams`. A many slot receives the legs (`[]` when nothing is bound); a single slot receives the first leg's data, or `None` when the slot is optional and unbound or its leg holds no data. A leg whose read fails with `DataNotFoundError` as the cause arrives with `data=None` and a `LOG` warning; any other read error fails the asset, optional or not.

- [ ] **Step 1: Write the failing tests**

Append to `tests/asset/test_base.py` (fixtures must be module level):

```python
class FakeLegSourceOne(il.Source):
    """Provider one."""

    class Campaigns(il.Asset):
        """Daily campaigns."""

        partitioning: ClassVar[PartitionConfig | None] = TimePartitionConfig(column="date")

        def data(self, context: il.ExecutionContext) -> Any:
            return [{"date": context.partition_date, "id": "one"}]


class FakeLegSourceTwo(il.Source):
    """Provider two."""

    class Campaigns(il.Asset):
        """Daily campaigns."""

        partitioning: ClassVar[PartitionConfig | None] = TimePartitionConfig(column="date")

        def data(self, context: il.ExecutionContext) -> Any:
            return [{"date": context.partition_date, "id": "two"}]


class TestUpstreamReads:
    @staticmethod
    def _matcher() -> type[il.Asset]:
        @il.asset(
            depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
            partitioning=TimePartitionConfig(column="date"),
        )
        def matches(context: il.ExecutionContext, campaigns: list[il.Upstream]) -> Any:
            return [
                {
                    "date": context.partition_date,
                    "source": leg.asset.source.key,
                    "rows": len(leg.data) if leg.data is not None else None,
                }
                for leg in campaigns
            ]

        return matches

    def test_many_slot_receives_one_upstream_per_leg(self):
        mem = il.MemoryDestination()
        one, two = FakeLegSourceOne(destinations=[mem]), FakeLegSourceTwo(destinations=[mem])
        matcher = self._matcher()(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        result = DAG(one, two, matcher).materialize(partition)
        assert result.status is ExecutionStatus.COMPLETED
        rows = mem.read(il.IOContext(asset=matcher, partition_or_window=partition))
        assert sorted(row["source"] for row in rows) == ["fake_leg_source_one", "fake_leg_source_two"]
        assert all(row["rows"] == 1 for row in rows)

    def test_missing_leg_arrives_as_none_with_a_warning(self):
        mem = il.MemoryDestination()
        one, two = FakeLegSourceOne(destinations=[mem]), FakeLegSourceTwo(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        DAG(one).materialize(partition)  # only provider one has data
        matcher = self._matcher()(destinations=[mem])
        warnings_seen: list[Event] = []
        EventBus.subscribe(lambda e: warnings_seen.append(e) if e.metadata.get("level") == "WARNING" else None)
        result = DAG(one(materializable=False), two(materializable=False), matcher).materialize(partition)
        assert result.status is ExecutionStatus.COMPLETED
        rows = {row["source"]: row["rows"] for row in mem.read(il.IOContext(asset=matcher, partition_or_window=partition))}
        assert rows == {"fake_leg_source_one": 1, "fake_leg_source_two": None}
        assert any("found no data in upstream" in e.metadata.get("message", "") for e in warnings_seen)

    def test_other_read_errors_fail_the_asset(self):
        class Broken(il.Destination):
            """Destination whose reads always fail for a reason other than missing data."""

            def read(self, context: il.IOContext) -> Any:
                raise RuntimeError("boom")

            def write(self, context: il.IOContext, data: Any) -> None:
                return None

        mem = il.MemoryDestination()
        one = FakeLegSourceOne(destinations=[Broken()])
        matcher = self._matcher()(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        result = DAG(one(materializable=False), matcher).materialize(partition)
        assert result.status is ExecutionStatus.FAILED

    def test_optional_many_slot_with_nothing_bound_receives_an_empty_list(self):
        @il.asset(depends_on={"campaigns": il.Dependency(key="*.campaigns", optional=True, many=True)})
        def lonely(campaigns: list[il.Upstream]) -> Any:
            return [{"n": len(campaigns)}]

        asset = lonely(destinations=[il.MemoryDestination()])
        assert asset.run(dag=DAG(asset)) == [{"n": 0}]

    def test_single_slot_receives_raw_data(self):
        mem = il.MemoryDestination()
        one = FakeLegSourceOne(destinations=[mem])
        partition = TimePartition(dt.date(2026, 1, 1))
        DAG(one).materialize(partition)

        @il.asset(depends_on={"c": "fake_leg_source_one.campaigns"}, partitioning=TimePartitionConfig(column="date"))
        def single(context: il.ExecutionContext, c: Any) -> Any:
            return [{"date": context.partition_date, "ids": [row["id"] for row in c]}]

        asset = single(destinations=[mem])
        DAG(one(materializable=False), asset).materialize(partition)
        assert mem.read(il.IOContext(asset=asset, partition_or_window=partition)) == [
            {"date": dt.date(2026, 1, 1), "ids": ["one"]}
        ]

    def test_optional_single_slot_is_none_on_missing_data_and_fails_otherwise(self):
        mem = il.MemoryDestination()
        one = FakeLegSourceOne(destinations=[mem])

        @il.asset(
            depends_on={"c": il.Dependency(key="fake_leg_source_one.campaigns", optional=True)},
            partitioning=TimePartitionConfig(column="date"),
        )
        def lenient(context: il.ExecutionContext, c: Any = None) -> Any:
            return [{"date": context.partition_date, "got": c is not None}]

        asset = lenient(destinations=[mem])
        partition = TimePartition(dt.date(2030, 5, 5))  # provider never ran for this day
        result = DAG(one(materializable=False), asset).materialize(partition)
        assert result.status is ExecutionStatus.COMPLETED
        assert mem.read(il.IOContext(asset=asset, partition_or_window=partition)) == [
            {"date": dt.date(2030, 5, 5), "got": False}
        ]

        class Broken(il.Destination):
            """Destination whose reads always fail for a reason other than missing data."""

            def read(self, context: il.IOContext) -> Any:
                raise RuntimeError("boom")

            def write(self, context: il.IOContext, data: Any) -> None:
                return None

        broken = FakeLegSourceOne(destinations=[Broken()])
        asset = lenient(destinations=[mem])
        assert DAG(broken(materializable=False), asset).materialize(partition).status is ExecutionStatus.FAILED
```

Check the top of the test file already imports `dt`, `Event`, `EventBus`, `ExecutionStatus`, `TimePartition`, `TimePartitionConfig` (it does per the module header); if the `EventBus.subscribe` API differs, mirror how the existing tests in this file capture events. The last test changes behaviour for optional single slots (a non-missing-data failure now fails instead of yielding `None`); if an existing test asserts the old swallow-everything behaviour, update it to expect `FAILED`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k UpstreamReads`
Expected: FAIL (`data()` receives the wrong shape or a `TypeError`).

- [ ] **Step 3: Implement**

`asset/base.py`: import `DataNotFoundError` from `interloper.errors` and `Upstream` from `interloper.asset.upstream`. Rewrite the dependency branch of `_build_kwargs` around one read:

```python
        declared = self.declared_upstreams()

        for parameter_name in signature.parameters:
            if parameter_name in ("self", "source", "kwargs"):
                continue
            if parameter_name == "context":
                kwargs["context"] = context
            elif parameter_name in self.resource_types:
                # Lazily-built clients cost under the data() span, not here.
                with tracer().start_as_current_span(
                    "interloper.asset.resolve_resource",
                    attributes={**self._span_attributes(), telemetry_attributes.RESOURCE_NAME: parameter_name},
                ):
                    kwargs[parameter_name] = self._resolve_resource(parameter_name)
            elif parameter_name in declared:
                dependency = declared[parameter_name]
                upstream_ids = self.upstreams.get(parameter_name, [])
                if upstream_ids and dag is None:
                    raise AssetError(
                        f"Asset '{self.key}' has upstreams but no DAG provided. "
                        "Pass a DAG to run() or materialize() for upstream resolution."
                    )
                legs = (
                    await self._read_upstreams(parameter_name, upstream_ids, dag, partition_or_window, context.metadata)
                    if upstream_ids and dag is not None
                    else []
                )
                if dependency.many:
                    kwargs[parameter_name] = legs
                elif legs:
                    kwargs[parameter_name] = legs[0].data
                elif dependency.optional:
                    kwargs[parameter_name] = None
```

(A non-optional single slot always has a leg here: the DAG's contract check guarantees a wired, present upstream.) Add the helper after `_build_kwargs`:

```python
    async def _read_upstreams(
        self,
        parameter_name: str,
        upstream_ids: list[str],
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Read every leg wired into one slot.

        A leg whose destination holds no data for the scope is handed over
        with ``data=None`` and a warning event, never dropped: the asset
        decides what a missing leg means. Any other read failure fails the
        asset, optional slot or not.

        Args:
            parameter_name: The ``data()`` parameter the legs are read for.
            upstream_ids: The wired upstream ids, every one present in *dag*.
            dag: The DAG the upstream assets are looked up in.
            partition_or_window: Scope of the reads.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            One :class:`Upstream` per leg, in wiring order.

        Raises:
            AssetError: If a leg cannot be read for a reason other than
                missing data.
        """
        legs: list[Upstream] = []
        for upstream_id in upstream_ids:
            upstream_asset = cast(Asset, dag.operation_map[upstream_id])
            try:
                data = await self._destination_read(upstream_asset, partition_or_window, metadata)
            except AssetError as error:
                if not isinstance(error.__cause__, DataNotFoundError):
                    raise
                EventBus.emit(
                    EventType.LOG,
                    metadata={
                        **self._event_metadata(metadata, partition_or_window),
                        "level": "WARNING",
                        "message": (
                            f"Asset '{self.key}' found no data in upstream '{upstream_asset.qualified_key}' for "
                            f"parameter '{parameter_name}' at {partition_or_window}; the leg is passed with data=None"
                        ),
                    },
                );
                data = None
            legs.append(Upstream(asset=upstream_asset, data=data))
        return legs
```

(Remove the stray `;` after the `EventBus.emit(...)` call when transcribing; it is not valid style.) Update the `_build_kwargs` docstring: "Every declared upstream is read through :meth:`_read_upstreams`; many-valued slots receive the legs, single slots the first leg's data."

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper/asset/base.py packages/interloper-core/tests/asset/test_base.py
git commit -m "feat(core)!: read every upstream slot through one path, missing legs as None

By Digitl"
```

---

### Task 8: Equal granularity across an upstream edge

**Files:**
- Modify: `dag/base.py:186-204` (`_check_partition_dependencies`)
- Test: `tests/dag/test_base.py`

- [ ] **Step 1: Write the failing test**

```python
class FakeDaily(il.Asset):
    """Daily asset."""

    partitioning: ClassVar[PartitionConfig | None] = il.TimePartitionConfig(column="date")


class FakeMonthly(il.Asset):
    """Monthly asset depending on a daily one."""

    partitioning: ClassVar[PartitionConfig | None] = il.TimePartitionConfig(
        column="date", granularity=il.TimeGranularity.MONTH
    )
    depends_on: ClassVar[dict[str, Any]] = {"daily": "fake_daily"}

    def data(self, daily: Any) -> Any:  # pragma: no cover
        return None


class TestGranularityAcrossEdges:
    def test_mixed_granularity_raises_even_for_read_only_upstreams(self):
        daily = FakeDaily(materializable=False)
        monthly = FakeMonthly(upstreams={"daily": [daily.id]})
        with pytest.raises(DAGError, match="partitioned by day"):
            DAG(daily, monthly)

    def test_equal_granularity_passes(self):
        upstream = FakeDaily()
        downstream = FakeDaily(upstreams={"x": [upstream.id]})
        DAG(upstream, downstream)  # no raise
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-core/tests/dag/test_base.py -q -k Granularity`
Expected: FAIL (no error raised for the mixed case).

- [ ] **Step 3: Implement**

Extend the loop in `_check_partition_dependencies` (import `TimePartitionConfig` from `interloper.partitioning`):

```python
        for operation_id, preds in self.predecessors.items():
            operation = self.operation_map[operation_id]
            for pred_id in preds:
                upstream = self.operation_map[pred_id]
                if upstream.partitioning is not None and operation.partitioning is None:
                    raise DAGError(
                        f"Invalid upstream: partitioned asset '{upstream.key}' "
                        f"cannot be an upstream of non-partitioned asset '{operation.key}'"
                    )
                if (
                    isinstance(upstream.partitioning, TimePartitionConfig)
                    and isinstance(operation.partitioning, TimePartitionConfig)
                    and upstream.partitioning.granularity is not operation.partitioning.granularity
                ):
                    raise DAGError(
                        f"Invalid upstream: '{upstream.key}' is partitioned by "
                        f"{upstream.partitioning.granularity.value} but its dependent '{operation.key}' by "
                        f"{operation.partitioning.granularity.value}; a run has one partition scope, so both "
                        f"ends of an edge must share a granularity."
                    )
```

Update the method docstring: "Check partition compatibility along every edge: no unpartitioned dependent of a partitioned upstream, and equal granularity between time-partitioned ends. Read-only upstreams are included on purpose: the read uses the dependent's partition against the upstream table."

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/dag -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper/dag/base.py packages/interloper-core/tests/dag/test_base.py
git commit -m "feat(core): require equal partition granularity across upstream edges

By Digitl"
```

---

### Task 9: Honour `default_destination_key` on read

**Files:**
- Modify: `asset/base.py:626-650` (`partition_row_counts`), `asset/base.py:790-814` (`_destination_read`), new `_read_destination`
- Test: `tests/asset/test_base.py`

**Interfaces:**
- Produces: `Asset._read_destination(self) -> Destination` (the destination downstream readers use).

- [ ] **Step 1: Write the failing test**

```python
class TestReadDestination:
    def test_default_destination_key_selects_the_read_destination(self):
        first, second = il.MemoryDestination(), il.CSVDestination(base_path="/tmp/unused")
        asset = FakeAsset(destinations=[first, second], default_destination_key=second.key)
        assert asset._read_destination() is second

    def test_falls_back_to_the_first_destination(self):
        first, second = il.MemoryDestination(), il.CSVDestination(base_path="/tmp/unused")
        asset = FakeAsset(destinations=[first, second])
        assert asset._read_destination() is first

    def test_raises_without_destinations(self):
        with pytest.raises(AssetError, match="No destination found"):
            FakeAsset()._read_destination()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset/test_base.py -q -k ReadDestination`
Expected: FAIL with `AttributeError`.

- [ ] **Step 3: Implement**

Add after `_resolve_destinations`:

```python
    def _read_destination(self) -> Destination:
        """The destination downstream readers load this asset from.

        The destination whose key equals ``default_destination_key`` when one
        is configured and present, else the first resolved destination.

        Returns:
            The destination to read from.

        Raises:
            AssetError: If the asset resolves no destination at all.
        """
        destinations = self._resolve_destinations()
        if not destinations:
            raise AssetError(f"No destination found for upstream asset '{self.key}'")
        preferred = next((d for d in destinations if d.key == self.default_destination_key), None)
        return preferred or destinations[0]
```

In `_destination_read` replace

```python
        destinations = upstream_asset._resolve_destinations()
        if not destinations:
            raise AssetError(f"No destination found for upstream asset '{upstream_asset.key}'")
        destination = destinations[0]
```

with `destination = upstream_asset._read_destination()`, and in `partition_row_counts` replace the destination lookup and `destinations[0]` with `self._read_destination()` (keep the `PartitionError` check first). Update `docs/guide/assets.md:126` (the `default_destination_key` row) to "With several destinations, the one downstream readers load from."

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-core/src/interloper/asset/base.py packages/interloper-core/tests/asset/test_base.py docs/guide/assets.md
git commit -m "fix(core): read upstream data from the configured default destination

By Digitl"
```

---

### Task 10: Documentation and the plugin skills

**Files:**
- Modify: `docs/guide/dependencies.md`, `docs/guide/assets.md:76-130`, `docs/guide/specs.md`, `docs/extending/components.md` (relations table), `docs/extending/operations.md:41-46`, `docs/reference/decorators.md:19-20`
- Modify: `plugins/interloper/skills/interloper-manifest/SKILL.md`, `plugins/interloper/skills/interloper-source/SKILL.md:122-123`

- [ ] **Step 1: Rewrite `docs/guide/dependencies.md`**

Keep the title "Dependencies" (the concept). In "Inside a source", the sentence at lines 33-34 becomes: "Inference records the contract on the asset class as `depends_on` (`{"users": "shop.users"}`); a `None` default records `il.Dependency(key="shop.segments", optional=True)`."

Replace the "Explicit contracts" example and the paragraph beginning "A cross-source contract is checked, not wired" with:

```markdown
## Explicit contracts

When the parameter name does not match, or the upstream lives in another source, declare the
upstream on the decorator. A plain string is a single, non-optional upstream on that key;
`il.Dependency` adds the optional and many-valued cases:

```py
@il.source
class Finance(il.Source):
    @il.asset(depends_on={"orders": "shop.orders", "fx": il.Dependency(key="rates.daily_fx", optional=True)})
    def revenue(self, orders: list[dict], fx: list[dict] | None = None) -> list[dict]:
        ...
```

Keys are bare (`orders`, a sibling), qualified (`shop.orders`, that source type, any instance) or
wildcard (`*.orders`, that asset key from any source). `depends_on` is the contract; the wiring that
satisfies it lives in `upstreams` on the instance (parameter name to a list of upstream ids), and
the platform stores it as `upstream` relations.

The DAG wires a qualified key when exactly one asset in the DAG satisfies it:

```py
dag = il.DAG(Shop(...), Finance(...))     # revenue.upstreams == {"orders": [shop.orders.id]}
```

Two matching assets (two `Shop` instances) raise `DAGError` and ask for explicit wiring by id;
none raise `DependencyNotFoundError` at build time, never a `TypeError` inside `data()`. A `data()`
parameter that is neither the context, a resource nor a declared upstream, and has no default, is
an `AssetError` at build time for the same reason.
```

Add a section after it:

```markdown
## Many upstreams

A slot can bind every asset matching a key. Declare it with `many=True`; `data()` receives a list
of `il.Upstream`, each carrying the upstream asset and its data:

```py
@il.asset(
    depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
    partitioning=il.TimePartitionConfig(column="date"),
)
def campaign_matches(context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]:
    return [
        {"date": context.partition_date, "provider": leg.asset.source.key, "rows": len(leg.data)}
        for leg in campaigns
        if leg.data is not None
    ]
```

The DAG binds every match it holds, and explicit wiring (`upstreams["campaigns"] = [id1, id2]`, or
a list in a spec) is kept as is. `optional=True` allows an empty list. A bound leg with no data for
the run's partition arrives with `data` set to `None` and a warning event names it; the asset
decides what a missing leg means. Any other read error fails the asset. The same rule holds for a
single slot: an optional one receives `None` when its upstream has no data for the partition.
```

Update the rules table:

| Rule | Error |
|------|-------|
| Every operation id is unique | `DAGError` |
| A declared single slot matches at most one asset in the DAG | `DAGError` |
| Every non-default `data()` parameter is the context, a resource or a declared upstream | `AssetError` |
| Non-optional slots are wired to a node in the DAG | `DependencyNotFoundError` |
| Wired upstreams satisfy the declared key | `DependencyContractError` |
| No cycles | `CircularDependencyError` |
| A non-partitioned asset never depends on a partitioned one | `DAGError` |
| Time-partitioned ends of an edge share a granularity | `DAGError` |

and add under it: "A run has one partition scope, so both ends of an edge must agree on granularity, read-only upstreams included."

In "How wiring works" the field is `upstreams`, "a mapping from parameter name to the upstream assets' **instance ids**" (a list, one entry for a single slot). In "Reading upstream data" replace "first resolved destination" with "the destination named by its `default_destination_key`, or its first destination".

- [ ] **Step 2: Update the other docs**

`docs/guide/assets.md`: lines 76-78 become "**Dependencies**: any other parameter is an upstream asset. Inside a source, a parameter named after a sibling asset is wired automatically; `depends_on` declares the rest, with `il.Dependency` for optional or many-valued slots. See [Dependencies](dependencies.md)." Lines 106-107 of the decorator example become `depends_on={"campaigns": "ads.campaigns", "budget": il.Dependency(key="finance.budget", optional=True)},  # upstream assets`. In the fields table the `dependencies` row becomes `upstreams`: "Parameter name to the upstream asset **ids**, always a list. Filled by the source and the DAG; can be set by hand."

`docs/guide/specs.md`: the source-spec paragraph names `upstreams` instead of "dependency wiring", and the example map shows `"upstreams": {...}`.

`docs/extending/components.md`: the `slots` row of the `RelationDefinition` table was rewritten in Task 2; in the same section replace "dependency slots from `requires`" with "upstream slots from `depends_on`" and the `inline` row's example "(asset dependencies)" with "(asset upstreams)".

`docs/extending/operations.md:41-46`: the `dependencies` row becomes `upstreams` ("parameter name to upstream ids"); replace the `optional_requires` row with `| `declared_upstreams()` | `{}` | `depends_on` as `Dependency` objects |`; the `validate_dependencies(nodes)` row becomes `validate_upstreams(nodes)` with "checks the signature, cardinality and identities".

`docs/reference/decorators.md:19-20`: replace the `requires` and `optional_requires` rows with one row "`depends_on` | `dict[str, str \| Dependency]` | class | Upstream assets, parameter to key; `il.Dependency` for optional or many-valued slots."

- [ ] **Step 3: Update the plugin skills**

`plugins/interloper/skills/interloper-source/SKILL.md:122-123`: the cross-source row becomes "`depends_on={"param": "other_source.asset"}` declares the contract; the DAG wires it when one match is present, otherwise wire by id"; the optional row becomes "parameter default `None`, or `depends_on={"param": il.Dependency(key="key", optional=True)}`"; add a row "Fan in over every matching asset | `depends_on={"param": il.Dependency(key="*.asset_key", many=True)}`, `data()` receives `list[il.Upstream]`".

`plugins/interloper/skills/interloper-manifest/SKILL.md`:

- Recipe YAML: the downstream override becomes `upstreams: {orders: [shop-orders]}` with the comment "explicit wiring; only needed when several candidates exist", and a comment above `targets:` says "a qualified `depends_on` key wires itself when exactly one match is in the job".
- Replace the bullet **`requires` does not wire.** with: **`depends_on` wires itself when unambiguous.** A qualified key (`shop.orders`) binds the single matching asset in the job; two matches (two `shop` instances) fail at load time with `DAGError`, and an unbound non-optional key fails at load time with `DependencyNotFoundError`. Wire by id (an `id` on the upstream, `upstreams:` on the downstream) only in the ambiguous case. A many-valued slot (`il.Dependency(key="*.campaigns", many=True)`) takes a list: `upstreams: {campaigns: [id-a, id-b]}`.
- In "Common mistakes" replace the `TypeError` bullet with: "Reading `DependencyNotFoundError: ... nothing is wired in the DAG` as a bug: the job lacks the upstream source, or two instances match and need explicit wiring."
- Keep the dry-run advice but drop the sentence about the `TypeError` symptom. Rename `dependencies` to `upstreams` wherever the skill shows the field.

- [ ] **Step 4: Build the docs and scan**

Run: `uv run --frozen zensical build 2>&1 | tail -5` (or the docs build command used by `.github/workflows` for the docs site; check `docs/` README). Then `grep -rn "optional_requires\|requires=\|RelationSlot\|dependencies:" docs plugins` must print nothing (`dependencies` as a word in prose is fine), and `grep -nP "\x{2014}"` over the touched files must print nothing for the lines you touched.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add docs/guide/dependencies.md docs/guide/assets.md docs/guide/specs.md docs/extending/components.md docs/extending/operations.md docs/reference/decorators.md plugins/interloper/skills/interloper-manifest/SKILL.md plugins/interloper/skills/interloper-source/SKILL.md
git commit -m "docs: document depends_on, il.Dependency, upstreams and DAG resolution of declared keys

By Digitl"
```

---

### Task 11: Full check, migration round trip and probe

- [ ] **Step 1: Run the check suites**

Run: `make check-python` and, from `packages/interloper-app/app/`, `pnpm run lint && pnpm exec nuxt typecheck`.
Expected: ruff clean, ty clean, pytest green across the workspace, frontend clean.

- [ ] **Step 2: Migration round trip on a throwaway database**

Never against the shared dev database. Use a scratch database name:

```bash
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db upgrade
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db downgrade 016
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db upgrade
```

(Check the exact subcommand flags with `uv run --frozen interloper db --help`.) Seed one `upstream` row before the downgrade to see it become `dependency` and back. Expected: no errors; `\d component_relations` after upgrade shows the unique index `WHERE type = 'resource'`.

- [ ] **Step 3: Probe the original failure from the scratchpad**

```python
import datetime as dt
import interloper as il

mem = il.MemoryDestination()

@il.source
class Shop(il.Source):
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "amount": 10}]

@il.source
class Finance(il.Source):
    @il.asset(depends_on={"orders": "shop.orders"}, partitioning=il.TimePartitionConfig(column="date"))
    def revenue(self, context: il.ExecutionContext, orders: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "revenue": sum(o["amount"] for o in orders)}]

dag = il.DAG(Shop(destinations=[mem]), Finance(destinations=[mem]))
print([[o.qualified_key for o in g] for g in dag.topological_generations()])
print(dag.materialize(il.TimePartition(dt.date(2026, 1, 1))).status)
```

Run with `uv run --frozen python <file>` from the scratchpad. Expected: two generations (`shop.orders` then `finance.revenue`) and `ExecutionStatus.COMPLETED`.

- [ ] **Step 4: Open the PR**

Title `feat(core)!: upstream relation, depends_on and il.Dependency, many-valued upstreams and DAG resolution`. Body: link the spec, list the behaviour changes under a "Breaking" heading (relation type `dependency` renamed `upstream` with migration 017, `dependencies` renamed `upstreams` and always a list, `requires` and `optional_requires` replaced by `depends_on`, `RelationSlot` renamed `Dependency` with `optional` replacing `required` in the catalog JSON, build-time failure for unbound slots and undeclared parameters, optional single slots no longer swallow non-missing-data errors, equal-granularity rule, default destination on read), end with `By Digitl`.
