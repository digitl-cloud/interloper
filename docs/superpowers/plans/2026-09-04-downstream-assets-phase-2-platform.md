# Many-valued upstreams, phase 2 (platform) Implementation Plan

> **SUPERSEDED 2026-09-07** by `2026-09-07-relation-model-phase-*.md` (design `2026-09-07-relation-model-design.md`). Kept for the record; phase 1 here was executed as PR #321 and is being reworked.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the platform persist, validate, hydrate and execute many-valued dependency slots and wildcard keys, and expose the contract to the toolkit and the agent.

**Architecture:** The store stays the authority on relation shape: single-slot repointing moves fully into `RelationStore` (phase 1's migration 017 already narrowed the unique index to `resource`), and `_check_slot_target` learns the wildcard. Hydration emits one id list per upstream slot. The toolkit and the agent read the slot contract from the catalog definitions.

**Tech Stack:** SQLModel, Alembic, FastAPI, Google ADK agent tools, pytest. Requires phase 1 merged (the `upstream` relation and `upstreams` list field, `depends_on`, `il.Dependency` with `optional` and `many`, `declared_upstreams()`, migration 017). Spec: `docs/superpowers/specs/2026-09-04-downstream-assets-design.md` section 2.5.

## Global Constraints

- Branch `feat/many-upstreams-platform` from `main` after phase 1 merged; rebase only.
- Conventional Commits ending with `By Digitl`; commit only when asked.
- `uv run --frozen ...` from the repo root; ruff 120; `ty` clean; full Google docstrings.
- Tests mirror modules: `packages/interloper-db/tests/store/test_relations.py`, `test_hydration.py`; `packages/interloper-scheduler/tests/test_executor.py`; `packages/interloper-toolkit/tests/test_toolkit.py`.
- No migration in this phase: 017 (relation rename and index narrowing) shipped with phase 1.
- No em-dashes anywhere.

---

### Task 1: Fixtures and the failing fan-in test

Phase 1's migration 017 already narrowed `uq_component_relations_slot` to `resource`; the store still repoints every slotted relation, which this test pins as the gap Task 2 closes.

**Files:**
- Test: `packages/interloper-db/tests/store/test_relations.py`

- [ ] **Step 1: Write the failing test**

Add fixtures at module level in `test_relations.py` next to `WireUpSource` / `WireDownSource`:

```python
class FanInProviderOne(il.Source):
    """Provider one."""

    class Campaigns(il.Asset):
        """Campaigns."""


class FanInProviderTwo(il.Source):
    """Provider two."""

    class Campaigns(il.Asset):
        """Campaigns."""


class FanInSource(il.Source):
    """Source owning a many-valued slot over every ``campaigns`` asset."""

    class Matches(il.Asset):
        """Fan-in asset."""

        depends_on: ClassVar[dict[str, Any]] = {"campaigns": il.Dependency(key="*.campaigns", many=True)}

        def data(self, campaigns: list[il.Upstream]) -> Any:  # pragma: no cover
            return None
```

Add a test class:

```python
class TestManyValuedSlots:
    @pytest.fixture
    def fan_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([FanInProviderOne, FanInProviderTwo, FanInSource, WireUpSource]))

    def test_many_slot_accumulates_legs(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        two = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_two")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        matches = _child(fan, "matches")

        fan_store.relations.add(matches.id, type="upstream", dst_id=_child(one, "campaigns").id, slot="campaigns")
        fan_store.relations.add(matches.id, type="upstream", dst_id=_child(two, "campaigns").id, slot="campaigns")

        edges = fan_store.relations.list_all(_ORG, type="upstream")
        assert {edge.dst_id for edge in edges} == {_child(one, "campaigns").id, _child(two, "campaigns").id}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_relations.py -q -k accumulates`
Expected: FAIL with one edge (the second add repointed the first). Task 2 makes it pass; stage the test with Task 2.

---

### Task 2: Store rules for many slots and the wildcard

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/relations.py` (`add`, `remove`, `_check_slot_target`, `_upsert_relation` call sites)
- Test: `packages/interloper-db/tests/store/test_relations.py`

**Interfaces:**
- Consumes: `il.Dependency.many`, `interloper.asset.base.ANY_SOURCE`.
- Behaviour: many slots accumulate; single slots repoint; wildcard keys accept any parent or none; removing one leg of a required many slot is allowed while another leg stays.
- Behaviour: `add` locks the referrer row (`with_for_update()`) for the duration of the bind, so two concurrent binds of one single-valued slot queue instead of racing; phase 1's migration removed the unique index that used to catch that race (ruling recorded in the phase 1 ledger).

- [ ] **Step 1: Add the failing tests**

Append to `TestManyValuedSlots`:

```python
    def test_wildcard_accepts_a_standalone_asset(self, component_db: Engine):
        class LooseCampaigns(il.Asset):
            """Standalone asset keyed ``campaigns``."""

            key: ClassVar[str] = "campaigns"

        store = Store(catalog=il.Catalog.from_assets([LooseCampaigns, FanInSource]))
        loose = store.components.create(_ORG, kind="asset", key="campaigns")
        fan = store.components.create(_ORG, kind="source", key="fan_in_source")
        relation = store.relations.add(_child(fan, "matches").id, type="upstream", dst_id=loose.id, slot="campaigns")
        assert relation.dst_id == loose.id

    def test_wildcard_rejects_a_different_asset_key(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        with pytest.raises(ConfigError, match="expects asset '\\*.campaigns'"):
            fan_store.relations.add(
                _child(fan, "matches").id, type="upstream", dst_id=_child(fan, "matches").id, slot="campaigns"
            )

    def test_removing_one_leg_keeps_the_slot_bound(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        two = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_two")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        matches = _child(fan, "matches")
        fan_store.relations.add(matches.id, type="upstream", dst_id=_child(one, "campaigns").id, slot="campaigns")
        fan_store.relations.add(matches.id, type="upstream", dst_id=_child(two, "campaigns").id, slot="campaigns")

        fan_store.relations.remove(matches.id, type="upstream", dst_id=_child(one, "campaigns").id)

        (edge,) = fan_store.relations.list_all(_ORG, type="upstream")
        assert edge.dst_id == _child(two, "campaigns").id

    def test_removing_the_last_leg_of_a_required_slot_is_blocked(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        matches = _child(fan, "matches")
        fan_store.relations.add(matches.id, type="upstream", dst_id=_child(one, "campaigns").id, slot="campaigns")
        with pytest.raises(ConfigError, match="cannot be unbound"):
            fan_store.relations.remove(matches.id, type="upstream", dst_id=_child(one, "campaigns").id)
```

Note `test_wildcard_rejects_a_different_asset_key` binds the matcher to itself on purpose; the self-edge check fires first with "itself". Change the target to `_child(one, "campaigns")` is the accepted case, so instead create a second source with an asset keyed differently: reuse `WireUpSource` (`rows`) in the fixture catalog and bind `_child(up, "rows")`; the expected message is `expects asset '*.campaigns', got 'rows'`. Adjust the fixture to include `WireUpSource`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_relations.py -q -k ManyValuedSlots`
Expected: FAIL (wildcard rejected with "expects an asset of source '*'", last-leg removal not blocked or one-leg removal blocked).

- [ ] **Step 3: Implement in `relations.py`**

Import `from interloper.asset.base import ANY_SOURCE, AssetIdentity`.

In `add`, load the referrer with a row lock so concurrent binds of the same slot serialise (the unique index no longer covers upstream rows):

```python
            src = session.exec(select(Component).where(Component.id == component_id).with_for_update()).one_or_none()
            if not src:
                raise NotFoundError(f"Component {component_id} not found")
```

(SQLite ignores `FOR UPDATE`; the store tests still pass, the guarantee holds on Postgres.) Then replace the upsert call:

```python
            slot_def = definition.slots.get(slot) if definition.slotted else None
            per_slot = definition.slotted and not (slot_def is not None and slot_def.many)
            relation = self._upsert_relation(session, src, dst, type, slot, per_slot=per_slot)
```

and extend the docstring: "Many-valued slots accumulate: each add of a new destination is another leg."

In `_check_slot_target`, after the `dst.key != expected.asset_key` check:

```python
        if expected.source_key == ANY_SOURCE:
            return
```

and update its docstring with: "a wildcard key (``*.asset``) accepts that asset key from any source instance, standalone assets included."

In `remove`, replace the blocked computation:

```python
            src = session.get(Component, component_id) if relations else None
            if src is not None:
                definition = self._relation_vocabulary(session, src).get(type)
                removed_slots = {relation.slot for relation in relations}
                still_bound = {
                    relation.slot
                    for relation in session.exec(
                        select(ComponentRelation).where(
                            ComponentRelation.src_id == component_id,
                            ComponentRelation.type == type,
                            ComponentRelation.dst_id != dst_id,
                            ComponentRelation.slot.in_(removed_slots),  # ty: ignore[unresolved-attribute]
                        )
                    ).all()
                }
                if blocked := self._blocked_unbinds(definition, (slot for slot in removed_slots if slot not in still_bound)):
                    raise ConfigError(
                        f"Required '{type}' slot(s) {blocked} of '{src.key}' cannot be unbound; "
                        f"repoint them or remove the dependent asset instead"
                    )
```

Docstring addition for `remove`: "A slot that keeps another leg after the removal is still bound and never blocks."

`_sync_relations` needs no code change: with the index narrowed, several bindings per slot insert fine, and `kept` already keeps a slot bound while any binding for it remains.

- [ ] **Step 4: Run the db suite**

Run: `uv run --frozen pytest packages/interloper-db -q`
Expected: PASS, including `test_rebinding_a_slot_repoints_it` (single slots still repoint).

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-db/src/interloper_db/store/relations.py packages/interloper-db/tests/store/test_relations.py
git commit -m "feat(db): accumulate legs on many-valued upstream slots and accept wildcard keys

By Digitl"
```

---

### Task 3: Hydration emits a list per upstream slot

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/hydration.py:146-160`
- Test: `packages/interloper-db/tests/store/test_hydration.py`

- [ ] **Step 1: Write the failing test**

Add to `test_hydration.py` (fixtures can import the `FanIn*` classes from `tests/store/test_relations.py` or redefine them; redefine to keep the test module self-contained):

```python
class TestManyValuedHydration:
    @pytest.fixture
    def fan_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([FanInProviderOne, FanInProviderTwo, FanInSource]))

    def test_two_legs_hydrate_as_a_list(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        two = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_two")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        matches = next(child for child in fan.children if child.key == "matches")
        legs = [next(c for c in s.children if c.key == "campaigns").id for s in (one, two)]
        for leg in legs:
            fan_store.relations.add(matches.id, type="upstream", dst_id=leg, slot="campaigns")

        live = fan_store.components.load(fan.id)
        assert sorted(live.matches.upstreams["campaigns"]) == sorted(str(leg) for leg in legs)

    def test_one_leg_hydrates_as_a_one_element_list(self, fan_store: Store):
        one = fan_store.components.create(_ORG, kind="source", key="fan_in_provider_one")
        fan = fan_store.components.create(_ORG, kind="source", key="fan_in_source")
        matches = next(child for child in fan.children if child.key == "matches")
        leg = next(c for c in one.children if c.key == "campaigns").id
        fan_store.relations.add(matches.id, type="upstream", dst_id=leg, slot="campaigns")

        live = fan_store.components.load(fan.id)
        assert live.matches.upstreams["campaigns"] == [str(leg)]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_hydration.py -q -k ManyValued`
Expected: both FAIL (the dict comprehension keeps one string per slot).

- [ ] **Step 3: Implement**

Replace the slotted branch in `_build_init`:

```python
            if definition.slotted and not definition.inline:
                # Id-carrying slots (upstreams) are always lists, one per slot.
                per_slot: dict[str, list[Any]] = {}
                for rel, value in zip(rels, values):
                    per_slot.setdefault(rel.slot, []).append(value)
                init[definition.field] = per_slot
            elif definition.slotted:
                init[definition.field] = {rel.slot: value for rel, value in zip(rels, values)}
            else:
                init[definition.field] = values
```

- [ ] **Step 4: Run the db suite**

Run: `uv run --frozen pytest packages/interloper-db -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-db/src/interloper_db/store/hydration.py packages/interloper-db/tests/store/test_hydration.py
git commit -m "feat(db): hydrate upstream slots as id lists

By Digitl"
```

---

### Task 4: Executor joins every leg (test)

Phase 1 already made `_resolve_upstream` walk the `upstreams` lists; this task pins it.

**Files:**
- Test: `packages/interloper-scheduler/tests/test_executor.py`

- [ ] **Step 1: Write the test**

```python
def test_resolve_upstream_joins_every_leg_of_a_many_slot() -> None:
    class Leg(il.Asset):
        """Leg asset."""

    class FanIn(il.Asset):
        """Fan-in asset."""

        depends_on: ClassVar[dict[str, Any]] = {"legs": il.Dependency(key="*.leg", many=True)}

        def data(self, legs: list[il.Upstream]) -> Any:  # pragma: no cover
            return None

    leg_a, leg_b = Leg(id=str(uuid4())), Leg(id=str(uuid4()))
    fan = FanIn(id=str(uuid4()), upstreams={"legs": [leg_a.id, leg_b.id]})
    loaded = {UUID(leg_a.id): leg_a, UUID(leg_b.id): leg_b}
    store = SimpleNamespace(components=SimpleNamespace(load=lambda component_id: loaded[component_id]))

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    operations: list[il.Operation] = [fan]
    executor._resolve_upstream(operations)

    assert [operation.id for operation in operations] == [fan.id, leg_a.id, leg_b.id]
    assert all(not operation.materializable for operation in operations[1:])
```

Add `import interloper as il`, `from typing import Any, ClassVar` to the test module imports if missing.

- [ ] **Step 2: Run the test**

Run: `uv run --frozen pytest packages/interloper-scheduler/tests/test_executor.py -q -k many_slot`
Expected: PASS.

- [ ] **Step 3: Stage (commit only if asked)**

```bash
git add packages/interloper-scheduler/tests/test_executor.py
git commit -m "test(scheduler): pin that every leg of a many-valued slot is joined

By Digitl"
```

---

### Task 5: Toolkit reads the slot contract

**Files:**
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/catalog.py:147-178` (`get_asset_schema`)
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/models.py:89-100` (`AssetSchemaResult`)
- Test: `packages/interloper-toolkit/tests/test_toolkit.py`

**Interfaces:**
- Produces: `AssetSchemaResult.depends_on: dict[str, dict[str, Any]]` (parameter to `key`, `optional`, `many`), replacing the `requires` and `optional_requires` fields.
- Produces: module function `declared_upstreams_of(asset_def: dict[str, Any]) -> dict[str, dict[str, Any]]` in `catalog.py`.

- [ ] **Step 1: Write the failing test**

Find how `test_toolkit.py` builds a `ToolkitContext` (search for `ToolkitContext(` and reuse that helper; the catalog is `il.Catalog.from_assets([...]).dump()`). Add:

```python
class FanInSource(il.Source):
    """Source owning a many-valued slot."""

    class Matches(il.Asset):
        """Fan-in asset."""

        depends_on: ClassVar[dict[str, Any]] = {
            "campaigns": il.Dependency(key="*.campaigns", many=True),
            "rules": il.Dependency(key="rules", optional=True),
        }

        def data(self, campaigns: list[il.Upstream], rules: Any = None) -> Any:  # pragma: no cover
            return None


def test_get_asset_schema_reports_slots_and_contracts(toolkit_ctx_factory):
    ctx = toolkit_ctx_factory(catalog=il.Catalog.from_assets([FanInSource]).dump())
    result = catalog.get_asset_schema(ctx, "fan_in_source", "matches")
    assert isinstance(result, AssetSchemaResult)
    assert result.depends_on == {
        "campaigns": {"key": "*.campaigns", "optional": False, "many": True},
        "rules": {"key": "rules", "optional": True, "many": False},
    }
```

Replace `toolkit_ctx_factory` with whatever fixture or helper the module already uses to build a context with a custom catalog (write one if none exists: a `ToolkitContext(store=SimpleNamespace(), catalog=..., org_id=uuid4())`).

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-toolkit/tests/test_toolkit.py -q -k slots_and_contracts`
Expected: FAIL (`AssetSchemaResult` has no `depends_on`).

- [ ] **Step 3: Implement**

`catalog.py`, add a module-level helper above `get_asset_schema`:

```python
def declared_upstreams_of(asset_def: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The dependencies a dumped asset definition declares.

    The contract travels only as ``relations.upstream.slots``; the dumped
    definition has no ``depends_on`` key.

    Args:
        asset_def: One dumped asset definition.

    Returns:
        Parameter name to dependency fields (``key``, ``optional``, ``many``).
    """
    return asset_def.get("relations", {}).get("upstream", {}).get("slots", {}) or {}
```

In `get_asset_schema` replace the two `requires=` / `optional_requires=` lines with one `depends_on=`:

```python
                return AssetSchemaResult(
                    source_key=source_key,
                    asset_key=asset_key,
                    qualified_key=f"{source_key}.{asset_key}",
                    asset_schema=asset_def.get("asset_schema"),
                    partitioning=asset_def.get("partitioning"),
                    tags=asset_def.get("tags", []),
                    depends_on=declared_upstreams_of(asset_def),
                )
```

`models.py`: on `AssetSchemaResult` replace the `requires` and `optional_requires` fields with `depends_on: dict[str, dict[str, Any]] = {}` and note in the class docstring that it carries the full dependency contract (`key`, `optional`, `many`).

- [ ] **Step 4: Run the toolkit suite**

Run: `uv run --frozen pytest packages/interloper-toolkit -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-toolkit/src/interloper_toolkit/catalog.py packages/interloper-toolkit/src/interloper_toolkit/models.py packages/interloper-toolkit/tests/test_toolkit.py
git commit -m "fix(toolkit)!: report depends_on from the relation slots instead of the always-empty requires

By Digitl"
```

---

### Task 6: Agent `bind_upstream` tool and slot-based unresolved report

**Files:**
- Modify: `packages/interloper-agent/src/interloper_agent/tools/collection.py:546-558` (`_unresolved_requirements`), new `bind_upstream`
- Modify: `packages/interloper-agent/src/interloper_agent/agent.py:174` (tool registration next to `scheduling.toggle_asset`)
- Test: `packages/interloper-agent/tests/tools/test_collection.py` (mirror of `tools/collection.py`; create if missing)

**Interfaces:**
- Produces: `bind_upstream(asset_id: str, upstream_asset_id: str, slot: str, tool_context: ToolContext | None = None) -> dict[str, Any]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_unresolved_requirements_reads_slots():
    defn = {
        "assets": [
            {
                "key": "matches",
                "relations": {"upstream": {"slots": {"campaigns": {"key": "*.campaigns", "optional": False, "many": True}}}},
            },
            {"key": "campaigns", "relations": {"upstream": {"slots": {"sibling": {"key": "other", "optional": False}}}}},
        ]
    }
    row = SimpleNamespace(children=[SimpleNamespace(key="matches"), SimpleNamespace(key="campaigns")])
    assert collection._unresolved_requirements(defn, row) == ["matches: campaigns"]


def test_bind_upstream_calls_the_relation_store(monkeypatch):
    calls = []
    fake_store = SimpleNamespace(
        relations=SimpleNamespace(add=lambda cid, **kw: calls.append((cid, kw)) or SimpleNamespace(slot=kw["slot"])),
        components=SimpleNamespace(get=lambda cid, kind: SimpleNamespace(key="matches")),
    )
    monkeypatch.setattr(collection, "get_store", lambda: fake_store)
    asset_id, upstream_id = str(uuid4()), str(uuid4())
    result = collection.bind_upstream(asset_id, upstream_id, "campaigns")
    assert result["status"] == "success"
    assert calls == [(UUID(asset_id), {"type": "upstream", "dst_id": UUID(upstream_id), "slot": "campaigns"})]
```

Check how other tests in `packages/interloper-agent/tests` patch `get_store` and mirror it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-agent/tests/tools/test_collection.py -q`
Expected: FAIL (`_unresolved_requirements` returns `[]`; `bind_upstream` missing).

- [ ] **Step 3: Implement**

`collection.py`, rewrite `_unresolved_requirements`:

```python
def _unresolved_requirements(defn: dict[str, Any], row: Any) -> list[str]:
    """Dependency slots of the enabled assets that reach outside the source.

    Bare keys are wired by the platform; qualified and wildcard keys are
    reported so the user binds them in the app or with ``bind_upstream``.

    Args:
        defn: The dumped source definition.
        row: The created source row, whose ``children`` are the enabled assets.

    Returns:
        One ``"asset: params"`` line per affected asset.
    """
    enabled = {a.key for a in row.children}
    lines = []
    for asset in defn.get("assets", []):
        if asset.get("key") not in enabled:
            continue
        slots = asset.get("relations", {}).get("upstream", {}).get("slots", {}) or {}
        outward = sorted(name for name, slot in slots.items() if "." in slot.get("key", ""))
        if outward:
            lines.append(f"{asset['key']}: {', '.join(outward)}")
    return sorted(lines)
```

Add the tool next to `toggle_asset`'s style:

```python
def bind_upstream(
    asset_id: str,
    upstream_asset_id: str,
    slot: str,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Bind an upstream asset into one of an asset's dependency slots.

    A many-valued slot gains another leg; a single slot is repointed.

    Args:
        asset_id: UUID of the downstream asset.
        upstream_asset_id: UUID of the upstream asset to bind.
        slot: The dependency slot (the downstream's ``data()`` parameter name).
    """
    try:
        store = get_store()
        relation = store.relations.add(
            UUID(asset_id), type="upstream", dst_id=UUID(upstream_asset_id), slot=slot
        )
        asset = store.components.get(UUID(asset_id), kind="asset")
        return {
            "status": "success",
            "message": f"Bound upstream into slot '{relation.slot}' of asset '{asset.key}'",
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
```

`agent.py`: add `collection.bind_upstream,` to the tool list next to `scheduling.toggle_asset` (line 174), and mention it in `prompts.py` where the setup guidance lists tools for wiring sources (search for `unresolved_requirements` or `wire` in `prompts.py`; add one sentence: "Use `bind_upstream` to bind an upstream asset into a dependency slot the user has confirmed.").

- [ ] **Step 4: Run the agent suite**

Run: `uv run --frozen pytest packages/interloper-agent -q`
Expected: PASS.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-agent/src/interloper_agent/tools/collection.py packages/interloper-agent/src/interloper_agent/agent.py packages/interloper-agent/src/interloper_agent/prompts.py packages/interloper-agent/tests/tools/test_collection.py
git commit -m "feat(agent): bind upstream slots and report outward requirements from the slot contract

By Digitl"
```

---

### Task 7: Full check and API smoke

- [ ] **Step 1: `make check-python`** must pass.

- [ ] **Step 2: API smoke on a dev instance on a non-3000 port**

`INTERLOPER_SERVER_PORT=3100 make dev-up`, then with the session cookie from a `:3000` login: `POST /components/{matcher_asset_id}/relations` twice with two different `dst_id` and the same `slot`, `GET /components/relations?type=upstream` shows both; `DELETE /components/{id}/relations/upstream/{dst}` removes one and keeps the other. This needs a many-slot asset in the catalog; if phase 3 is not merged yet, verify with the `FanInSource` fixtures through the store tests only and record that in the PR.

- [ ] **Step 3: Open the PR**

Title `feat(platform)!: persist and expose many-valued upstream slots`. Body links the spec, notes the migration and the `depends_on` field replacing `requires` / `optional_requires` on the toolkit result, ends with `By Digitl`.
