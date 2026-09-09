# Relation model, phase 2 (platform) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The platform stores, hydrates and exposes relations by `name`: rows become `(src_id, name, dst_id)`, `RelationStore` applies `Relation.accepts` on identities built from rows, the hydrator emits the same nested-or-reference shape as `to_spec()`, the API and toolkit speak `name`, the agent binds by name, and the scheduler stops hand-rolling read-only upstreams.

**Architecture:** One migration rewrites revision 017 in place. `RelationStore` and `ComponentStore` call the core classmethods (`Relation.accepts`, `Source.sibling_bindings`) rather than reimplementing them. `Hydrator._build_init` decides nested versus `{"ref": id}` by `parent_id` and hands `Component.from_spec` a `resolve=store.load` callable. Spec: `docs/superpowers/specs/2026-09-07-relation-model-design.md`, sections 8 and 9.

**Tech Stack:** SQLModel, Alembic, FastAPI, pytest with the in-memory `component_db` fixture, plus one round trip on a throwaway Postgres database.

## Global Constraints

- Stacked PR: branch `feat/relation-model-platform` from `feat/many-upstreams-core` (phase 1, PR #321) once phase 1's final review is clean; the PR's base is `feat/many-upstreams-core`, retargeted to `main` when #321 merges. Rebase only, `--force-with-lease`. Commits use `--no-verify` until the repo-wide pre-commit pytest is green again (this phase makes it green).
- Conventional Commits ending `By Digitl`; the row rename is breaking: `feat!:`. Commit only when Guillaume has asked.
- `uv run --frozen` everywhere; ruff 120; ty clean; full Google docstrings; tests mirror modules (`packages/interloper-db/tests/store/test_relations.py` for `store/relations.py`).
- Never run migrations or seeds against the shared dev database. Verification uses a throwaway database (`createdb interloper_scratch`), see Task 9.
- No em-dashes anywhere.
- `packages/interloper-db/src/interloper_db/` is the root of every db path below.

---

### Task 1: Rows by `name` and the rewritten migration 017

**Files:**
- Modify: `models/components.py:130-200` (`ComponentRelation`: drop `type`, `slot`; add `name: str` as the second primary-key column; indexes `ix_component_relations_org_id_name (org_id, name)`, `ix_component_relations_dst_id_name (dst_id, name)`; drop `uq_component_relations_slot`)
- Rewrite: `migrations/versions/017_upstream_relation.py` as `migrations/versions/017_relation_name.py` (same `revision = "017"`, `down_revision = "016"`; delete the old file)
- Test: `packages/interloper-db/tests/models/test_components.py` (row shape), `packages/interloper-db/tests/test_migrations.py` if present, else add the round-trip check to Task 9

**Interfaces:**
- Produces: `ComponentRelation(src_id, name, dst_id, org_id, src_kind, dst_kind)`; every consumer in this plan filters on `name`.

- [ ] **Step 1: Write the failing test**

```python
def test_relation_row_is_keyed_by_name(component_db: Engine) -> None:
    with Session(component_db) as session:
        src = Component(org_id=_ORG, kind="source", key="s")
        dst = Component(org_id=_ORG, kind="destination", key="d")
        session.add_all([src, dst]); session.flush()
        session.add(ComponentRelation(src_id=src.id, name="destinations", dst_id=dst.id, org_id=_ORG, src_kind="source", dst_kind="destination"))
        session.commit()
        row = session.exec(select(ComponentRelation)).one()
        assert row.name == "destinations"
        assert not hasattr(row, "slot")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --frozen pytest packages/interloper-db/tests/models -q -k keyed_by_name`
Expected: FAIL (`unexpected keyword 'name'`).

- [ ] **Step 3: Change the model and write the migration**

```python
# migrations/versions/017_relation_name.py
"""Key component relations by name.

A relation row used to carry a ``type`` (the vocabulary entry) and a ``slot``
(empty for list-shaped types). The framework now declares one named
``Relation`` per link, so a row is ``(src_id, name, dst_id)``: ``name`` is the
slot for slotted rows and the plural field name for the others. Whether a
relation is single-valued is a class rule the store enforces, so the partial
unique index on resource slots goes.

Revision ID: 017
Revises: 016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "017"
down_revision: str | None = "016"
branch_labels: str | None = None
depends_on: str | None = None

_TABLE = "component_relations"
_PLURAL = {"destination": "destinations", "target": "targets", "watch": "watches"}


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("name", sa.String(), nullable=True))
    op.execute("UPDATE component_relations SET name = slot WHERE type IN ('resource', 'dependency', 'upstream')")
    for type_, name in _PLURAL.items():
        op.execute(f"UPDATE component_relations SET name = '{name}' WHERE type = '{type_}'")
    op.execute("DELETE FROM component_relations WHERE name IS NULL OR name = ''")
    op.alter_column(_TABLE, "name", nullable=False)
    op.drop_index("uq_component_relations_slot", table_name=_TABLE)
    op.drop_index("ix_component_relations_org_id_type", table_name=_TABLE)
    op.drop_index("ix_component_relations_dst_id_type", table_name=_TABLE)
    op.drop_constraint("component_relations_pkey", _TABLE, type_="primary")
    op.drop_column(_TABLE, "type")
    op.drop_column(_TABLE, "slot")
    op.create_primary_key("component_relations_pkey", _TABLE, ["src_id", "name", "dst_id"])
    op.create_index("ix_component_relations_org_id_name", _TABLE, ["org_id", "name"])
    op.create_index("ix_component_relations_dst_id_name", _TABLE, ["dst_id", "name"])


def downgrade() -> None:
    op.add_column(_TABLE, sa.Column("type", sa.String(), nullable=True))
    op.add_column(_TABLE, sa.Column("slot", sa.String(), nullable=True, server_default=""))
    for type_, name in _PLURAL.items():
        op.execute(f"UPDATE component_relations SET type = '{type_}' WHERE name = '{name}'")
    op.execute("UPDATE component_relations SET type = 'upstream', slot = name WHERE type IS NULL AND dst_kind = 'asset'")
    op.execute("UPDATE component_relations SET type = 'resource', slot = name WHERE type IS NULL")
    op.alter_column(_TABLE, "type", nullable=False)
    op.drop_index("ix_component_relations_org_id_name", table_name=_TABLE)
    op.drop_index("ix_component_relations_dst_id_name", table_name=_TABLE)
    op.drop_constraint("component_relations_pkey", _TABLE, type_="primary")
    op.drop_column(_TABLE, "name")
    op.create_primary_key("component_relations_pkey", _TABLE, ["src_id", "type", "slot", "dst_id"])
    op.create_index("uq_component_relations_slot", _TABLE, ["src_id", "type", "slot"], unique=True, postgresql_where=sa.text("type = 'resource'"))
    op.create_index("ix_component_relations_org_id_type", _TABLE, ["org_id", "type"])
    op.create_index("ix_component_relations_dst_id_type", _TABLE, ["dst_id", "type"])
```

Check the real primary-key constraint name in migration 016 (or `\d component_relations` on the scratch database) and use it. The `DELETE ... name IS NULL` line removes rows of unknown types; production has none (the four types above are the whole vocabulary), state that in the docstring.

- [ ] **Step 4: Run the model tests**

Run: `uv run --frozen pytest packages/interloper-db/tests/models -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-db/src/interloper_db/models/components.py packages/interloper-db/src/interloper_db/migrations/versions
git commit -m "feat!(db): key component relations by name; rewrite migration 017

By Digitl"
```

---

### Task 2: `RelationStore` by name, with the core acceptance rule

**Files:**
- Modify: `store/relations.py` (whole module)
- Test: `packages/interloper-db/tests/store/test_relations.py`

**Interfaces:**
- Produces:
  ```py
  Binding = UUID                                                    # was tuple[UUID, str]
  RelationStore.list_all(org_id, *, name=None, src_kind=None, dst_kind=None) -> list[ComponentRelation]
  RelationStore.add(component_id, *, name, dst_id) -> ComponentRelation
  RelationStore.remove(component_id, *, name, dst_id) -> None
  RelationStore._sync_relations(session, src, bindings: dict[str, list[UUID]] | None) -> None
  RelationStore._relation(session, src) -> dict[str, il.Relation]   # catalog vocabulary by name
  RelationStore._identity(session, row) -> il.ComponentIdentity     # (parent key or None, key)
  ```
- `add`: `SELECT ... FOR UPDATE` on the source row (Postgres; no-op on SQLite), `relation = vocabulary[name]` else `ConfigError`, `relation.accepts(dst.kind, identity(dst), owner=identity(src))` else `ConfigError`, then insert (many) or delete-others-and-insert (single). Re-adding an identical row returns it.
- `remove`: `ConfigError` when the relation is non-optional and the row is its last one.
- `_relation_detaches(row)` (used by the delete guard): `relation.on_delete == "detach"` or `relation.optional`.

- [ ] **Step 1: Rewrite the tests**

Replace the fixtures' asset classes:

```python
class GuardUpstream(il.Asset):
    """Upstream asset for the unbind-guard tests."""

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class GuardRequired(il.Asset):
    """Asset with a required upstream on ``guard_upstream``."""

    up = il.Relation("asset", "guard_upstream")

    def data(self, context: il.ExecutionContext, up: il.Upstream) -> list[dict]:
        return []


class GuardOptional(il.Asset):
    """Asset with an optional upstream on ``guard_upstream``."""

    up = il.Relation("asset", "guard_upstream", optional=True)

    def data(self, context: il.ExecutionContext, up: il.Upstream | None) -> list[dict]:
        return []
```

and the assertions:

```python
def test_add_single_repoints(store, ...) -> None:
    store.relations.add(src.id, name="connection", dst_id=conn_a.id)
    store.relations.add(src.id, name="connection", dst_id=conn_b.id)
    assert [r.dst_id for r in _relations(session, src.id, "connection")] == [conn_b.id]

def test_add_many_accumulates(...) -> None:
    store.relations.add(matcher_asset.id, name="campaigns", dst_id=fb_campaigns.id)
    store.relations.add(matcher_asset.id, name="campaigns", dst_id=tt_campaigns.id)
    assert len(_relations(session, matcher_asset.id, "campaigns")) == 2

def test_add_rejects_wrong_kind(...) -> None:
    with pytest.raises(ConfigError, match="does not accept"):
        store.relations.add(src.id, name="connection", dst_id=destination.id)

def test_add_rejects_unknown_name(...) -> None:
    with pytest.raises(ConfigError, match="declares no relation 'nope'"):
        store.relations.add(src.id, name="nope", dst_id=conn_a.id)

def test_add_checks_declared_key_against_parent(...) -> None:
    # GuardRequired.up accepts guard_upstream only: an asset with another key is refused
    with pytest.raises(ConfigError, match="does not accept"):
        store.relations.add(required.id, name="up", dst_id=other_asset.id)

def test_remove_last_required_is_refused(...) -> None:
    with pytest.raises(ConfigError, match="non-optional"):
        store.relations.remove(required.id, name="up", dst_id=upstream.id)

def test_remove_optional_detaches(...) -> None: ...

def test_list_all_filters_by_kinds(...) -> None:
    rows = store.relations.list_all(_ORG, src_kind="asset", dst_kind="asset")
    assert {r.name for r in rows} == {"up"}
```

Keep the wildcard target test from PR #321's phase 2 plan: `campaigns = il.Relation("asset", "*.campaigns", many=True)` accepts a `campaigns` child of any source.

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_relations.py -q`
Expected: FAIL (`add() got an unexpected keyword argument 'name'`).

- [ ] **Step 3: Rewrite the store**

```python
    def add(self, component_id: UUID, *, name: str, dst_id: UUID) -> ComponentRelation:
        with session_scope(self._engine) as session:
            src = self._lock(session, component_id)
            relation = self._relation(session, src, name)
            dst = session.get(Component, dst_id)
            if dst is None or dst.org_id != src.org_id:
                raise NotFoundError(f"Component {dst_id} not found (relation '{name}')")
            if not relation.accepts(dst.kind, self._identity(session, dst), owner=self._identity(session, src)):
                raise ConfigError(
                    f"'{src.key}'.{name} does not accept {dst.kind} '{dst.key}' "
                    f"(declared: kind {relation.kinds()}, key {relation.keys() or 'any'})"
                )
            existing = self._rows(session, src.id, name)
            if any(row.dst_id == dst.id for row in existing):
                return next(row for row in existing if row.dst_id == dst.id)
            if not relation.many:
                for row in existing:
                    session.delete(row)
            row = ComponentRelation(src_id=src.id, name=name, dst_id=dst.id, org_id=src.org_id, src_kind=src.kind, dst_kind=dst.kind)
            session.add(row)
            commit(session)
            return row
```

`_lock` issues `select(Component).where(Component.id == component_id).with_for_update()` when the dialect is Postgres. `_relation(session, src, name)` reads `self._catalog.vocabulary(src.kind, src.key, parent_key=parent key when src.parent_id)` and raises `ConfigError(f"'{src.key}' ({src.kind}) declares no relation '{name}' (declared: {sorted(vocabulary)})")`. `_identity` returns `il.ComponentIdentity(parent.key if row.parent_id else None, row.key)`.

Delete `_check_vocabulary`, `_resolve_dst`, `_check_slot_target`, `_blocked_unbinds`, `_upsert_relation`, `_add_relation` and the `Binding` tuple alias.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_relations.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-db/src/interloper_db/store/relations.py packages/interloper-db/tests/store/test_relations.py
git commit -m "feat!(db): RelationStore binds by name through Relation.accepts

By Digitl"
```

---

### Task 3: `ComponentStore`: sibling rows from the core rule, guards by name

**Files:**
- Modify: `store/components.py:95-146` and `182-241` (`relations: dict[str, list[UUID]]`), `store/components.py:242-338` (`delete`, `_blocking_referrers`, `_blocking_referrers_into` read `on_delete`/`optional` by `name`), `store/components.py:719-830` (`_ensure_children` calls `_bind_siblings(session, source_cls, children)`; delete `_wire_intra_upstreams`)
- Test: `packages/interloper-db/tests/store/test_components.py`

**Interfaces:**
- Produces: `ComponentStore._bind_siblings(session, source_cls, children_by_key)` using `source_cls.sibling_bindings()`; idempotent (skips names that already hold a row).

- [ ] **Step 1: Write the failing tests**

```python
def test_create_source_binds_sibling_upstreams(store, ...) -> None:
    source = store.components.create(_ORG, kind="source", key="wire_up_source")
    rows = store.relations.list_all(_ORG, src_kind="asset", dst_kind="asset")
    assert {(r.name, r.dst_id) for r in rows} == {("rows", _child(source, "rows").id)}

def test_create_with_relations_by_name(store, ...) -> None:
    source = store.components.create(_ORG, kind="source", key="widget", relations={"connection": [conn.id], "destinations": [bq.id]})
    assert {r.name for r in source.out_relations} == {"connection", "destinations"}

def test_delete_blocks_on_required_referrer_and_detaches_optional(...) -> None: ...   # port from today's tests, by name
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_components.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement**

```python
    @staticmethod
    def _bind_siblings(session: Session, source_cls: type[il.Source], children_by_key: dict[str, Component]) -> None:
        bound = {
            (row.src_id, row.name)
            for row in session.exec(
                select(ComponentRelation).where(col(ComponentRelation.src_id).in_([c.id for c in children_by_key.values()]))
            ).all()
        }
        for asset_key, names in source_cls.sibling_bindings().items():
            child = children_by_key.get(asset_key)
            if child is None:
                continue
            for name, sibling_key in names.items():
                sibling = children_by_key.get(sibling_key)
                if sibling is None or (child.id, name) in bound:
                    continue
                session.add(ComponentRelation(src_id=child.id, name=name, dst_id=sibling.id, org_id=child.org_id, src_kind="asset", dst_kind="asset"))
        session.flush()
```

Delete guard: `_blocking_referrers` asks `self._relations._relation_detaches(session, referrer_row, relation_row)`, which resolves the referrer's vocabulary by `relation_row.name`.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_components.py packages/interloper-db/tests/store/test_relations.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-db/src/interloper_db/store/components.py packages/interloper-db/tests/store/test_components.py
git commit -m "feat(db): sibling upstream rows come from Source.sibling_bindings; guards read on_delete by name

By Digitl"
```

---

### Task 4: Hydrator: the parent rule and `resolve=store.load`

**Files:**
- Modify: `store/hydration.py:119-214` (`_build_init`, `_relations_by_name`, `_dst_value`)
- Modify: `store/components.py:339-435` (`_load` passes `resolve=self.load` to `il.Component.from_spec`; `_load_owned_asset` unchanged)
- Test: `packages/interloper-db/tests/store/test_hydration.py`

**Interfaces:**
- Produces: `Hydrator._build_init(session, row) -> dict[str, Any]` where each relation name maps to a nested spec (target without `parent_id`), a `{"ref": "<dst_id>"}` (target with `parent_id`), or a list of those when `relation.many`.

- [ ] **Step 1: Write the failing tests**

```python
def test_parentless_target_nests(store, ...) -> None:
    init = hydrator._build_init(session, source_row)            # source with connection + destinations rows
    assert init["connection"]["key"] == "conn"
    assert isinstance(init["destinations"], list) and init["destinations"][0]["key"] == "bq"

def test_asset_target_is_a_reference(...) -> None:
    init = hydrator._build_init(session, finance_row)
    assert init["assets"]["revenue"]["orders"] == {"ref": str(shop_orders_row.id)}

def test_load_resolves_cross_source_upstream_through_store(store, ...) -> None:
    finance = store.components.load(finance_row.id)
    assert finance.revenue.orders.id == str(shop_orders_row.id)
    assert finance.revenue.orders.parent.key == "shop"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-db/tests/store/test_hydration.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement**

```python
    def _build_init(self, session, db_component):
        init = self.decode_data(db_component) if KINDS[db_component.kind].sensitive else dict(db_component.config or {})
        vocabulary = self._catalog.vocabulary(db_component.kind, db_component.key, parent_key=db_component.parent_key(session))
        for name, rows in self._relations_by_name(session, db_component.id).items():
            relation = vocabulary.get(name)
            if relation is None:
                raise HydrationError(f"Component {db_component.id} ({db_component.kind}) has '{name}' relations its class does not declare")
            values = [self._dst_value(session, row) for row in rows]
            init[name] = values if relation.many else values[0]
        children = session.exec(select(Component).where(Component.parent_id == db_component.id).order_by(Component.created_at)).all()
        if assets := {child.key: {"id": str(child.id), **self._build_init(session, child)} for child in children}:
            init["assets"] = assets
        return init

    def _dst_value(self, session, row) -> dict[str, Any]:
        db_dst = session.get(Component, row.dst_id)
        if db_dst is None:
            raise HydrationError(...)
        if db_dst.parent_id is not None:
            return {"ref": str(db_dst.id)}
        return self.build_component_spec(session, db_dst).model_dump(mode="json")
```

In `_load`: `il.Component.from_spec(spec, resolve=lambda ref: self.load(UUID(ref)))`. `from_spec` (phase 1) looks the reference up in the document first, so an asset referenced by a job that also targets its source is not loaded twice.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-db -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-db/src/interloper_db/store/hydration.py packages/interloper-db/src/interloper_db/store/components.py packages/interloper-db/tests/store/test_hydration.py
git commit -m "feat(db): hydrate relations by name; parented targets become references resolved through the store

By Digitl"
```

---

### Task 5: API: `name` on the wire

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/components.py:60-95` (models), `:230-260` (`_relations_of`, `_bindings`), `:287-320` (`list_relations(name, src_kind, dst_kind)`), `:456-530` (`add_relation`, `remove_relation` at `/{component_id}/relations/{name}/{dst_id}`), `:655-680` (fetch-field lookup: `relation = component_cls.relations.get(slot)`; `resource_cls = relation.target`)
- Test: `packages/interloper-api/tests/test_components.py`

**Interfaces:**
- Produces: `RelationEntry(dst_id)`, `RelationCreateRequest(name, dst_id)`, `RelationRef(dst_id, dst_kind)`, `RelationResponse(src_id, name, dst_id, dst_kind)`; `ComponentResponse.relations: dict[str, list[RelationRef]]` keyed by name.

- [ ] **Step 1: Write the failing tests**

```python
def test_add_relation_by_name(client, ...) -> None:
    response = client.post(f"/components/{source_id}/relations", json={"name": "destinations", "dst_id": str(bq_id)})
    assert response.status_code == 201
    assert response.json()["name"] == "destinations"

def test_remove_relation_route_uses_name(client, ...) -> None:
    assert client.delete(f"/components/{source_id}/relations/destinations/{bq_id}").status_code == 204

def test_list_relations_filters_by_kind(client, ...) -> None:
    rows = client.get("/components/relations", params={"src_kind": "asset", "dst_kind": "asset"}).json()
    assert all(r["dst_kind"] == "asset" for r in rows)

def test_component_response_relations_keyed_by_name(client, ...) -> None:
    body = client.get(f"/components/{source_id}").json()
    assert set(body["relations"]) <= {"connection", "destinations"}
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-api/tests/test_components.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement** the model and route changes listed above. `_bindings` returns `{name: [entry.dst_id ...]}`.

- [ ] **Step 4: Run the API suite**

Run: `uv run --frozen pytest packages/interloper-api -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-api
git commit -m "feat!(api): relations are addressed by name; org-wide list filters by kinds

By Digitl"
```

---

### Task 6: Toolkit: lineage by kinds, one `bind_relation` tool

**Files:**
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/lineage.py` (`list_all(org_id, src_kind="asset", dst_kind="asset")`; `param_name=dep.name`)
- Modify: `packages/interloper-toolkit/src/interloper_toolkit/collection.py` (add `bind_relation(ctx, component_id, name, dst_id) -> BindResult | ToolError` and `unbind_relation`), `models.py` (`BindResult(src_id, name, dst_id, dst_kind)`), register both where the other collection tools are registered
- Test: `packages/interloper-toolkit/tests/test_toolkit.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_get_upstream_reports_relation_name(ctx, store, ...) -> None:
    # finance.revenue -[orders]-> shop.orders, created through store.relations.add(..., name="orders", ...)
    result = get_upstream(ctx, str(revenue_row.id))
    assert [(e.param_name, e.asset_id) for e in result.upstream] == [("orders", str(orders_row.id))]

def test_lineage_ignores_non_asset_relations(ctx, store, ...) -> None:
    # a source -[destinations]-> destination row must not appear as an edge
    result = get_full_lineage(ctx, str(revenue_row.id), direction="upstream")
    assert all(item.asset_key for item in result.items)

def test_bind_relation_creates_row(ctx, store, ...) -> None:
    result = bind_relation(ctx, str(source_row.id), "destinations", str(bq_row.id))
    assert (result.name, result.dst_kind) == ("destinations", "destination")

def test_bind_relation_wrong_kind_is_tool_error(ctx, store, ...) -> None:
    result = bind_relation(ctx, str(source_row.id), "connection", str(bq_row.id))
    assert isinstance(result, ToolError) and "does not accept" in result.error
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-toolkit -q`

- [ ] **Step 3: Implement**

```python
def bind_relation(ctx: ToolkitContext, component_id: str, name: str, dst_id: str) -> BindResult | ToolError:
    try:
        row = ctx.store.relations.add(UUID(component_id), name=name, dst_id=UUID(dst_id))
    except (ConfigError, NotFoundError) as error:
        return ToolError(error=str(error))
    return BindResult(src_id=str(row.src_id), name=row.name, dst_id=str(row.dst_id), dst_kind=row.dst_kind)
```

`unbind_relation` mirrors it over `remove`. Lineage functions call `ctx.store.relations.list_all(ctx.org_id, src_kind="asset", dst_kind="asset")` and use `dep.name`.

- [ ] **Step 4: Run** `uv run --frozen pytest packages/interloper-toolkit -q`

- [ ] **Step 5: Commit** `feat(toolkit): lineage by asset kinds; generic bind_relation and unbind_relation tools`.

---

### Task 7: Agent: bind the connection by relation name

**Files:**
- Modify: `packages/interloper-agent/src/interloper_agent/tools/collection.py:497-540` (`_source_relations`), `:780-795` (`relations={"targets": targets}`)
- Test: `packages/interloper-agent/tests/tools/test_collection.py`

- [ ] **Step 1: Write the failing test**

```python
def test_source_relations_binds_connection_by_name(...) -> None:
    defn = {"relations": {"connection": {"kind": "connection", "key": "facebook_ads_connection", "optional": False}, "destinations": {"kind": "destination", "many": True, "optional": True}}}
    relations, error = _source_relations(store, org_id, defn, "facebook_ads", str(connection.id), [str(bq.id)])
    assert error is None
    assert relations == {"connection": [connection.id], "destinations": [bq.id]}
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement**

```python
    relations_defn = defn.get("relations") or {}
    connection_relations = {n: r for n, r in relations_defn.items() if "connection" in (r["kind"] if isinstance(r["kind"], list) else [r["kind"]])}
    bindings: dict[str, list[UUID]] = {}
    if connection is not None:
        name = next((n for n, r in connection_relations.items() if not r.get("key") or connection.key in ([r["key"]] if isinstance(r["key"], str) else r["key"])), None)
        if name is None:
            return None, {"status": "error", "error": f"Connection '{connection.key}' does not fit any relation of '{source_key}'"}
        bindings[name] = [connection.id]
    for name, r in connection_relations.items():
        if not r.get("optional") and name not in bindings:
            return None, {"status": "error", "error": f"'{source_key}' requires a '{r.get('key') or 'connection'}' as '{name}'; pick one from the collection or set one up first"}
    if destination_ids:
        bindings["destinations"] = [...]
```

- [ ] **Step 4: Run** `uv run --frozen pytest packages/interloper-agent -q`, **Step 5: Commit** `feat(agent): bind source connections and destinations by relation name`.

---

### Task 8: Scheduler: hooks by name, no hand-rolled upstream join

**Files:**
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/hooks.py:183-195` (`ComponentRelation.name == "watches"`)
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/executor.py:110-190` (delete `_resolve_upstream` and its call; the DAG now includes bound upstreams read-only, phase 1 Task 6, and the hydrator bound them, Task 4)
- Test: `packages/interloper-scheduler/tests/test_hooks.py`, `test_executor.py` (the existing "upstream joined as non-materializable" test must still pass through the DAG; adapt its setup to bind through the store)

- [ ] **Step 1: Run the scheduler suite to see the failures**, **Step 2: Implement**, **Step 3: Run** `uv run --frozen pytest packages/interloper-scheduler -q`, **Step 4: Commit** `refactor(scheduler): hooks match on the watches relation; the DAG joins read-only upstreams`.

---

### Task 9: Throwaway-database round trip and the full suite

- [ ] **Step 1: Create a scratch database and migrate to 016, seed a few rows in the old shape, upgrade, downgrade, upgrade**

```bash
createdb interloper_scratch
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db upgrade 016
psql interloper_scratch -c "INSERT INTO components ... ; INSERT INTO component_relations (src_id, type, slot, dst_id, org_id, src_kind, dst_kind) VALUES (... 'resource', 'connection' ...), (... 'destination', '' ...), (... 'upstream', 'orders' ...);"
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db upgrade head
psql interloper_scratch -c "SELECT src_kind, name, dst_kind FROM component_relations ORDER BY name;"
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db downgrade 016
psql interloper_scratch -c "SELECT type, slot FROM component_relations ORDER BY type;"
INTERLOPER_POSTGRES_DATABASE=interloper_scratch uv run --frozen interloper db upgrade head
dropdb interloper_scratch
```

Expected after upgrade: `connection`, `destinations`, `orders`. After downgrade: `(resource, connection)`, `(destination, '')`, `(upstream, orders)`. Use the exact CLI verb the repo provides for migrations (check `interloper db --help`).

- [ ] **Step 2: Whole-repo checks**

Run: `uv run --frozen ruff check && uv run --frozen ty check && uv run --frozen pytest -q`
Expected: green across every package except `interloper-app` (no Python tests) and any phase 4 owned TypeScript.

- [ ] **Step 3: Retired-name sweep**

Run: `grep -rn "slot\b\|\.type ==\|type=\"upstream\"\|type=\"watch\"\|relation_types\|resource_types\|slots\b" packages/interloper-db packages/interloper-api packages/interloper-toolkit packages/interloper-agent packages/interloper-scheduler --include='*.py' | grep -v "tests/\|# " || echo clean`
Expected: `clean` (review any hit that is a legitimate unrelated use of the word).

- [ ] **Step 4: Squash, push, open PR** `feat!: platform relations by name` when Guillaume asks. Deployment note for the PR body: run migration 017 before the api, scheduler and worker images roll (they read `name`), and update the `interloper-manifests` repo in lockstep (its YAML `dependencies:` keys become relation names).
