# Components List Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `GET /components` cheap on large organisations and stop every app page from loading the whole collection.

**Architecture:** The collection endpoint lists root components with their owned components nested, mirroring the core's "owner is the unit" rule. The store reads each row once (status, payload, public subset, discriminator) and owns the delete-impact rule the app used to mirror. Relation refs carry their target's key and name so a page of one kind renders without loading other kinds.

**Tech Stack:** Python 3.10+, SQLModel/SQLAlchemy (`selectinload`), FastAPI, pydantic; Nuxt 4 + Pinia + @nuxt/ui in `packages/interloper-app/app`.

**Spec:** `docs/superpowers/specs/2026-09-16-components-list-performance-design.md`

## Global Constraints

- Run Python commands from the repo root with `uv run ...`; frontend commands from `packages/interloper-app/app` with `pnpm`.
- Do **not** commit. Finished work rests uncommitted in the tree; Guillaume commits on explicit ask.
- Follow `.claude/rules/python-style.md`: full names (no abbreviations), Google docstrings with every applicable section, sparse comments, section dividers only where they exist already.
- Tests mirror the module layout: store tests go in `packages/interloper-db/tests/store/test_components.py`, route tests in `packages/interloper-api/tests/routes/test_components.py`. No new `test_<feature>.py` files.
- `ComponentStore.list_all` keeps its flat, row-level semantics. The toolkit and agent depend on it.
- The API change is breaking (`feat!`): the unfiltered list no longer repeats owned assets at top level, and `?kind=asset` returns standalone assets only.

---

## Task 1: Relation rows carry their target

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/models/components.py` (class `ComponentRelation`, after `dst_kind`)
- Modify: `packages/interloper-db/src/interloper_db/store/components.py:51-59` (`COMPONENT_LOAD_OPTIONS`)
- Test: `packages/interloper-db/tests/store/test_components.py` (class `TestCrud`)

**Interfaces:**
- Produces: `ComponentRelation.dst: Component | None`, a view-only relationship eager-loaded on every row the store hands out (root relations and children's relations).

- [ ] **Step 1: Write the failing test** in `TestCrud`

```python
    def test_relations_carry_their_target(self, store: Store, connection: Component):
        source = store.components.create(
            _ORG, kind="source", key="wire_down_source", relations={"connection": [connection.id]}
        )

        row = store.components.get(source.id)

        relation = next(r for r in row.out_relations if r.name == "connection")
        assert (relation.dst.id, relation.dst.key) == (connection.id, "wire_connection")
```

- [ ] **Step 2: Run it**

Run: `uv run pytest packages/interloper-db/tests/store/test_components.py::TestCrud::test_relations_carry_their_target -v`
Expected: FAIL with `AttributeError: 'ComponentRelation' object has no attribute 'dst'`

- [ ] **Step 3: Add the relationship and load it**

In `models/components.py`, after `dst_kind: str` on `ComponentRelation`:

```python
    dst: Optional["Component"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "foreign(ComponentRelation.dst_id) == Component.id",
            "viewonly": True,
        },
    )
```

In `store/components.py`, import `ComponentRelation` is already there; change the load options:

```python
COMPONENT_LOAD_OPTIONS = [
    selectinload(Component.parent),  # ty: ignore[invalid-argument-type]
    selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
    selectinload(Component.children)  # ty: ignore[invalid-argument-type]
    .selectinload(Component.out_relations)  # ty: ignore[invalid-argument-type]
    .selectinload(ComponentRelation.dst),  # ty: ignore[invalid-argument-type]
]
```

Update the comment above it to say the relations' targets ride along too.

- [ ] **Step 4: Run the store tests**

Run: `uv run pytest packages/interloper-db/tests/store -q`
Expected: all PASS (the new test included).

---

## Task 2: `list_roots`

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/components.py:162-183` (`list_all`)
- Test: `packages/interloper-db/tests/store/test_components.py` (class `TestCrud`)

**Interfaces:**
- Produces: `ComponentStore.list_roots(org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]`; roots only (`parent_id IS NULL`), eager-loaded, oldest first, children nested.

- [ ] **Step 1: Write the failing tests** in `TestCrud`

```python
    def test_list_roots_nests_owned_components(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_up_source")
        store.components.create(uuid4(), kind="connection", key="wire_connection", config={}, encrypted=False)

        roots = store.components.list_roots(_ORG)

        assert [row.id for row in roots] == [connection.id, source.id]
        nested = next(row for row in roots if row.id == source.id)
        assert {child.key for child in nested.children} == {"rows", "totals"}

    def test_list_roots_filters_root_kinds(self, store: Store, connection: Component):
        store.components.create(_ORG, kind="source", key="wire_up_source")

        assert [row.id for row in store.components.list_roots(_ORG, kinds=["connection"])] == [connection.id]
        # Owned assets are not roots: a kind filter never surfaces them.
        assert store.components.list_roots(_ORG, kinds=["asset"]) == []
```

- [ ] **Step 2: Run them**

Run: `uv run pytest packages/interloper-db/tests/store/test_components.py::TestCrud -k list_roots -v`
Expected: FAIL with `AttributeError: 'ComponentStore' object has no attribute 'list_roots'`

- [ ] **Step 3: Implement** by sharing the statement with `list_all`

```python
    def list_all(self, org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]:
        """List an organisation's component rows, optionally filtered by kind.

        Every row lists, owned ones included: this is the row-level view the
        toolkit reads. :meth:`list_roots` is the collection view.

        Args:
            org_id: Organisation UUID.
            kinds: Kinds to include (``None`` = all).

        Returns:
            Eager-loaded component rows, oldest first.
        """
        with session_scope(self._engine) as session:
            return list(session.exec(self._listing(org_id, kinds)).all())

    def list_roots(self, org_id: UUID, *, kinds: list[str] | None = None) -> list[Component]:
        """List an organisation's root components, their owned components nested under ``children``.

        The collection's unit is the owner: an owned component (a source's
        asset) never lists on its own but rides inside the root that owns it,
        the way the catalog reaches an owned definition through its owner.

        Args:
            org_id: Organisation UUID.
            kinds: Root kinds to include (``None`` = all).

        Returns:
            Eager-loaded root rows, oldest first, each carrying its children.
        """
        with session_scope(self._engine) as session:
            statement = self._listing(org_id, kinds).where(col(Component.parent_id).is_(None))
            return list(session.exec(statement).all())

    @staticmethod
    def _listing(org_id: UUID, kinds: list[str] | None) -> SelectOfScalar[Component]:
        """The eager-loaded, oldest-first selection of an organisation's components.

        Args:
            org_id: Organisation UUID.
            kinds: Kinds to include (``None`` = all).

        Returns:
            The select statement, for the caller to narrow further.
        """
        statement = (
            select(Component)
            .where(Component.org_id == org_id)
            .options(*COMPONENT_LOAD_OPTIONS)
            .order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
        )
        if kinds:
            statement = statement.where(col(Component.kind).in_(kinds))
        return statement
```

Import `SelectOfScalar` from `sqlmodel.sql.expression`.

- [ ] **Step 4: Run the store tests and the type check**

Run: `uv run pytest packages/interloper-db/tests/store -q && uv run ty check packages/interloper-db`
Expected: PASS, no type errors.

---

## Task 3: One reading per row

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/components.py` (`status`, `public_config`, `discriminator`, new `read`, new `ComponentReading` near the top of the module after `COMPONENT_LOAD_OPTIONS`)
- Modify: `packages/interloper-db/src/interloper_db/store/__init__.py` and `packages/interloper-db/src/interloper_db/__init__.py` (export `ComponentReading`)
- Test: `packages/interloper-db/tests/store/test_components.py` (class `TestStatus`, plus a new `TestReading` class right after it)

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class ComponentReading:
      status: ComponentStatus
      config: dict[str, Any] | None      # decoded payload; None when it cannot be read
      public_config: dict[str, Any]      # the schema's x-public subset of config
      discriminator: str | None
  ComponentStore.read(row: Component, *, parent_key: str | None = None) -> ComponentReading
  ```
- `status()`, `public_config()`, `discriminator()` remain, as views over `read()`.

- [ ] **Step 1: Write the failing tests**

Add to `TestStatus`:

```python
    def test_a_standalone_asset_resolves_flat(self, store: Store):
        row = Component(org_id=_ORG, kind="asset", key="guard_upstream")
        assert store.components.status(row) is ComponentStatus.OK
```

Add a new class after `TestStatus`:

```python
class TestReading:
    """One decode per row: status, payload, public subset and discriminator together."""

    @staticmethod
    def _counting_store(catalog: il.Catalog) -> tuple[Store, list[bytes]]:
        calls: list[bytes] = []

        def _decrypt(data: bytes) -> bytes:
            calls.append(data)
            return data[::-1]

        return Store(catalog=catalog, encrypt=lambda b: b[::-1], decrypt=_decrypt), calls

    def test_reads_a_secret_row_with_one_decrypt(self, component_db: Engine):
        catalog = il.Catalog(components={PublicToggleConnection.key: PublicToggleConnection.definition()})
        store, calls = self._counting_store(catalog)
        row = store.components.create(
            _ORG, kind="connection", key=PublicToggleConnection.key, config={"api_key": "s3cret", "auto_renew": False}
        )
        calls.clear()

        reading = store.components.read(row)

        assert len(calls) == 1
        assert reading.status is ComponentStatus.OK
        assert reading.config == {"api_key": "s3cret", "auto_renew": False}
        assert reading.public_config == {"auto_renew": False}
        assert reading.discriminator is None

    def test_an_unreadable_row_discloses_nothing(self, component_db: Engine):
        catalog = il.Catalog(components={PublicToggleConnection.key: PublicToggleConnection.definition()})
        written, _ = self._counting_store(catalog)
        row = written.components.create(_ORG, kind="connection", key=PublicToggleConnection.key, config={"api_key": "s"})

        def _wrong_key(_data: bytes) -> bytes:
            raise InvalidToken

        reading = Store(catalog=catalog, encrypt=lambda b: b[::-1], decrypt=_wrong_key).components.read(row)

        assert reading.status is ComponentStatus.UNREADABLE
        assert reading.config is None
        assert reading.public_config == {}

    def test_a_drifted_plain_row_keeps_its_config(self, store: Store):
        row = Component(org_id=_ORG, kind="job", key="gone_job", config={"cron": "0 * * * *"})

        reading = store.components.read(row)

        assert reading.status is ComponentStatus.MISSING
        assert reading.config == {"cron": "0 * * * *"}
        assert reading.public_config == {}

    def test_the_views_agree_with_the_reading(self, component_db: Engine):
        store = Store(catalog=il.Catalog.from_assets([DiscriminatedSource]))
        row = store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "42"})

        reading = store.components.read(row)

        assert store.components.status(row) is reading.status
        assert store.components.discriminator(row) == reading.discriminator == "42"
        assert store.components.public_config(row) == reading.public_config
```

- [ ] **Step 2: Run them**

Run: `uv run pytest packages/interloper-db/tests/store/test_components.py -k "TestReading or standalone_asset" -v`
Expected: FAIL (`read` missing; the standalone asset test may already pass, that is fine).

- [ ] **Step 3: Implement**

At the top of `store/components.py`, add `from dataclasses import dataclass` and, after `COMPONENT_LOAD_OPTIONS`:

```python
@dataclass(frozen=True)
class ComponentReading:
    """What one read of a component row yields for a response.

    ``config`` is the decoded payload, ``None`` when the row cannot be read;
    ``public_config`` is its schema-marked ``x-public`` subset; ``discriminator``
    is the class's discriminator value read off it. The whole reading costs
    one decode, so a response derives every field from it instead of
    decoding per field.
    """

    status: ComponentStatus
    config: dict[str, Any] | None
    public_config: dict[str, Any]
    discriminator: str | None
```

Replace `status`, `public_config` and `discriminator` in the `Hydration & status` section with:

```python
    def read(self, db_component: Component, *, parent_key: str | None = None) -> ComponentReading:
        """Read a row once: its status and every view of its payload a response shows.

        The catalog answers first: without a resolvable key there is no
        schema to read the payload against, so drift outranks readability.
        The payload is then decoded once; a secret payload that does not
        decode makes the row ``UNREADABLE``, while a plain kind's config is
        kept even when its key has drifted, so the UI can still show what the
        row holds.

        Args:
            db_component: The row to read.
            parent_key: The owning source's key when the caller already knows
                it. Defaults to ``None``, which reads it from the database for
                an owned row.

        Returns:
            The reading: status, decoded config, its public subset and the
            discriminator value.
        """
        status = self._key_status(db_component, parent_key=parent_key)
        config = self._current_config(db_component)
        if config is None and status is ComponentStatus.OK:
            status = ComponentStatus.UNREADABLE
        return ComponentReading(
            status=status,
            config=config,
            public_config=self._public_subset(db_component, config),
            discriminator=self._discriminator_of(db_component, config),
        )

    def status(self, db_component: Component, *, parent_key: str | None = None) -> ComponentStatus:
        """Usability status of a component row: catalog key, then payload.

        Args:
            db_component: The row to resolve.
            parent_key: The owning source's key when the caller already knows
                it. Defaults to ``None``, which reads it from the database.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING`` for the row's catalog key, or
            ``UNREADABLE`` when the key resolves but its payload does not
            decode.
        """
        return self.read(db_component, parent_key=parent_key).status

    def public_config(self, db_component: Component) -> dict[str, Any]:
        """The disclosable subset of a component's config payload.

        Args:
            db_component: The row to read the payload from.

        Returns:
            The fields the config schema marks ``x-public``; empty when none
            are, the row's key has drifted, or the payload cannot be read.
        """
        return self.read(db_component).public_config

    def discriminator(self, db_component: Component) -> str | None:
        """The value of the component's discriminator field, read off its stored config.

        Args:
            db_component: The row to read the payload from.

        Returns:
            The discriminator value as a string, or ``None`` when the class
            declares none, the value is blank, or the key or payload can't be
            read.
        """
        return self.read(db_component).discriminator

    def _key_status(self, db_component: Component, *, parent_key: str | None = None) -> ComponentStatus:
        """Catalog status of a row's key, an owned row resolving through its owner.

        Args:
            db_component: The row to resolve.
            parent_key: The owner's key when the caller already knows it.
                Defaults to ``None``, which reads it a row away.

        Returns:
            ``OK``, ``DISABLED`` or ``MISSING``.
        """
        if db_component.parent_id is not None:
            if parent_key is None:
                with session_scope(self._engine) as session:
                    parent_key = db_component.parent_key(session)
            return asset_status(self._catalog, db_component.key, source_key=parent_key)
        return source_status(self._catalog, db_component.key)

    def _public_subset(self, db_component: Component, config: dict[str, Any] | None) -> dict[str, Any]:
        """The ``x-public`` fields of a decoded payload, per the row's config schema.

        Args:
            db_component: The row whose key selects the schema.
            config: The decoded payload, or ``None`` when it could not be read.

        Returns:
            The disclosed fields; empty when the schema marks none public, the
            key does not resolve, or there is no payload.
        """
        definition = self._catalog.get(db_component.key)
        if definition is None or config is None:
            return {}
        properties = definition.config_schema.get("properties", {})
        public_fields = {name for name, schema in properties.items() if schema.get("x-public")}
        return {name: value for name, value in config.items() if name in public_fields}

    def _discriminator_of(self, db_component: Component, config: dict[str, Any] | None) -> str | None:
        """The discriminator value a decoded payload carries for the row's class.

        Args:
            db_component: The row whose key and kind select the class.
            config: The decoded payload, or ``None`` when it could not be read.

        Returns:
            The value as a string, or ``None`` when the class declares no
            discriminator, the value is blank, or there is no payload.
        """
        cls = self._resolve_class(db_component)
        field = cls.discriminator_field() if cls else None
        if field is None or config is None:
            return None
        value = config.get(field)
        return str(value) if value else None
```

Keep `decode_config` and `_current_config` as they are. Delete the old `status`/`public_config`/`discriminator` bodies (no duplicate logic remains). Export `ComponentReading` from `interloper_db.store` and `interloper_db` (`__all__` sorted).

- [ ] **Step 4: Run the store tests, ruff and ty**

Run: `uv run pytest packages/interloper-db/tests -q && uv run ruff check packages/interloper-db && uv run ty check packages/interloper-db`
Expected: PASS. `test_plaintext_rows_are_never_decrypted` and the whole `TestStatus`/`TestDiscriminator` classes must still pass unchanged.

---

## Task 4: Server-side delete impact

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/components.py` (`delete`, `_blocking_referrers`, `_blocking_referrers_into`, new `delete_impact`, new `DeleteImpact` dataclass next to `ComponentReading`)
- Modify: any other caller of `_blocking_referrers_into` (grep: `grep -rn "_blocking_referrers_into" packages/interloper-db/src`), switching it to `.blocking`
- Modify: `packages/interloper-db/src/interloper_db/store/__init__.py` and `packages/interloper-db/src/interloper_db/__init__.py` (export `DeleteImpact`)
- Test: `packages/interloper-db/tests/store/test_components.py` (new class `TestDeleteImpact` after `TestDeleteInUseGuard`)

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class DeleteImpact:
      blocking: list[dict[str, str | None]]   # {id, kind, key, name}, sorted by display name
      detaching: list[dict[str, str | None]]
  ComponentStore.delete_impact(component_ids: list[UUID]) -> DeleteImpact   # raises NotFoundError
  ComponentStore._referrers_into(session, target_ids: set[UUID], subtree_ids: set[UUID]) -> DeleteImpact
  ```

- [ ] **Step 1: Write the failing tests**

```python
class TestDeleteImpact:
    """The delete preview: who blocks, who detaches, before anything is deleted."""

    def test_splits_blocking_from_detaching_referrers(self, store: Store):
        up = store.components.create(_ORG, kind="source", key="wire_up_source", name="Up")
        down = store.components.create(_ORG, kind="source", key="wire_down_source", name="Down")
        reader = store.components.create(_ORG, kind="source", key="wire_down_optional_source", name="Reader")
        store.relations.add(_child(down, "consumer").id, name="rows", dst_id=_child(up, "rows").id)
        store.relations.add(_child(reader, "reader").id, name="rows", dst_id=_child(up, "rows").id)

        impact = store.components.delete_impact([up.id])

        assert [r["id"] for r in impact.blocking] == [str(down.id)]
        assert impact.detaching == [
            {"id": str(reader.id), "kind": "source", "key": "wire_down_optional_source", "name": "Reader"}
        ]
        assert store.components.get(up.id) is not None  # a preview deletes nothing

    def test_a_referrer_that_blocks_anywhere_is_only_blocking(self, store: Store):
        up = store.components.create(_ORG, kind="source", key="wire_up_source")
        required = store.components.create(_ORG, kind="asset", key="guard_required")
        optional = store.components.create(_ORG, kind="asset", key="guard_optional")
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        store.relations.add(required.id, name="up", dst_id=upstream.id)
        store.relations.add(optional.id, name="up", dst_id=upstream.id)

        impact = store.components.delete_impact([upstream.id, up.id])

        assert [r["id"] for r in impact.blocking] == [str(required.id)]
        assert [r["id"] for r in impact.detaching] == [str(optional.id)]

    def test_an_unknown_id_is_not_found(self, store: Store):
        with pytest.raises(NotFoundError):
            store.components.delete_impact([uuid4()])
```

If `guard_required` / `guard_optional` need different setup, copy it from `TestUpstreamDeleteSemantics.test_delete_blocks_on_blocking_referrer_and_detaches_the_detaching_one` (line ~373) rather than inventing new fixtures.

- [ ] **Step 2: Run them**

Run: `uv run pytest packages/interloper-db/tests/store/test_components.py::TestDeleteImpact -v`
Expected: FAIL with `AttributeError: ... 'delete_impact'`

- [ ] **Step 3: Implement**

Next to `ComponentReading`:

```python
@dataclass(frozen=True)
class DeleteImpact:
    """What deleting a set of components does to the components bound to them.

    Each entry is a ``{id, kind, key, name}`` mapping, the shape
    :class:`~interloper.errors.InUseError` reports. A referrer that blocks
    through any relation is listed only as blocking.
    """

    blocking: list[dict[str, str | None]]
    detaching: list[dict[str, str | None]]
```

In `ComponentStore`, after `delete`:

```python
    def delete_impact(self, component_ids: list[UUID]) -> DeleteImpact:
        """Preview what deleting *component_ids* does to the components bound to them.

        The same rule :meth:`delete` enforces, evaluated without deleting:
        referrers outside the subtree (the ids plus their owned components)
        whose relations into it block, and those whose relations detach.

        Args:
            component_ids: The components about to be deleted.

        Returns:
            The blocking and detaching referrers.

        Raises:
            NotFoundError: If any of the ids does not exist.
        """
        with session_scope(self._engine) as session:
            for component_id in component_ids:
                if session.get(Component, component_id) is None:
                    raise NotFoundError(f"Component {component_id} not found")
            child_ids = session.exec(select(Component.id).where(col(Component.parent_id).in_(component_ids))).all()
            subtree_ids = set(component_ids) | set(child_ids)
            return self._referrers_into(session, subtree_ids, subtree_ids)
```

Rewrite `_blocking_referrers` to end with `return self._referrers_into(session, subtree_ids, subtree_ids).blocking`, and replace `_blocking_referrers_into` with:

```python
    def _referrers_into(self, session: Session, target_ids: set[UUID], subtree_ids: set[UUID]) -> DeleteImpact:
        """Referrers whose relations point into *target_ids* from outside *subtree_ids*, by outcome.

        An edge whose name the referrer declares ``on_delete="detach"``
        detaches; any other blocks. A referrer that is a source-owned asset is
        reported as its parent source, the unit the user can act on, and one
        that blocks through any edge is only reported as blocking.

        Args:
            session: Open session to query in.
            target_ids: Component IDs whose in-bound relations are inspected.
            subtree_ids: Component IDs that count as "inside" — relations
                originating there are ignored.

        Returns:
            The blocking and detaching referrers, each sorted by display name.
        """
        rows = session.exec(
            select(ComponentRelation).where(
                col(ComponentRelation.dst_id).in_(target_ids),
                col(ComponentRelation.src_id).not_in(subtree_ids),
            )
        ).all()
        blocking: dict[UUID, Component] = {}
        detaching: dict[UUID, Component] = {}
        for relation in rows:
            src = session.get(Component, relation.src_id)
            if src is None:
                continue
            detaches = self._relations._relation_detaches(session, src, relation)
            if src.parent_id is not None and src.parent_id not in subtree_ids:
                src = session.get(Component, src.parent_id) or src
            (detaching if detaches else blocking)[src.id] = src
        for component_id in blocking:
            detaching.pop(component_id, None)
        return DeleteImpact(blocking=self._referrer_refs(blocking.values()), detaching=self._referrer_refs(detaching.values()))

    @staticmethod
    def _referrer_refs(components: Iterable[Component]) -> list[dict[str, str | None]]:
        """``{id, kind, key, name}`` mappings for *components*, sorted by display name.

        Args:
            components: The referrer rows.

        Returns:
            One mapping per component, in display order.
        """
        return [
            {"id": str(c.id), "kind": c.kind, "key": c.key, "name": c.name}
            for c in sorted(components, key=lambda c: ((c.name or c.key).lower(), str(c.id)))
        ]
```

Import `Iterable` from `collections.abc`. Fix every other `_blocking_referrers_into(...)` caller to `_referrers_into(...).blocking`. Export `DeleteImpact` alongside `ComponentReading`.

- [ ] **Step 4: Run the store tests, ruff and ty**

Run: `uv run pytest packages/interloper-db/tests -q && uv run ruff check packages/interloper-db && uv run ty check packages/interloper-db`
Expected: PASS, including `TestDeleteInUseGuard`, `TestUpstreamDeleteSemantics` and `TestChildRemovalGuard` unchanged.

---

## Task 5: API list over roots, one reading per row, self-describing relation refs

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/components.py` (module docstring, `RelationRef`, `ComponentResponse.from_row`, `_relations_of`, `list_components`)
- Test: `packages/interloper-api/tests/routes/test_components.py` (`_row`, `CrudStore`, `TestListComponents`, `TestPublicConfigDisclosure`, and any relation fake built with `SimpleNamespace(name=..., dst_id=..., dst_kind=...)`)

**Interfaces:**
- Consumes: `ComponentStore.list_roots`, `ComponentStore.read -> ComponentReading`, `ComponentRelation.dst`.
- Produces: `RelationRef` = `{dst_id, dst_kind, dst_key, dst_name}`; `GET /components/?kind=...` lists roots.

- [ ] **Step 1: Update the fakes and write the failing tests**

In `_row(...)`, keep the signature. Add a helper next to it:

```python
def _relation(name: str, dst_id: UUID, dst_kind: str, *, dst_key: str = "k", dst_name: str | None = None) -> Any:
    return SimpleNamespace(
        name=name, dst_id=dst_id, dst_kind=dst_kind, dst=SimpleNamespace(id=dst_id, key=dst_key, name=dst_name)
    )
```

Replace every `SimpleNamespace(name=..., dst_id=..., dst_kind=...)` relation fake in the file with `_relation(...)`.

In `CrudStore.__init__`, replace the components facet's `list_all=self._list_all` with `list_roots=self._list_roots` (rename the method and keep recording into `self.listed`), and replace the `status`/`decode_config`/`public_config`/`discriminator` lambdas with:

```python
            read=lambda row, parent_key=None: ComponentReading(
                status=ComponentStatus.OK, config=row.config, public_config={}, discriminator=None
            ),
```

Import `ComponentReading` from `interloper_db`. Where `TestPublicConfigDisclosure` (line ~228) builds its own fake store, give it a `read` that returns the reading the test wants (`public_config={"auto_renew": True}` for the list case, full `config` for the detail case) instead of separate `status`/`public_config`/`decode_config` lambdas.

Add to `TestListComponents`:

```python
    def test_lists_roots(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_client.get("/components/?kind=source")

        assert crud_store.listed == [{"org_id": _ORG_ID, "kinds": ["source"]}]

    def test_relation_refs_carry_their_target(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        connection_id = uuid4()
        crud_store.rows = [
            _row(kind="source", key="fb", relations=[_relation("connection", connection_id, "connection", dst_key="facebook_ads", dst_name="FB")])
        ]

        [row] = crud_client.get("/components/").json()

        assert row["relations"] == {
            "connection": [{"dst_id": str(connection_id), "dst_kind": "connection", "dst_key": "facebook_ads", "dst_name": "FB"}]
        }
```

- [ ] **Step 2: Run the route tests**

Run: `uv run pytest packages/interloper-api/tests/routes/test_components.py -q`
Expected: FAIL (`list_roots` not called, `dst_key` missing).

- [ ] **Step 3: Implement**

`RelationRef`:

```python
class RelationRef(BaseModel):
    """One relation binding in a component response, with enough of its target to label it."""

    dst_id: UUID
    dst_kind: str
    dst_key: str
    dst_name: str | None = None
```

`_relations_of`:

```python
    for relation in row.out_relations:
        grouped.setdefault(relation.name, []).append(
            RelationRef(
                dst_id=relation.dst_id, dst_kind=relation.dst_kind, dst_key=relation.dst.key, dst_name=relation.dst.name
            )
        )
```

`from_row` body, replacing the `status = ...` through `config = ...` block:

```python
        reading = store.components.read(row, parent_key=parent_key)
        config = reading.config
        if KINDS[row.kind].sensitive and not include_config:
            config = None if reading.status is ComponentStatus.UNREADABLE else reading.public_config
```

and pass `discriminator=reading.discriminator, status=reading.status`. Update the docstring: it reads the row once; secret kinds disclose the public subset in list responses, the decoded payload in detail responses, nothing when unreadable.

`list_components`:

```python
    rows = store.components.list_roots(org_id, kinds=kind)
```

with the docstring saying: lists the organisation's root components, owned components nested under `children`; `kind` filters roots, so `asset` yields standalone assets only. Update the module docstring's `children` sentence to: "owned components ride under their owner's ``children`` and never list on their own".

- [ ] **Step 4: Run the API tests, ruff and ty**

Run: `uv run pytest packages/interloper-api/tests -q && uv run ruff check packages/interloper-api && uv run ty check packages/interloper-api`
Expected: PASS.

---

## Task 6: `GET /components/delete-impact`

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/components.py` (new models after `RelationResponse`; new route right after `list_relations`, before `/{component_id}` routes)
- Test: `packages/interloper-api/tests/routes/test_components.py` (`CrudStore`, new class `TestDeleteImpact` after `TestListRelations`)

**Interfaces:**
- Consumes: `ComponentStore.delete_impact(list[UUID]) -> DeleteImpact`.
- Produces: `GET /components/delete-impact?id=<uuid>&id=<uuid>` → `{"blocking": [UsedByRef], "detaching": [UsedByRef]}` with `UsedByRef = {id: str, kind: str, key: str, name: str | None}`.

- [ ] **Step 1: Extend the fake and write the failing tests**

In `CrudStore.__init__`: `self.impact_requested: list[list[UUID]] = []` and `self.impact = DeleteImpact(blocking=[], detaching=[])`; in the components facet add `delete_impact=self._delete_impact`:

```python
    def _delete_impact(self, component_ids: list[UUID]) -> DeleteImpact:
        if self.error:
            raise self.error
        self.impact_requested.append(component_ids)
        return self.impact
```

Tests:

```python
class TestDeleteImpact:
    """``GET /components/delete-impact`` — the preview behind the delete confirmation."""

    def test_returns_blocking_and_detaching_referrers(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        first, second = uuid4(), uuid4()
        referrer = {"id": str(uuid4()), "kind": "source", "key": "fb", "name": "FB"}
        crud_store.impact = DeleteImpact(blocking=[referrer], detaching=[])

        response = crud_client.get(f"/components/delete-impact?id={first}&id={second}")

        assert response.status_code == 200
        assert response.json() == {"blocking": [referrer], "detaching": []}
        assert crud_store.impact_requested == [[first, second]]

    def test_a_non_member_gets_404(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.role = None

        assert crud_client.get(f"/components/delete-impact?id={uuid4()}").status_code == 404

    def test_an_unknown_id_is_404(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.error = NotFoundError("gone")

        assert crud_client.get(f"/components/delete-impact?id={uuid4()}").status_code == 404
```

- [ ] **Step 2: Run them**

Run: `uv run pytest packages/interloper-api/tests/routes/test_components.py::TestDeleteImpact -v`
Expected: FAIL with 422 or 404 (the path is captured by `/{component_id}` until the route exists).

- [ ] **Step 3: Implement**

Models:

```python
class UsedByRef(BaseModel):
    """A component bound to something about to be deleted, as the 409 ``used_by`` payload names it."""

    id: str
    kind: str
    key: str
    name: str | None = None


class DeleteImpactResponse(BaseModel):
    """The preview behind a delete confirmation: who blocks it, who merely detaches."""

    blocking: list[UsedByRef]
    detaching: list[UsedByRef]

    @classmethod
    def from_impact(cls, impact: DeleteImpact) -> DeleteImpactResponse:
        """Convert the store's preview to its response model.

        Args:
            impact: The store's blocking and detaching referrers.

        Returns:
            The response model.
        """
        return cls(
            blocking=[UsedByRef(**ref) for ref in impact.blocking],  # ty: ignore[missing-argument]
            detaching=[UsedByRef(**ref) for ref in impact.detaching],  # ty: ignore[missing-argument]
        )
```

(Drop the `ty: ignore` comments if `ty` accepts the unpacking.) Route, placed directly after `list_relations`:

```python
@router.get("/delete-impact")
def get_delete_impact(
    user: CurrentUserDep,
    store: StoreDep,
    component_id: Annotated[list[UUID], Query(alias="id")],
) -> DeleteImpactResponse:
    """Preview what deleting the given components does to the components bound to them.

    The same rule the delete guard enforces, evaluated without deleting, so
    the confirmation can say up front what blocks and what detaches.

    Args:
        user: The authenticated user.
        store: The Store instance.
        component_id: The components about to be deleted.

    Returns:
        The blocking and detaching referrers.

    Raises:
        HTTPException: 404 when any id is unknown.
    """
    for one in component_id:
        load_authorized(store.components.get, one, user, store, label="Component")
    try:
        impact = store.components.delete_impact(component_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return DeleteImpactResponse.from_impact(impact)
```

Import `DeleteImpact` from `interloper_db`.

- [ ] **Step 4: Run the API tests, ruff and ty**

Run: `uv run pytest packages/interloper-api/tests -q && uv run ruff check packages/interloper-api && uv run ty check packages/interloper-api`
Expected: PASS.

---

## Task 7: App types and store

**Files:**
- Modify: `packages/interloper-app/app/app/types/component.ts` (`RelationRef`, new `DeleteImpact`)
- Modify: `packages/interloper-app/app/app/stores/components.ts`

**Interfaces:**
- Produces:
  ```ts
  interface RelationRef { dst_id: string; dst_kind: string; dst_key: string; dst_name: string | null }
  interface DeleteImpact { blocking: UsedByRef[]; detaching: UsedByRef[] }
  componentsStore.deleteImpact(ids: string | string[]): Promise<DeleteImpact>   // async now
  componentsStore.byId / byKind / search see owned components through the roots' children
  ```

- [ ] **Step 1: Types**

```ts
import type { UsedByRef } from '~/utils/apiErrors'

/** A relation entry embedded on a component (`component.relations[name]`), with enough of its target to label it. */
export interface RelationRef {
    dst_id: string
    dst_kind: string
    dst_key: string
    dst_name: string | null
}

/** `GET /components/delete-impact`: who blocks a deletion, who merely detaches. */
export interface DeleteImpact {
    blocking: UsedByRef[]
    detaching: UsedByRef[]
}
```

Update the `children` doc comment on `ComponentRecord` to: "Owned components (a source's assets). The list endpoint nests them here and never repeats them at top level."

- [ ] **Step 2: Store index and lookups**

Replace `byKind`, `byId`, `search`, `_upsert`, `_remove`:

```ts
    /** Every component by id, owned ones included: the roots' trees flattened. */
    const index = computed(() => {
        const map = new Map<string, ComponentRecord>()
        const visit = (record: ComponentRecord) => {
            map.set(record.id, record)
            for (const child of record.children ?? []) visit(child)
        }
        for (const root of components.value) visit(root)
        return map
    })

    function byKind(kind: string): ComponentRecord[] {
        return [...index.value.values()].filter(c => c.kind === kind)
    }

    function byId(id: string): ComponentRecord | undefined {
        return index.value.get(id)
    }

    function search(query: string, kind?: string): ComponentRecord[] {
        const base = kind ? byKind(kind) : [...index.value.values()]
        if (!query) return base
        const q = query.toLowerCase()
        return base.filter(c => (c.name?.toLowerCase().includes(q) ?? false) || c.key.toLowerCase().includes(q))
    }

    /** Place a fetched record: inside its owner's children when it has one, among the roots otherwise. */
    function _upsert(component: ComponentRecord) {
        const siblings = component.parent_id ? byId(component.parent_id)?.children : components.value
        if (!siblings) return
        const idx = siblings.findIndex(c => c.id === component.id)
        if (idx >= 0) siblings[idx] = { ...siblings[idx], ...component }
        else siblings.push(component)
    }

    function _remove(id: string) {
        components.value = components.value.filter(c => c.id !== id)
        for (const root of components.value) {
            if (root.children?.some(c => c.id === id)) root.children = root.children.filter(c => c.id !== id)
        }
    }
```

- [ ] **Step 3: Delete impact and realtime**

Delete `_relationDetaches` and the old `deleteImpact`; add:

```ts
    /** The server's delete preview for `ids`: the same rule its guard enforces, before anything is deleted. */
    async function deleteImpact(ids: string | string[]): Promise<DeleteImpact> {
        const params = new URLSearchParams()
        for (const id of Array.isArray(ids) ? ids : [ids]) params.append('id', id)
        return apiFetch<DeleteImpact>(`/components/delete-impact?${params}`)
    }
```

Import `DeleteImpact` from `~/types/component`. Remove `useCatalogStore()` from the store if nothing else uses it. In the realtime section replace `_refetchChanged` with:

```ts
    // The owner is the unit: an owned component's change refetches its owner,
    // whose detail carries the children.
    function _refetchChanged(record: Record<string, any>) {
        fetchOne(record.parent_id ?? record.id).catch(() => {})
    }
```

- [ ] **Step 4: Typecheck**

Run (from `packages/interloper-app/app`): `pnpm exec nuxt typecheck`
Expected: errors only in the consumers changed by Tasks 8 and 9 (`DataTable.vue`, `Table.vue`, `SourceNode.vue`); none in the store or types.

---

## Task 8: Async delete preview in the tables and the graph

**Files:**
- Modify: `packages/interloper-app/app/app/components/ui/DataTable.vue:31-38, 77-83`
- Modify: `packages/interloper-app/app/app/components/collection/Table.vue:67-70`
- Modify: `packages/interloper-app/app/app/components/graph/SourceNode.vue:103-105`

- [ ] **Step 1: DataTable**

Prop:

```ts
    /**
     * Impact preview for deleting the given ids (`componentsStore.deleteImpact`).
     * `blocking` referrers disable the destructive button; `detaching` ones
     * are listed as a heads-up. The backend guard stays the authority, so a
     * failed preview falls back to a plain confirmation.
     */
    deleteImpact?: (ids: string[]) => Promise<DeleteImpact>
```

`requestDelete`:

```ts
    const { blocking, detaching } = await props.deleteImpact?.(ids).catch(() => NO_IMPACT) ?? NO_IMPACT
```

Import `DeleteImpact` from `~/types/component`; type `NO_IMPACT` as `DeleteImpact`.

- [ ] **Step 2: Table.vue and SourceNode.vue**

Both call sites already sit in `async` functions. Change `componentsStore.deleteImpact(...)` to `await componentsStore.deleteImpact(...)` in each.

- [ ] **Step 3: Typecheck**

Run: `pnpm exec nuxt typecheck`
Expected: PASS.

---

## Task 9: Pages fetch their own kind and label targets from the refs

**Files:**
- Modify: `packages/interloper-app/app/app/pages/components/jobs.vue:30-31, 77-82, 112-124, 193`
- Modify: `packages/interloper-app/app/app/pages/components/hooks.vue:27-28, 31-34, 54-60, 119`
- Modify: `packages/interloper-app/app/app/pages/components/destinations.vue:29-30, 37-40, 48-52, 96`
- Modify: `packages/interloper-app/app/app/pages/components/sources.vue:29-30, 78-82, 150`
- Modify: `packages/interloper-app/app/app/pages/components/[kind].vue:44-48, 203`
- Modify: `packages/interloper-app/app/app/composables/destinationBadge.ts`

**Interfaces:**
- Consumes: `RelationRef.dst_key` / `dst_name`; `relationRefs(record, name)` from `~/types/component`; `componentIcon(key, kind?)` from `~/utils/catalog` (check its signature and pass `dst_kind` if it takes one).

- [ ] **Step 1: jobs.vue**

```ts
componentsStore.fetchAll(['job'])
```

(remove `fetchRelations()`)

```ts
/** The relation refs a job targets, in relation order. */
function targetsOf(job: ComponentRecord): RelationRef[] {
    return relationRefs(job, 'targets')
}
```

Column: `accessorFn: row => targetsOf(row).map(t => t.dst_name ?? t.dst_key).join(', ')`; cell: `icon: componentIcon(first.dst_key, first.dst_kind)`, `label: first.dst_name ?? first.dst_key`. Import `RelationRef` and `relationRefs` (drop `relationIds` if unused). Retry: `@retry="componentsStore.fetchAll(['job'])"`.

- [ ] **Step 2: hooks.vue**

Same shape: `fetchAll(['hook'])`, `watchedBy(hook): RelationRef[] = relationRefs(hook, 'watches')`, labels from `dst_name ?? dst_key`, icon from `componentIcon(ref.dst_key, ref.dst_kind)`, retry `fetchAll(['hook'])`.

- [ ] **Step 3: destinations.vue**

```ts
componentsStore.fetchAll(['destination'])

/** The connection ref a destination is bound to, if any. */
function connectionOf(destination: ComponentRecord): RelationRef | undefined {
    return relationRefs(destination, 'connection')[0]
}
```

Accessor: `connectionOf(row)?.dst_name ?? connectionOf(row)?.dst_key ?? ''`; the cell reads `resource.dst_key` / `resource.dst_name` where it read `resource.key` / `resource.name`. Retry `fetchAll(['destination'])`. Remove the `resourceMap` import if unused.

- [ ] **Step 4: destinationBadge.ts and sources.vue**

```ts
/** What the badge needs of a destination: its catalog key and display name. */
export interface DestinationLike {
    key: string
    name: string | null
}

    function getBadgeForDestinations(destinations: DestinationLike[]): DestinationBadge | null { /* body unchanged */ }

    function getBadgeForSource(source: ComponentRecord): DestinationBadge | null {
        return getBadgeForDestinations(
            relationRefs(source, 'destinations').map(ref => ({ key: ref.dst_key, name: ref.dst_name })),
        )
    }
```

`getBadgeForAssetId` keeps its `byKind('source')` lookup. Import `relationRefs` instead of `relationIds`.

In `sources.vue`: `componentsStore.fetchAll(['source'])`, drop `fetchRelations()`, replace `destinationsOf` with

```ts
/** The destinations a source is bound to, as the badge reads them. */
function destinationsOf(source: ComponentRecord): DestinationLike[] {
    return relationRefs(source, 'destinations').map(ref => ({ key: ref.dst_key, name: ref.dst_name }))
}
```

and retry `fetchAll(['source'])`. Check the column that consumes `destinationsOf` reads only `key`/`name` (it hands the list to `getBadgeForDestinations`).

- [ ] **Step 5: [kind].vue**

```ts
// Only this kind: the delete preview and relation labels come from the API now.
componentsStore.fetchAll([kind.value])
watch(kind, () => componentsStore.fetchAll([kind.value]))
```

Retry: `@retry="componentsStore.fetchAll([kind.value])"`.

- [ ] **Step 6: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: PASS. Fix unused imports the linter reports.

---

## Task 10: Verification

**Files:**
- Create (scratchpad, not the repo): `<scratchpad>/bench_components.py`

- [ ] **Step 1: Full Python and frontend checks**

Run from the repo root: `make check`
Expected: ruff, ty, pytest, pnpm lint and nuxt typecheck all pass.

- [ ] **Step 2: Benchmark the list path on a synthetic organisation**

```python
"""Time the components list response build on an in-memory org: rows vs roots, and queries issued."""

import time
from uuid import uuid4

import interloper as il
from interloper_assets.demo.source import DemoSource
from sqlalchemy import event
from sqlalchemy.pool import StaticPool

from interloper_api.routes.components import ComponentResponse
from interloper_db import engine as engine_module
from interloper_db.store import Store

engine = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
from interloper_db.models import Component, ComponentRelation, Quota, Usage  # noqa: E402

for model in (Component, ComponentRelation, Quota, Usage):
    model.__table__.create(engine)

queries = [0]
event.listen(engine, "before_cursor_execute", lambda *a, **k: queries.__setitem__(0, queries[0] + 1))

store = Store(catalog=il.Catalog.from_assets([DemoSource]))
org = uuid4()
for i in range(100):
    store.components.create(org, kind="source", key=DemoSource.key, name=f"demo {i}", config={"dataset": f"ds_{i}"})

for label, rows in (("list_all (old shape)", store.components.list_all(org)), ("list_roots", store.components.list_roots(org))):
    queries[0] = 0
    started = time.perf_counter()
    payload = [ComponentResponse.from_row(row, store, include_config=False) for row in rows]
    print(f"{label}: {len(rows)} rows -> {len(payload)} items, {queries[0]} queries, {time.perf_counter() - started:.3f}s")
```

Run: `uv run python <scratchpad>/bench_components.py`
Expected: `list_all` issues on the order of one query per owned asset (500 for 100 demo sources of 5 assets); `list_roots` issues zero queries during the response build and returns 100 items. Record both lines for the PR description. If `DemoSource` rejects the `dataset` config or collides, use whatever distinct config the source-collision guard accepts (see `TestSourceCollisionGuard` in the store tests).

- [ ] **Step 3: Live check on the seeded dev instance**

Run: `INTERLOPER_SERVER_PORT=3100 make dev-up` (needs a `:3000` login session already established; see AGENTS.md). Then, with the `verify` skill or the browser:

- `/components/sources`, `/components/jobs`, `/components/destinations`, `/components/hooks`, `/components/connections`: each page issues exactly one `GET /components?kind=<kind>` on mount, rows render with relation labels, and the delete confirmation shows blocking/detaching referrers from `GET /components/delete-impact`.
- `/collection` and `/graph`: sources expand to their assets; assets appear once; the graph draws asset upstream edges.
- `/timeline`: job rows resolve their source and asset targets.
- `GET /api/components` returns no top-level row with a non-null `parent_id`.

Note anything that does not behave in the final report rather than patching around it.
