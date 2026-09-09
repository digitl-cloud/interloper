# Relation model, phase 1 (core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `Relation` primitive declares every link between components; `Component` gains the operations that use it (collect, bind, trickle, defaults, validation, definition, serialisation); each kind declares its anchor relations; assets infer theirs from `data()` and inject them; the DAG builds edges from bound instances and includes read-only upstreams; a manifest is `to_spec()` output with the structural parent rule. PR #321 is reworked on top of this and keeps its read path, `il.Upstream`, granularity check and `default_destination_key` handling.

**Architecture:** `component/relation.py` holds `Relation`, `ComponentIdentity` and the `Bound` descriptor. `component/base.py` owns every relation operation and the serialisation rule; kinds only declare. `Asset` is the one class with inference and injection. The DAG reads `upstream_relations()` and `bound()`. Nothing else knows about relations. Spec: `docs/superpowers/specs/2026-09-07-relation-model-design.md`, sections 2 to 7 and 10.

**Tech Stack:** Python 3.10+, pydantic v2, pytest, ruff, ty. No database work in this phase.

## Global Constraints

> Rulings during execution (2026-09-07): `Relation` is its own descriptor and `Bound` does not exist; the typed declaration form is `x: list[Destination] = Relation(...)` with a `TYPE_CHECKING`-only `__new__ -> Any` on `Relation`; `Resource` targets are always self-filling (environment-backed, read-time failure); commits use `--no-verify` for this phase; phase 2 stacks on this branch.

- Work on branch `feat/many-upstreams-core` (PR #321), rebased on `origin/main`. Squash as you go: the branch ends as a small set of `feat!:` commits. Push with `--force-with-lease`. Retitle the PR to `feat!: one Relation primitive for every component link` when the phase is done.
- Conventional Commits, `feat!:` for the breaking commits, messages end with `By Digitl`. Commit only when Guillaume has asked for commits in the session; otherwise leave work staged.
- Run Python from the repo root with `uv run --frozen ...` (never bare `uv run`).
- ruff line length 120; `uv run --frozen ruff check` and `uv run --frozen ty check` pass at the end of every task; every function carries full Google docstring sections (Args, Returns, Raises for own raises only, DOC502), private functions and dunders included.
- Tests mirror modules one to one: `tests/component/test_relation.py` for `component/relation.py`, existing files otherwise. No standalone `test_<feature>.py`.
- Comment sparingly, never restate the code, never explain other components. No em-dashes anywhere (code, docs, commit messages).
- `packages/interloper-core/src/interloper/` is the root of every core path below unless a path starts with `packages/`; `packages/interloper-core/tests/` for core tests.
- Retired names (spec section 10) must not survive in `packages/*/src`, `docs/`, or `plugins/` at the end of the phase, except where a later phase's plan explicitly owns the file (`interloper-db`, `-api`, `-toolkit`, `-agent`, `-scheduler`, `-app`).

---

### Task 1: The primitive: `Relation`, `ComponentIdentity`, `Bound`

**Files:**
- Create: `component/relation.py`
- Modify: `component/__init__.py` (export the three names)
- Test: `tests/component/test_relation.py`
- Read first: `asset/base.py:65-127` (`AssetIdentity`, whose `resolve` and `satisfies` move here), `resource/ref.py` (`ResourceRef`, whose descriptor mechanics `Bound` replaces; note the `IgnoredDescriptor` base it subclasses so pydantic ignores it)

**Interfaces:**
- Produces:
  ```py
  class ComponentIdentity(NamedTuple):
      source_key: str | None
      key: str
      @classmethod
      def of(cls, component: Component) -> ComponentIdentity
      @classmethod
      def resolve(cls, declared_key: str, *, own_source_key: str | None) -> ComponentIdentity
      def satisfies(self, declared_key: str, *, own_source_key: str | None) -> bool
      def __str__(self) -> str            # "source.asset" or "key"

  ANY_SOURCE = "*"

  class Relation(BaseModel):
      kind: str | list[str]
      key: str | list[str] = ""
      many: bool = False
      optional: bool = False
      default: Callable[[], Any] | None = None
      on_delete: Literal["block", "detach"] = "block"
      name: str = ""
      target: type | None = None          # the class when declared by class; excluded from dumps
      def __init__(self, kind_or_class: str | list[str] | type = "", key: str | list[str] = "", /, **data)
      def kinds(self) -> list[str]
      def keys(self) -> list[str]
      def accepts(self, kind: str, identity: ComponentIdentity, *, owner: ComponentIdentity) -> bool
      @property
      def self_filling(self) -> bool     # default set, or single-valued with a target whose fields are all optional
      def fallback(self) -> Any | None   # fresh default() or target() when self_filling, else None

  class Bound:                            # IgnoredDescriptor
      def __init__(self, relation: Relation)
      def __set_name__(self, owner, name)
      def __get__(self, instance, owner)  # class access: the Relation; instance: bound value(s) or None / []
      def __set__(self, instance, value)  # instance.bind(name, *values)
  ```

- [ ] **Step 1: Write the failing tests**

```python
# tests/component/test_relation.py
"""Tests for the relation primitive (``interloper.component.relation``)."""

from __future__ import annotations

import pytest

import interloper as il
from interloper.component.relation import ANY_SOURCE, Bound, ComponentIdentity, Relation


class Conn(il.Connection):
    """A connection class for shorthand tests."""


class TestComponentIdentity:
    def test_resolve_bare_key_uses_own_source(self) -> None:
        assert ComponentIdentity.resolve("orders", own_source_key="shop") == ComponentIdentity("shop", "orders")

    def test_resolve_qualified_key(self) -> None:
        assert ComponentIdentity.resolve("shop.orders", own_source_key="other") == ComponentIdentity("shop", "orders")

    def test_resolve_wildcard(self) -> None:
        assert ComponentIdentity.resolve("*.campaigns", own_source_key="x") == ComponentIdentity(ANY_SOURCE, "campaigns")

    def test_satisfies_exact_qualified(self) -> None:
        assert ComponentIdentity("shop", "orders").satisfies("shop.orders", own_source_key=None)
        assert not ComponentIdentity("shop", "orders").satisfies("shop.items", own_source_key=None)

    def test_satisfies_wildcard_matches_any_source(self) -> None:
        assert ComponentIdentity("fb", "campaigns").satisfies("*.campaigns", own_source_key="matcher")
        assert not ComponentIdentity("fb", "ads").satisfies("*.campaigns", own_source_key="matcher")

    def test_satisfies_bare_key_for_non_asset(self) -> None:
        assert ComponentIdentity(None, "bigquery_destination").satisfies("bigquery_destination", own_source_key=None)

    def test_str(self) -> None:
        assert str(ComponentIdentity("shop", "orders")) == "shop.orders"
        assert str(ComponentIdentity(None, "bq")) == "bq"


class TestRelation:
    def test_class_shorthand_sets_kind_key_and_target(self) -> None:
        relation = Relation(Conn)
        assert relation.kind == "connection"
        assert relation.key == "conn"
        assert relation.target is Conn

    def test_string_form(self) -> None:
        relation = Relation("asset", "*.campaigns", many=True)
        assert relation.kinds() == ["asset"]
        assert relation.keys() == ["*.campaigns"]
        assert relation.many is True

    def test_list_kinds_and_keys(self) -> None:
        relation = Relation(["source", "asset"], ["a", "b"])
        assert relation.kinds() == ["source", "asset"]
        assert relation.keys() == ["a", "b"]

    def test_accepts_checks_kind(self) -> None:
        relation = Relation("destination")
        owner = ComponentIdentity(None, "shop")
        assert relation.accepts("destination", ComponentIdentity(None, "bq"), owner=owner)
        assert not relation.accepts("connection", ComponentIdentity(None, "bq"), owner=owner)

    def test_accepts_empty_key_takes_any_key_of_the_kind(self) -> None:
        relation = Relation("destination")
        assert relation.accepts("destination", ComponentIdentity(None, "anything"), owner=ComponentIdentity(None, "s"))

    def test_accepts_any_listed_key(self) -> None:
        relation = Relation("destination", ["bq", "gcs"])
        owner = ComponentIdentity(None, "shop")
        assert relation.accepts("destination", ComponentIdentity(None, "gcs"), owner=owner)
        assert not relation.accepts("destination", ComponentIdentity(None, "s3"), owner=owner)

    def test_accepts_bare_asset_key_means_owner_source(self) -> None:
        relation = Relation("asset", "campaigns")
        owner = ComponentIdentity("fb", "stats")
        assert relation.accepts("asset", ComponentIdentity("fb", "campaigns"), owner=owner)
        assert not relation.accepts("asset", ComponentIdentity("tt", "campaigns"), owner=owner)

    def test_self_filling_and_fallback(self) -> None:
        class Cfg(il.Config):
            threshold: int = il.InputField(default=1)

        class Needy(il.Config):
            token: str = il.InputField()

        assert Relation(Cfg).self_filling is True
        assert isinstance(Relation(Cfg).fallback(), Cfg)
        assert Relation(Needy).self_filling is False
        assert Relation(Needy).fallback() is None
        assert Relation(Needy, default=lambda: Needy(token="t")).self_filling is True
        assert Relation("destination", many=True).self_filling is False

    def test_dump_excludes_target_and_default(self) -> None:
        dumped = Relation(Conn, default=Conn).model_dump(mode="json")
        assert dumped == {"kind": "connection", "key": "conn", "many": False, "optional": False, "on_delete": "block", "name": ""}


class TestBound:
    def test_class_access_returns_relation(self) -> None:
        class Owner:
            conn = Bound(Relation(Conn))

        assert isinstance(Owner.conn, Relation)
        assert Owner.conn.name == "conn"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/component/test_relation.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'interloper.component.relation'`

- [ ] **Step 3: Write the module**

```python
# component/relation.py
"""The relation primitive: what a component declares about the components it links to."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field

from interloper.utils.descriptors import IgnoredDescriptor  # the base ResourceRef uses today; move it here if it lives in resource/ref.py

if TYPE_CHECKING:
    from interloper.component.base import Component

ANY_SOURCE = "*"


class ComponentIdentity(NamedTuple):
    """What a component is, for relation matching: its owning source key (assets) and its key."""

    source_key: str | None
    key: str

    @classmethod
    def of(cls, component: Component) -> ComponentIdentity:
        parent = component.parent
        return cls(parent.key if parent is not None else None, component.key)

    @classmethod
    def resolve(cls, declared_key: str, *, own_source_key: str | None) -> ComponentIdentity:
        source_key, dot, key = declared_key.rpartition(".")
        if not dot:
            return cls(own_source_key, declared_key)
        return cls(source_key, key)

    def satisfies(self, declared_key: str, *, own_source_key: str | None) -> bool:
        expected = ComponentIdentity.resolve(declared_key, own_source_key=own_source_key)
        if expected.key != self.key:
            return False
        if expected.source_key in (None, ANY_SOURCE):
            return self.source_key is None or expected.source_key == ANY_SOURCE
        return expected.source_key == self.source_key

    def __str__(self) -> str:
        return f"{self.source_key}.{self.key}" if self.source_key else self.key


class Relation(BaseModel):
    """One declared link from an owner component to the components that may fill it."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: str | list[str]
    key: str | list[str] = ""
    many: bool = False
    optional: bool = False
    default: Callable[[], Any] | None = Field(default=None, exclude=True)
    on_delete: Literal["block", "detach"] = "block"
    name: str = ""
    target: type | None = Field(default=None, exclude=True)

    def __init__(self, kind_or_class: str | list[str] | type = "", key: str | list[str] = "", /, **data: Any) -> None:
        if isinstance(kind_or_class, type):
            data.update(kind=kind_or_class.kind, key=kind_or_class.key, target=kind_or_class)
        else:
            data.update(kind=data.get("kind", kind_or_class), key=data.get("key", key))
        super().__init__(**data)

    def kinds(self) -> list[str]:
        return [self.kind] if isinstance(self.kind, str) else list(self.kind)

    def keys(self) -> list[str]:
        if not self.key:
            return []
        return [self.key] if isinstance(self.key, str) else list(self.key)

    def accepts(self, kind: str, identity: ComponentIdentity, *, owner: ComponentIdentity) -> bool:
        if kind not in self.kinds():
            return False
        keys = self.keys()
        if not keys:
            return True
        return any(identity.satisfies(declared, own_source_key=owner.source_key) for declared in keys)

    @property
    def self_filling(self) -> bool:
        if self.default is not None:
            return True
        if self.many or self.target is None:
            return False
        fields = getattr(self.target, "model_fields", {})
        return all(not field.is_required() for name, field in fields.items() if name != "id")

    def fallback(self) -> Any | None:
        if self.default is not None:
            return self.default()
        if self.self_filling and self.target is not None:
            return self.target()
        return None


class Bound(IgnoredDescriptor):
    """Descriptor installed for each relation: the Relation on the class, the bound value(s) on an instance."""

    def __init__(self, relation: Relation) -> None:
        self.relation = relation

    def __set_name__(self, owner: type, name: str) -> None:
        self.relation.name = name

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self.relation
        return instance.bound(self.relation.name)

    def __set__(self, instance: Any, value: Any) -> None:
        targets = value if isinstance(value, (list, tuple)) else ([] if value is None else [value])
        instance._bound.pop(self.relation.name, None)
        if targets:
            instance.bind(self.relation.name, *targets)
```

Write full Google docstrings on every function (the sketch above omits them for brevity; the file must not). `satisfies` for a non-asset identity (`source_key is None`) matches an exact key only; the wildcard form applies to assets.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen pytest packages/interloper-core/tests/component/test_relation.py -q`
Expected: PASS (the `TestBound` test passes because `bound()` is not called on class access).

- [ ] **Step 5: Lint and type check, then commit**

Run: `uv run --frozen ruff check packages/interloper-core && uv run --frozen ty check`

```bash
git add packages/interloper-core/src/interloper/component/relation.py packages/interloper-core/src/interloper/component/__init__.py packages/interloper-core/tests/component/test_relation.py
git commit -m "feat(core): add the Relation primitive, ComponentIdentity and the Bound descriptor

By Digitl"
```

---

### Task 2: `Component` declares and binds relations

Replaces `resource_types`, `resources`, `ResourceRef`, `relation_types`, `RelationDefinition`, `Dependency`, `_infer_resource_refs`, `relation_definitions` and the kwarg routing in `__init__`.

**Files:**
- Modify: `component/base.py` (whole class body; keep `KINDS`, `_adopt_kind`, discriminator, `anchor`, `resolve_key`, `__str__`)
- Delete: `resource/ref.py`; update `resource/__init__.py`
- Modify: `component/__init__.py`, `__init__.py` (exports; `il.Relation`, `il.ComponentIdentity` in, `il.Dependency`, `il.RelationDefinition`, `il.ResourceRef` out)
- Test: `tests/component/test_base.py` (rewrite the relation tests), delete `tests/resource/test_ref.py` if present

**Interfaces:**
- Produces on `Component`:
  ```py
  relations: ClassVar[dict[str, Relation]]         # merged, name -> Relation, Bound installed per entry
  _bound: dict[str, list[Component]]               # PrivateAttr; single relations hold a one-item list
  _parent: Component | None                        # PrivateAttr
  parent -> Component | None                       # property; setter used by Source
  def __init__(self, /, **data)                    # relation names accepted as kwargs; nothing is bound implicitly
  @classmethod def collect(cls) -> None            # called from __init_subclass__
  def bind(self, name: str, *targets: Component) -> None
  def unbind(self, name: str, *targets: Component) -> None
  def bound(self, name: str) -> Component | list[Component] | None
  def bound_ids(self) -> dict[str, list[str]]
  ```
- `collect()` precedence: `relations` classvar declared on the class body (from the decorator or written by hand) wins, then `Relation` class attributes, then annotations whose value is a `Component` subclass. A subclass entry replaces the parent's entry of the same name. Annotated relations are removed from pydantic's fields (as `_infer_resource_refs` does today).
- `bind` raises `ConfigError` when `relation.accepts` is false or a single relation would hold two targets; `unbind` raises `ConfigError` when it would empty a non-optional relation.

- [ ] **Step 1: Write the failing tests**

Replace the resource and relation tests in `tests/component/test_base.py` with:

```python
class Conn(il.Connection):
    """Connection for binding tests."""


class Cfg(il.Config):
    """Config for binding tests."""

    threshold: int = il.InputField(default=1)


class Dest(il.Destination):
    """Destination declaring its connection by annotation."""

    connection: Conn


class Widget(il.Source):
    """Source with an annotated connection, an explicit optional config, and a self-filling config."""

    connection: Conn
    config = il.Relation(Cfg, optional=True)
    fallback = il.Relation(Cfg)          # Cfg has only defaulted fields: self-filling, never bound


class TestCollect:
    def test_annotation_becomes_relation(self) -> None:
        assert Widget.relations["connection"].kind == "connection"
        assert Widget.relations["connection"].key == "conn"
        assert "connection" not in Widget.model_fields

    def test_relation_attribute_keeps_flags(self) -> None:
        assert Widget.relations["config"].optional is True
        assert Widget.config.name == "config"

    def test_anchor_relations_inherited(self) -> None:
        assert Widget.relations["destinations"].many is True

    def test_subclass_replaces_same_name(self) -> None:
        class Narrow(Widget):
            connection = il.Relation(Conn, optional=True)

        assert Narrow.relations["connection"].optional is True
        assert Widget.relations["connection"].optional is False


class TestBind:
    def test_kwargs_bind(self) -> None:
        conn = Conn()
        widget = Widget(connection=conn)
        assert widget.connection is conn
        assert widget.bound("connection") is conn
        assert widget.bound_ids()["connection"] == [conn.id]

    def test_unbound_single_is_none_and_many_is_empty(self) -> None:
        widget = Widget(connection=Conn())
        assert widget.config is None
        assert widget.destinations == []

    def test_self_filling_relation_is_not_bound_but_resolves(self) -> None:
        widget = Widget(connection=Conn())
        assert widget.fallback is None
        assert isinstance(widget.resolve("fallback"), Cfg)
        assert "fallback" not in widget.bound_ids()

    def test_missing_required_is_a_build_error(self) -> None:
        with pytest.raises(ConfigError, match="connection"):
            Widget()

    def test_wrong_kind_rejected(self) -> None:
        with pytest.raises(ConfigError, match="connection"):
            Widget(connection=Cfg())  # type: ignore[arg-type]

    def test_single_relation_rejects_second_target(self) -> None:
        widget = Widget(connection=Conn())
        with pytest.raises(ConfigError, match="single"):
            widget.bind("connection", Conn())

    def test_many_accumulates(self) -> None:
        widget = Widget(connection=Conn())
        first, second = Dest(connection=Conn()), Dest(connection=Conn())
        widget.bind("destinations", first)
        widget.bind("destinations", second)
        assert widget.destinations == [first, second]

    def test_unbind_detaches_optional(self) -> None:
        cfg = Cfg()
        widget = Widget(connection=Conn(), config=cfg)
        widget.unbind("config", cfg)
        assert widget.config is None

    def test_unbind_refuses_to_empty_required(self) -> None:
        conn = Conn()
        widget = Widget(connection=conn)
        with pytest.raises(ConfigError, match="non-optional"):
            widget.unbind("connection", conn)

    def test_unknown_kwarg_is_a_type_error(self) -> None:
        with pytest.raises(TypeError, match="unexpected"):
            Widget(connection=Conn(), nope=1)
```

Keep the existing discriminator, anchor and `resolve_key` tests.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/component/test_base.py -q`
Expected: FAIL (`Widget` has no `relations`, `il.Relation` missing from exports).

- [ ] **Step 3: Rewrite `Component`**

Key pieces (full docstrings in the file):

```python
class Component(Serializable):
    kind: ClassVar[str]
    relations: ClassVar[dict[str, Relation]] = {}
    id: str = ""
    _bound: dict[str, list[Component]] = PrivateAttr(default_factory=dict)
    _parent: Component | None = PrivateAttr(default=None)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.collect()

    @classmethod
    def collect(cls) -> None:
        inherited: dict[str, Relation] = {}
        for base in reversed(cls.__mro__[1:]):
            inherited.update(getattr(base, "relations", {}) or {})
        own: dict[str, Relation] = {}
        annotations = cls.__dict__.get("__annotations__", {})
        for name, hint in list(annotations.items()):
            resolved = _resolve_hint(hint)   # handles "Conn", Conn, Conn | None (Optional makes it optional)
            if resolved is not None and issubclass(resolved.type, Component):
                own[name] = Relation(resolved.type, optional=resolved.optional)
        for name, value in list(cls.__dict__.items()):
            if isinstance(value, Relation):
                own[name] = value
        declared = cls.__dict__.get("relations")
        if isinstance(declared, dict):
            own.update(declared)
        cls.relations = {**inherited, **own}
        for name, relation in cls.relations.items():
            relation = relation.model_copy(update={"name": name})
            cls.relations[name] = relation
            setattr(cls, name, Bound(relation))
            annotations.pop(name, None)
            cls.model_fields.pop(name, None)
        cls.model_rebuild(force=True)
```

`_resolve_hint` unwraps `X | None` and `Optional[X]` to `(X, optional=True)`, forward-reference strings through the module namespace; anything that is not a `Component` subclass is left as a pydantic field. Because `__init_subclass__` runs before pydantic finishes the class in some paths, keep the same `__pydantic_init_subclass__` hook the current `_infer_resource_refs` uses and move the field removal there if needed (the existing code shows which hook works).

```python
    def __init__(self, /, **data: Any) -> None:
        cls = type(self)
        targets = {name: data.pop(name) for name in list(data) if name in cls.relations}
        unknown = [name for name in data if name not in cls.model_fields]
        if unknown:
            raise TypeError(f"{cls.__name__} got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        super().__init__(**data)
        for name, value in targets.items():
            if value is None:
                continue
            self.bind(name, *(value if isinstance(value, (list, tuple)) else [value]))
        self.validate_relations() if not type(self)._defer_validation else None

    def bind(self, name: str, *targets: Component) -> None:
        relation = self._relation(name)
        owner = self.identity
        for target in targets:
            if not relation.accepts(target.kind, target.identity, owner=owner):
                raise ConfigError(
                    f"{type(self).__name__}.{name} does not accept {target.kind} '{target.qualified_key}' "
                    f"(declared: kind {relation.kinds()}, key {relation.keys() or 'any'})"
                )
        current = self._bound.get(name, [])
        if relation.many:
            self._bound[name] = current + [t for t in targets if all(t is not c for c in current)]
        else:
            if len(targets) > 1 or (current and current[0] is not targets[0]):
                raise ConfigError(f"{type(self).__name__}.{name} is single-valued; unbind before binding another target")
            self._bound[name] = list(targets)

    def unbind(self, name: str, *targets: Component) -> None:
        relation = self._relation(name)
        remaining = [t for t in self._bound.get(name, []) if all(t is not r for r in targets)]
        if not remaining and not relation.optional and self._bound.get(name):
            raise ConfigError(f"{type(self).__name__}.{name} is non-optional and cannot be emptied")
        self._bound[name] = remaining

    def bound(self, name: str) -> Component | list[Component] | None:
        relation = self._relation(name)
        values = self._bound.get(name, [])
        if relation.many:
            return list(values)
        return values[0] if values else None

    def bound_ids(self) -> dict[str, list[str]]:
        return {name: [t.id for t in targets] for name, targets in self._bound.items() if targets}
```

`_relation(name)` raises `KeyError` with the declared names for an unknown name. `validate_relations`, `trickle`, `definition`, `to_spec` come in Tasks 3 and 7; for this task add `parent` (property over `_parent` with a setter), `identity` (`ComponentIdentity.of(self)`), `qualified_key` (`str(self.identity)`), `_defer_validation: ClassVar[bool] = False`, `resolve(name)` (`self.bound(name)` if bound else `relation.fallback()`), and a minimal `validate_relations()` that raises `ConfigError` naming every unbound non-optional relation that is not `self_filling`.

Delete `Dependency`, `RelationDefinition`, `resource_types`, `relation_types`, `resources`, `trickle_resources`, `_infer_resource_refs`, `_check_relation_fields`, `relation_definitions`. `ComponentDefinition.relations` becomes `dict[str, Relation]`. Remove `resource/ref.py` and every import of `ResourceRef`. Update `__init__.py` exports.

`Connection` classes with required credential fields (the OAuth connections, see memory `oauth-credential-fields-required`) are not self-filling, so a source whose `connection` is unbound fails validation at build; a `Config` with only defaulted fields is self-filling and keeps working unbound, as today.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen pytest packages/interloper-core/tests/component -q`
Expected: PASS. Other suites fail at import until Tasks 4 and 5; that is expected here.

- [ ] **Step 5: Commit**

```bash
git add -A packages/interloper-core/src/interloper/component packages/interloper-core/src/interloper/resource packages/interloper-core/src/interloper/__init__.py packages/interloper-core/tests/component packages/interloper-core/tests/resource
git commit -m "feat!(core): Component declares and binds relations through one Relation map

By Digitl"
```

---

### Task 3: Trickle, defaults, validation and definition

**Files:**
- Modify: `component/base.py`
- Test: `tests/component/test_base.py`

**Interfaces:**
- Produces on `Component`:
  ```py
  def trickle(self, child: Component) -> None
  def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None
  @classmethod def definition(cls) -> ComponentDefinition   # relations exported as dict[str, Relation]
  ```
- `__init__` calls `validate_relations()` with `nodes=None` unless the class sets `_defer_validation` (assets, Task 5), so a source can trickle first and validate afterwards.
- `trickle(child)`: for each child relation still unbound, with the same name as one of the parent's bound relations, bind every parent target the child relation `accepts`. Never overrides an existing child binding.
- `validate_relations(nodes)` raises `ConfigError` (unbound non-optional without a fallback, single with several, identity mismatch) and, when `nodes` is given, for a bound `asset`-kind target whose id is not in `nodes` unless the relation is optional.

- [ ] **Step 1: Write the failing tests**

```python
class Child(il.Asset):
    """Asset declaring a connection relation."""

    connection = il.Relation(Conn, optional=True)

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class TestTrickle:
    def test_fills_unbound_same_name(self) -> None:
        conn = Conn()
        parent = Widget(connection=conn)
        child = Child()
        parent.trickle(child)
        assert child.connection is conn

    def test_never_overrides_explicit_binding(self) -> None:
        own = Conn()
        parent = Widget(connection=Conn())
        child = Child(connection=own)
        parent.trickle(child)
        assert child.connection is own

    def test_skips_targets_the_child_rejects(self) -> None:
        class Other(il.Connection):
            """Another connection kind."""

        class Picky(il.Asset):
            connection = il.Relation(Other, optional=True)

            def data(self, context: il.ExecutionContext) -> list[dict]:
                return []

        parent = Widget(connection=Conn())
        child = Picky()
        parent.trickle(child)
        assert child.connection is None


class TestValidateRelations:
    def test_identity_mismatch_reported(self) -> None:
        widget = Widget(connection=Conn())
        widget._bound["connection"] = [Cfg()]  # bypass bind to simulate a stale binding
        with pytest.raises(ConfigError, match="connection"):
            widget.validate_relations()

    def test_asset_target_must_be_a_node_when_nodes_given(self) -> None:
        class Up(il.Asset):
            def data(self, context: il.ExecutionContext) -> list[dict]:
                return []

        class Down(il.Asset):
            up = il.Relation("asset", "up")

            def data(self, context: il.ExecutionContext, up: il.Upstream) -> list[dict]:
                return []

        up, down = Up(), Down(up=up)
        down.validate_relations({up.id: up})
        with pytest.raises(ConfigError, match="not in the DAG"):
            down.validate_relations({})


class TestDefinition:
    def test_relations_exported(self) -> None:
        relations = Widget.definition().relations
        assert relations["connection"].model_dump(mode="json") == {
            "kind": "connection", "key": "conn", "many": False, "optional": False, "on_delete": "block", "name": "connection",
        }
        assert relations["destinations"].many is True
```

(The `Down` test needs Task 5's asset inference to accept `up: il.Upstream`; write it now, it passes after Task 5.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/component/test_base.py -q -k "Trickle or Validate or Definition"`
Expected: FAIL with `AttributeError: 'Widget' object has no attribute 'trickle'`

- [ ] **Step 3: Implement**

```python
    def trickle(self, child: Component) -> None:
        for name, relation in type(child).relations.items():
            if child._bound.get(name) or name not in self._bound:
                continue
            accepted = [
                t for t in self._bound[name] if relation.accepts(t.kind, t.identity, owner=child.identity)
            ]
            if accepted:
                child.bind(name, *(accepted if relation.many else accepted[:1]))

    def resolve(self, name: str) -> Component | list[Component] | None:
        bound = self.bound(name)
        if bound is None or bound == []:
            return self._relation(name).fallback() if not self._relation(name).many else []
        return bound

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        problems: list[str] = []
        for name, relation in type(self).relations.items():
            targets = self._bound.get(name, [])
            if not targets:
                if not relation.optional and not relation.self_filling:
                    problems.append(f"'{name}' is unbound and non-optional")
                continue
            if not relation.many and len(targets) > 1:
                problems.append(f"'{name}' is single-valued but holds {len(targets)} targets")
            for target in targets:
                if not relation.accepts(target.kind, target.identity, owner=self.identity):
                    problems.append(f"'{name}' holds {target.kind} '{target.qualified_key}', which it does not accept")
                elif nodes is not None and "asset" in relation.kinds() and target.kind == "asset" and target.id not in nodes:
                    if not relation.optional:
                        problems.append(f"'{name}' points at asset '{target.qualified_key}' which is not in the DAG")
        if problems:
            raise ConfigError(f"{type(self).__name__} '{self.qualified_key}': " + "; ".join(problems))
```

`definition()` passes `relations=dict(cls.relations)`.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/component -q`
Expected: PASS except `test_asset_target_must_be_a_node_when_nodes_given` until Task 5.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/component/base.py packages/interloper-core/tests/component/test_base.py
git commit -m "feat(core): trickle, defaults, relation validation and definition on Component

By Digitl"
```

---

### Task 4: Kinds declare their anchor relations

**Files:**
- Modify: `destination/base.py:48-70` (drop `relation_types`, `resource_types`; `validate_fetch_field_providers(cls, cls.relations)`), `destination/decorator.py:80-95` (`relations=` replaces `resources=`)
- Modify: `source/base.py` (`destinations = Relation("destination", many=True, optional=True)`; drop `destination_types`, `destinations` field, `_coerce_destinations`, `relation_definitions`, `_infer_upstreams`, `_resolve_upstreams`, `trickle_resources` calls; add `sibling_bindings()` and `_bind_siblings()`; `_resolve` uses `trickle`), `source/decorator.py:15-30` (`relations=`, `destinations=` sets the `destinations` relation's key list)
- Modify: `job/base.py:45-96` (`targets`, `destinations` relations; cascade through `trickle`), `hook/base.py:67-76` (`watches`), `hook/trigger.py:29-48` (`targets`)
- Modify: `resource/fields.py:66-116` (`validate_fetch_field_providers(cls, relations: dict[str, Relation])` looks up `relations[slot].target`)
- Modify: `catalog/base.py:87-110` (`vocabulary` returns `dict[str, Relation]`), `catalog/base.py:255-275` (`_dependencies_of` collects `relation.target` for every relation with a target)
- Test: `tests/source/test_base.py`, `tests/source/test_decorator.py`, `tests/destination/test_base.py`, `tests/job/test_base.py`, `tests/hook/test_base.py`, `tests/resource/test_fields.py`, `tests/catalog/test_base.py`

**Interfaces:**
- Produces:
  ```py
  Source.relations["destinations"] == Relation("destination", many=True, optional=True)
  Source.sibling_bindings() -> dict[str, dict[str, str]]        # asset key -> {relation name: sibling asset key}
  Source._bind_siblings(self) -> None
  Job.relations["targets"] == Relation(["source", "asset"], many=True, optional=True, on_delete="detach")
  Job.relations["destinations"] == Relation("destination", many=True, optional=True)
  Hook.relations["watches"] == Relation(["source", "asset", "job"], many=True, optional=True, on_delete="detach")
  TriggerHook.relations["targets"] == Relation(["source", "asset", "job"], many=True, optional=True)
  @il.source(relations={...}, destinations=[BigQueryDestination])   # destinations narrows the key list
  @il.destination(relations={...})
  Catalog.vocabulary(kind, key, parent_key=None) -> dict[str, Relation]
  ```
- `internal_fields` lose `destinations`, `targets`, `watches`, `resources` (no longer pydantic fields). `Source.internal_fields` keeps `assets`, `normalizer`, `select`.
- `Source.model_post_init`: build assets, set `asset.parent = self`, `_resolve()` (dataset, table, defaults, `self.trickle(asset)`), `_bind_siblings()`, then `asset.validate_relations()` for each asset (assets defer validation in `__init__`, see Task 5).

- [ ] **Step 1: Write the failing tests**

In `tests/source/test_base.py` add:

```python
class TestSourceRelations:
    def test_anchor_declares_destinations(self) -> None:
        relation = il.Source.relations["destinations"]
        assert (relation.kind, relation.many, relation.optional) == ("destination", True, True)

    def test_decorator_destinations_narrows_keys(self) -> None:
        @il.source(destinations=[MemoryDestinationClassUsedInTests])
        class Narrow(il.Source):
            pass

        assert Narrow.relations["destinations"].keys() == [MemoryDestinationClassUsedInTests.key]

    def test_connection_trickles_to_assets(self) -> None:
        conn = Conn()
        source = Shop(connection=conn)   # Shop: connection: Conn; asset orders(self, context, connection: Conn)
        assert source.orders.connection is conn

    def test_sibling_bindings_and_bind(self) -> None:
        assert Shop.sibling_bindings() == {"revenue": {"orders": "orders"}}
        source = Shop(connection=Conn())
        assert source.revenue.orders is source.orders

    def test_cross_source_key_stays_unbound(self) -> None:
        source = Finance()   # revenue: relations={"orders": il.Relation("asset", "shop.orders")}
        assert source.revenue.bound("orders") is None
```

Add matching tests for `Job` (targets/destinations relations, `trickle` to targets), `Hook`/`TriggerHook`, `Destination` (`connection` annotation, fetch provider validation reads relations), and in `tests/catalog/test_base.py` that `vocabulary()` returns `Relation` objects and that the closure includes a relation's `target` class.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/source packages/interloper-core/tests/job packages/interloper-core/tests/hook packages/interloper-core/tests/destination packages/interloper-core/tests/catalog -q`
Expected: FAIL (import errors on `relation_types`, missing `relations` entries).

- [ ] **Step 3: Implement**

Source anchor:

```python
class Source(Component, Workload):
    destinations = Relation("destination", many=True, optional=True)
    asset_types: ClassVar[list[type[Asset]]] = []
    internal_fields: ClassVar[frozenset[str]] = frozenset({"assets", "normalizer", "select"})

    @classmethod
    def sibling_bindings(cls) -> dict[str, dict[str, str]]:
        siblings = {asset_cls.key for asset_cls in cls.asset_types}
        bindings: dict[str, dict[str, str]] = {}
        for asset_cls in cls.asset_types:
            for name, relation in asset_cls.relations.items():
                if "asset" not in relation.kinds():
                    continue
                for declared in relation.keys():
                    expected = ComponentIdentity.resolve(declared, own_source_key=cls.key)
                    if expected.source_key == cls.key and expected.key in siblings and expected.key != asset_cls.key:
                        bindings.setdefault(asset_cls.key, {})[name] = expected.key
        return bindings

    def _bind_siblings(self) -> None:
        by_key = {asset.key: asset for asset in self.assets}
        for asset_key, names in type(self).sibling_bindings().items():
            asset = by_key.get(asset_key)
            if asset is None:
                continue
            for name, sibling_key in names.items():
                if not asset._bound.get(name) and sibling_key in by_key:
                    asset.bind(name, by_key[sibling_key])
```

`_resolve` keeps dataset, table, `default_destination_key`, normalizer and strategy defaults and replaces `asset.destinations = list(self.destinations)` and `trickle_resources` with `self.trickle(asset)`; the destination loop calls `self.trickle(destination)`. Every `bind` on a source re-trickles: override `bind` to call `super().bind` then `for asset in self.assets: self.trickle(asset)`.

Decorators: `_SOURCE_PARAMS` maps `"relations"` to a classvar and `"destinations"` to a helper that sets `relations["destinations"] = Relation("destination", key=[d.key for d in destinations], many=True, optional=True)`. Remove `"resources"`. Same for `@il.destination(relations=...)`.

Job:

```python
class Job(Component, Workload):
    targets = Relation(["source", "asset"], many=True, optional=True, on_delete="detach")
    destinations = Relation("destination", many=True, optional=True)

    def operations(self) -> list[Operation]:
        return [operation for target in self.targets for operation in target.operations()]

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        for target in self.targets:
            self.trickle(target)
        for destination in self.destinations:
            self.trickle(destination)
```

`trickle(target)` on a source target fills its `destinations` (same name, accepted) which then re-trickles into its assets through the overridden `Source.bind`.

`validate_fetch_field_providers(cls, relations)`: `relation = relations.get(slot)`; `resource_cls = relation.target if relation else None`; error text says "which is not a declared relation".

Catalog `_dependencies_of`: `[r.target for r in component.relations.values() if r.target is not None]`, plus for sources the recursion into `asset_types` stays.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/source packages/interloper-core/tests/job packages/interloper-core/tests/hook packages/interloper-core/tests/destination packages/interloper-core/tests/catalog packages/interloper-core/tests/resource -q`
Expected: PASS for everything not involving asset inference (Task 5).

- [ ] **Step 5: Commit**

```bash
git add -A packages/interloper-core/src/interloper packages/interloper-core/tests
git commit -m "feat!(core): kinds declare anchor relations; sources bind siblings and trickle

By Digitl"
```

---

### Task 5: Asset inference, injection and the read path

**Files:**
- Modify: `asset/base.py` (drop `AssetIdentity`, `depends_on`, `upstreams`, `_coerce_upstreams`, `destination_types`, `destinations` field, `_validate_destinations`, `_infer_resource_types`, `declared_upstreams`, `sibling_upstreams`, `relation_definitions`, `validate_upstreams`, `_resolve_resource`, `_resolve_destinations`; add `collect()`, `_defer_validation`, `upstream_relations()`; rewrite `_build_kwargs`, `__call__`, `_read_destination`, `_validate_destination`, `identity`)
- Modify: `asset/decorator.py` (`relations=` replaces `resources=` and `depends_on=`; `destinations=` sets the key list)
- Modify: `operation/base.py` (drop `upstreams`, `declared_upstreams`, `validate_upstreams`; add `upstream_relations()`)
- Modify: `asset/__init__.py`, `__init__.py` (drop `AssetIdentity`)
- Test: `tests/asset/test_base.py`, `tests/asset/test_decorator.py`, `tests/operation/test_base.py`, `tests/asset/test_upstream.py` (unchanged content, verify)

**Interfaces:**
- Produces:
  ```py
  Asset._defer_validation: ClassVar[bool] = True          # Source validates after trickle; standalone assets validate in DAG
  @classmethod Asset.collect(cls) -> None                  # generic + data() signature inference
  Operation.upstream_relations(self) -> dict[str, Relation]   # relations with "asset" in kinds()
  Asset.__call__(*, id=None, materializable=None, dataset=None, default_destination_key=None,
                 materialization_strategy=None, normalizer=_UNSET, **relations) -> Self
  ```
- Inference rules (spec 5.1). Reserved parameter names: `self`, `context`, `source`, `kwargs`. A parameter with any other annotation, or none, raises `TypeError` at class creation: `"<Class>.data() parameter '<name>' is neither a Component class nor il.Upstream; nothing can fill it"`.
- Injection: `context`, `source` as today; `asset`-kind relation: `list[Upstream]` when `many`, else the single `Upstream` or `None`; other relations: `self.resolve(name)`, so an unbound config with defaulted fields is instantiated fresh at read time exactly as `_resolve_resource` step 4 does today, and trickle from the source or job (which happened earlier) always wins.

- [ ] **Step 1: Write the failing tests**

In `tests/asset/test_base.py`:

```python
class TestInference:
    def test_component_annotation_becomes_relation(self) -> None:
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, connection: Conn) -> list[dict]:
                return []

        assert A.relations["connection"].key == "conn"
        assert A.relations["connection"].optional is False

    def test_none_default_makes_optional(self) -> None:
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, config: Cfg | None = None) -> list[dict]:
                return []

        assert A.relations["config"].optional is True

    def test_upstream_annotation_is_bare_asset_key(self) -> None:
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:
                return []

        relation = A.relations["orders"]
        assert (relation.kind, relation.key, relation.many) == ("asset", "orders", False)

    def test_list_upstream_is_many(self) -> None:
        class A(il.Asset):
            def data(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]:
                return []

        assert A.relations["campaigns"].many is True

    def test_unknown_parameter_is_a_definition_error(self) -> None:
        with pytest.raises(TypeError, match="nothing can fill it"):
            class A(il.Asset):
                def data(self, context: il.ExecutionContext, x: str) -> list[dict]:
                    return []

    def test_explicit_relations_win(self) -> None:
        class A(il.Asset):
            campaigns = il.Relation("asset", "*.campaigns", many=True)

            def data(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]:
                return []

        assert A.relations["campaigns"].key == "*.campaigns"


class TestInjection:
    def test_resource_and_upstream_injected(self) -> None:
        seen: dict[str, Any] = {}

        @il.source
        class Shop(il.Source):
            connection: Conn

            @il.asset(partitioning=PARTITION)
            def orders(self, context: il.ExecutionContext) -> list[dict]:
                return [{"date": context.partition_date, "id": "o1"}]

            @il.asset(partitioning=PARTITION)
            def revenue(self, context: il.ExecutionContext, connection: Conn, orders: il.Upstream) -> list[dict]:
                seen.update(connection=connection, orders=orders)
                return [{"date": context.partition_date, "n": len(orders.data or [])}]

        conn, memory = Conn(), il.MemoryDestination()
        shop = Shop(connection=conn, destinations=[memory])
        il.DAG(shop).materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
        assert seen["connection"] is conn
        assert seen["orders"].asset is shop.orders
        assert [row["id"] for row in seen["orders"].data] == ["o1"]

    def test_many_receives_one_leg_per_bound_upstream(self) -> None:
        seen: dict[str, Any] = {}

        @il.source
        class Matcher(il.Source):
            @il.asset(partitioning=PARTITION, relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)})
            def matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict]:
                seen["legs"] = campaigns
                return []

        memory = il.MemoryDestination()
        fb, tt = FbLike(destinations=[memory]), TtLike(destinations=[memory])   # each declares a `campaigns` asset
        matcher = Matcher(destinations=[memory])
        il.DAG(fb, tt, matcher).materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
        assert {leg.asset.id for leg in seen["legs"]} == {fb.campaigns.id, tt.campaigns.id}

    def test_missing_partition_gives_none_data(self) -> None:
        # Port of the PR #321 test: the upstream is bound but never materialised, so its leg arrives as
        # Upstream(asset, data=None) and a LOG warning names it; the asset still runs.
        ...as in tests/asset/test_base.py::test_read_upstreams_missing_data today, with `orders: il.Upstream`
        instead of `depends_on` and `upstreams=`.
```

`FbLike` and `TtLike` are two module-level `@il.source` classes in the test file, each with one `campaigns` asset returning a single stamped row; `PARTITION = il.TimePartitionConfig(column="date")`.

Port the existing PR #321 read-path tests (`_read_upstreams`, `default_destination_key`, at-most-one) to the new declaration forms; their assertions stay. Add to `tests/asset/test_decorator.py` tests for `@il.asset(relations={...})` and `@il.asset(destinations=[...])`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset packages/interloper-core/tests/operation -q`
Expected: FAIL (`A.relations` lacks inferred entries; `depends_on` references in old tests).

- [ ] **Step 3: Implement**

```python
class Asset(Component, Operation):
    destinations = Relation("destination", many=True, optional=True)
    _defer_validation: ClassVar[bool] = True

    @classmethod
    def collect(cls) -> None:
        super().collect()
        if "data" not in cls.__dict__:
            return
        try:
            signature = inspect.signature(cls.data)
        except (TypeError, ValueError):
            return
        hints = typing.get_type_hints(cls.data, include_extras=False) if not hasattr(cls.data, "__signature__") else _hints_from_signature(signature)
        inferred: dict[str, Relation] = {}
        for name, parameter in signature.parameters.items():
            if name in ("self", "context", "source", "kwargs") or name in cls.relations:
                continue
            hint, optional = _unwrap_optional(hints.get(name, parameter.annotation))
            optional = optional or parameter.default is None
            if hint is Upstream:
                inferred[name] = Relation("asset", name, optional=optional)
            elif typing.get_origin(hint) is list and typing.get_args(hint) == (Upstream,):
                inferred[name] = Relation("asset", name, many=True, optional=optional)
            elif isinstance(hint, type) and issubclass(hint, Component):
                inferred[name] = Relation(hint, optional=optional)
            else:
                raise TypeError(
                    f"{cls.__name__}.data() parameter '{name}' is neither a Component class nor il.Upstream; "
                    "nothing can fill it"
                )
        if inferred:
            cls.relations = {**cls.relations, **inferred}
            for name, relation in inferred.items():
                relation.name = name
                setattr(cls, name, Bound(relation))
```

(The decorator sets `data.__signature__`; read annotations from that signature's parameters, resolving string annotations with `typing.get_type_hints` on the original function, which the decorator stores as `cls._data_fn`.)

```python
    async def _build_kwargs(self, context, partition_or_window, dag):
        kwargs: dict[str, Any] = {}
        for name in inspect.signature(self.data).parameters:
            if name in ("self", "kwargs"):
                continue
            if name == "context":
                kwargs["context"] = context
            elif name == "source":
                kwargs["source"] = self._parent
            elif name in type(self).relations:
                relation = type(self).relations[name]
                if "asset" in relation.kinds():
                    targets = self._bound.get(name, [])
                    if targets and dag is None:
                        raise AssetError(f"Asset '{self.key}' has upstreams but no DAG provided. ...")
                    legs = await self._read_upstreams(name, targets, dag, partition_or_window, context.metadata) if targets else []
                    kwargs[name] = legs if relation.many else (legs[0] if legs else None)
                else:
                    kwargs[name] = self.resolve(name)
        return kwargs
```

`_read_upstreams` takes the bound `Asset` instances instead of ids; "not in the DAG" becomes `dag is not None and upstream.id not in dag.operation_map`, still skip with the warning. `_read_destination` uses `self.destinations` (bound, trickled) and `default_destination_key`. `_validate_destination` uses `type(self).relations["destinations"].accepts(...)`. `identity` returns `ComponentIdentity.of(self)`. `__call__` accepts `**relations` and passes them as kwargs to the copy after `model_copy`, binding through `__init__`. `Operation.upstream_relations()` returns `{n: r for n, r in type(self).relations.items() if "asset" in r.kinds()}`.

`asset/decorator.py`: replace `resources=` and `depends_on=` with `relations: dict[str, Relation] | None`, set `classvars["relations"]`; `destinations=` sets `classvars["relations"]["destinations"] = Relation("destination", key=[d.key ...], many=True, optional=True)`.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/asset packages/interloper-core/tests/operation packages/interloper-core/tests/component -q`
Expected: PASS, including Task 3's deferred `Down` test.

- [ ] **Step 5: Commit**

```bash
git add -A packages/interloper-core/src/interloper packages/interloper-core/tests
git commit -m "feat!(core): assets infer relations from data() and inject bound instances and Upstream legs

By Digitl"
```

---

### Task 6: DAG over bound relations, with read-only upstreams

**Files:**
- Modify: `dag/base.py:87-198` (`_build_graph`, new `_include_read_only_upstreams`, `_resolve_declared`), `dag/base.py:204-208` (`_check_relations`), `dag/base.py:490-515` (`mini_dag`)
- Test: `tests/dag/test_base.py`

**Interfaces:**
- Produces:
  ```py
  DAG._include_read_only_upstreams(self) -> None    # after flattening workloads, before edges
  DAG._resolve_declared(self) -> None               # binds through asset.bind()
  DAG._check_relations(self) -> None                # validate_relations(operation_map) per live node
  ```
- Behaviour: a bound asset target absent from the flattened operations is appended as a copy with `materializable=False` (via `target(materializable=False)`, keeping its id) so it is read, never run. `mini_dag(operation_id)` becomes `DAG(operation)` where operation's bound upstreams are pulled in by the same mechanism, plus the existing behaviour of marking everything but the target non-materialisable.

- [ ] **Step 1: Write the failing tests**

```python
class TestReadOnlyUpstreams:
    def test_bound_upstream_outside_workloads_joins_read_only(self) -> None:
        fb = FbLike(...)                      # a source with a `campaigns` asset
        matcher = Matcher(...)                # asset campaign_matches: il.Relation("asset", "*.campaigns", many=True)
        matcher.campaign_matches.bind("campaigns", fb.campaigns)
        dag = il.DAG(matcher)
        node = dag.operation_map[fb.campaigns.id]
        assert node.materializable is False
        assert dag.get_predecessors(matcher.campaign_matches.id) == [fb.campaigns.id]

    def test_unbound_wildcard_binds_every_candidate_in_dag(self) -> None:
        fb, tt, matcher = FbLike(...), TtLike(...), Matcher(...)
        dag = il.DAG(fb, tt, matcher)
        assert set(dag.get_predecessors(matcher.campaign_matches.id)) == {fb.campaigns.id, tt.campaigns.id}

    def test_single_with_two_candidates_is_an_error(self) -> None:
        with pytest.raises(DAGError, match="explicitly"):
            il.DAG(ShopA(), ShopB(), FinanceSingle())     # revenue: il.Relation("asset", "*.orders")

    def test_required_unbound_after_resolution_fails(self) -> None:
        with pytest.raises(ConfigError, match="unbound"):
            il.DAG(FinanceSingle())
```

Port the PR #321 DAG tests (granularity check, at-most-one, `_resolve_declared` for bare keys within a source instance) to the new declaration forms.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/dag -q`
Expected: FAIL (`operation.upstreams` no longer exists).

- [ ] **Step 3: Implement**

```python
    def _build_graph(self, items):
        ...flatten as today...
        self.operation_map = {operation.id: operation for operation in self.operations}
        ...duplicate check as today...
        self._resolve_declared()
        self._include_read_only_upstreams()
        for operation in self.operations:
            self.successors.setdefault(operation.id, [])
        for operation in self.operations:
            if not operation.materializable:
                continue
            self.predecessors[operation.id] = []
            for name, relation in operation.upstream_relations().items():
                for target in operation._bound.get(name, []):
                    self.predecessors[operation.id].append(target.id)
                    self.successors[target.id].append(operation.id)

    def _include_read_only_upstreams(self) -> None:
        queue = list(self.operations)
        while queue:
            operation = queue.pop()
            for name in operation.upstream_relations():
                for target in operation._bound.get(name, []):
                    if target.id in self.operation_map:
                        continue
                    read_only = target(materializable=False)
                    self.operations.append(read_only)
                    self.operation_map[read_only.id] = read_only
                    queue.append(read_only)

    def _resolve_declared(self) -> None:
        assets = [op for op in self.operations if isinstance(op, Asset)]
        for asset in assets:
            if not asset.materializable:
                continue
            for name, relation in asset.upstream_relations().items():
                if asset._bound.get(name) or not relation.keys():
                    continue
                candidates = [
                    c for c in assets
                    if c is not asset and relation.accepts("asset", c.identity, owner=asset.identity)
                    and (any("." in k for k in relation.keys()) or c.parent is asset.parent)
                ]
                if not candidates:
                    continue
                if relation.many:
                    asset.bind(name, *candidates)
                elif len(candidates) > 1:
                    raise DAGError(...)      # keep the PR #321 message
                else:
                    asset.bind(name, candidates[0])
```

Note `target(materializable=False)` must preserve the id and bound relations of the copy (the `__call__` from Task 5 copies `_bound`). `_check_relations` replaces `_check_upstreams`. `_check_partition_dependencies` unchanged.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests/dag -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/dag/base.py packages/interloper-core/tests/dag/test_base.py
git commit -m "feat(core): DAG edges from bound relations; bound upstreams outside the run join read-only

By Digitl"
```

---

### Task 7: Serialisation: the parent rule and `{ref: id}`

**Files:**
- Modify: `component/base.py` (`to_spec` override), `serializable/base.py:40-60` (`Spec.id` semantics unchanged; `Spec.reconstruct` gains a document-scoped id registry and `resolve`), `serializable/base.py:532-570` (`from_spec(..., resolve=None)`, `from_spec_file`)
- Modify: `source/base.py:277-313` (`to_spec` assets override map: each asset init carries its relation values through the same rule), `source/base.py:189-226` (`_apply_asset_overrides` passes relation kwargs through)
- Modify: `dag/base.py:26-63` (`DAGSpec.items` one per root; `reconstruct` shares one registry across items), `dag/base.py:397-460` (`to_spec`, `from_spec`, `from_spec_file`)
- Test: `tests/serializable/test_base.py`, `tests/component/test_base.py`, `tests/source/test_base.py`, `tests/dag/test_base.py`

**Interfaces:**
- Produces:
  ```py
  Component.to_spec(self) -> Spec                         # relation values: nested Spec, {"ref": id}, or list of either
  Component.from_spec(cls, spec, catalog=None, *, resolve: Callable[[str], Component] | None = None) -> Self
  Spec.reconstruct(self, catalog=None, *, resolve=None, registry: dict[str, Component] | None = None) -> Serializable
  DAGSpec.items: list[Spec]; DAGSpec.reconstruct(catalog=None, *, resolve=None) -> DAG
  ```
- Emission rule (spec section 7): during one `to_spec()` traversal, a target with a `parent` is always `{"ref": id}`; a parentless target is a nested spec the first time it is seen and `{"ref": id}` afterwards. Assets are emitted under their source's `assets` override map (containment), never under a relation.
- Reconstruction: two passes over a document. Pass one builds every inline component, registering each by id (including assets built by `_apply_asset_overrides`). Pass two binds `{"ref": id}` values: registry first, then `resolve(id)`, else `SpecError("unresolved reference '<id>'")`. Because bind happens after construction, inline components are constructed with their relation kwargs (validated then) and ref-bound relations are validated by `validate_relations()` at the end of pass two.

- [ ] **Step 1: Write the failing tests**

```python
class TestSpecRule:
    def test_parentless_target_inline_once_then_ref(self) -> None:
        bq = Dest(connection=Conn())
        a, b = Widget(connection=Conn(), destinations=[bq]), Widget(connection=Conn(), destinations=[bq])
        job = CronJob(cron="0 6 * * *", targets=[a, b])
        init = job.to_spec().init
        first = init["targets"][0]["init"]["destinations"][0]
        second = init["targets"][1]["init"]["destinations"][0]
        assert "key" in first or "path" in first
        assert second == {"ref": bq.id}

    def test_asset_target_is_always_ref_and_lives_under_its_source(self) -> None:
        shop, finance = Shop(connection=Conn()), Finance()
        finance.revenue.bind("orders", shop.orders)
        job = CronJob(cron="0 6 * * *", targets=[shop, finance])
        init = job.to_spec().init
        assert init["targets"][1]["init"]["assets"]["revenue"]["orders"] == {"ref": shop.orders.id}
        assert init["targets"][0]["init"]["assets"]["orders"]["id"] == shop.orders.id

    def test_round_trip_shares_instances(self) -> None:
        bq = Dest(connection=Conn())
        shop, finance = Shop(connection=Conn(), destinations=[bq]), Finance(destinations=[bq])
        finance.revenue.bind("orders", shop.orders)
        job = CronJob(cron="0 6 * * *", targets=[shop, finance])
        rebuilt = il.CronJob.from_spec(job.to_spec())
        rebuilt_shop, rebuilt_finance = rebuilt.targets
        assert rebuilt_finance.revenue.orders is rebuilt_shop.orders
        assert rebuilt_shop.destinations[0] is rebuilt_finance.destinations[0]

    def test_unresolved_ref_without_resolve_is_spec_error(self) -> None:
        spec = Spec(key="finance", init={"assets": {"revenue": {"orders": {"ref": "nope"}}}})
        with pytest.raises(SpecError, match="unresolved reference 'nope'"):
            il.Source.from_spec(spec, catalog)

    def test_resolve_callable_supplies_missing_ref(self) -> None:
        shop = Shop(connection=Conn())
        spec = Spec(key="finance", init={"assets": {"revenue": {"orders": {"ref": shop.orders.id}}}})
        finance = il.Source.from_spec(spec, catalog, resolve={shop.orders.id: shop.orders}.__getitem__)
        assert finance.revenue.orders is shop.orders


class TestDAGSpec:
    def test_one_item_per_root_and_read_only_parent(self) -> None:
        fb, matcher = FbLike(...), Matcher(...)
        matcher.campaign_matches.bind("campaigns", fb.campaigns)
        spec = il.DAG(matcher).to_spec()
        assert len(spec.items) == 2
        fb_item = next(i for i in spec.items if i.id == fb.id)
        assert fb_item.init["assets"]["campaigns"]["materializable"] is False

    def test_dag_spec_round_trip_from_file(self, tmp_path: Path) -> None:
        shop, finance = Shop(connection=Conn(), destinations=[Dest(connection=Conn())]), Finance()
        finance.revenue.bind("orders", shop.orders)
        spec = il.DAG(shop, finance).to_spec()
        path = tmp_path / "dag.yaml"
        path.write_text(yaml.safe_dump_all([item.model_dump(mode="json", exclude_defaults=True) for item in spec.items]))
        rebuilt = il.DAG.from_spec_file(path)
        assert set(rebuilt.operation_map) == set(il.DAG(shop, finance).operation_map)
        revenue = rebuilt.operation_map[finance.revenue.id]
        assert revenue.orders.id == shop.orders.id
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-core/tests/serializable packages/interloper-core/tests/dag -q -k "Spec"`
Expected: FAIL (relations not in `to_spec` output).

- [ ] **Step 3: Implement**

```python
# component/base.py
    def to_spec(self) -> Spec:
        return self._to_spec(seen=set())

    def _to_spec(self, *, seen: set[str]) -> Spec:
        seen.add(self.id)
        spec = super().to_spec()                     # fields only
        init = dict(spec.init or {})
        for name, targets in self._bound.items():
            if not targets:
                continue
            values = [self._emit(target, seen) for target in targets]
            init[name] = values if type(self).relations[name].many else values[0]
        return spec.model_copy(update={"id": self.id, "init": init or None})

    @staticmethod
    def _emit(target: Component, seen: set[str]) -> dict[str, Any]:
        if target.parent is not None or target.id in seen:
            return {"ref": target.id}
        return target._to_spec(seen=seen).model_dump(mode="json", exclude_defaults=True)
```

`Source.to_spec` builds the `assets` override map by calling each asset's `_to_spec(seen=seen)` and lifting its `init` plus `id` into the map (same `seen` set, so a destination bound to the source and trickled to its assets is inline on the source and a ref on the assets). `Spec.reconstruct` gains `registry` and `resolve`: the `load` walker leaves `{"ref": id}` dicts in place, `cls(**kwargs)` receives relation kwargs with inline instances only, then a second walker binds refs on the built instance (`instance.bind(name, *[lookup(ref) ...])`) and registers `instance` and its assets by id. `DAGSpec.reconstruct` shares one registry across items and passes `resolve`. `Serializable.from_spec` and `from_spec_file` grow the `resolve` keyword and pass it on. `DAG.to_spec` emits one `Spec` per root (workloads given to the constructor plus parents of read-only upstreams, each parent serialised with only its read-only asset marked).

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-core/tests -q`
Expected: PASS for every core test.

- [ ] **Step 5: Commit**

```bash
git add -A packages/interloper-core/src/interloper packages/interloper-core/tests
git commit -m "feat!(core): manifests are to_spec() output; parented targets serialise as references

By Digitl"
```

---

### Task 8: Connectors and the demo source move to annotations

**Files:**
- Modify: every `packages/interloper-assets/src/interloper_assets/*/source.py` using `@il.source(resources={...})` (24 files; `grep -rl "resources=" packages/interloper-assets/src`): drop the kwarg, add `connection: <ConnectionClass>` as the first class-body annotation. Function-based sources use `relations={"connection": il.Relation(<ConnectionClass>)}`.
- Modify: `packages/interloper-assets/src/interloper_assets/demo/source.py` (`a: str` becomes `a: il.Upstream`, `x: str | None = None` becomes `x: il.Upstream | None = None`, `b`, `c`, `d` on `e` likewise)
- Modify: `packages/interloper-assets/src/interloper_assets/campaign_matcher/source.py` (leave the placeholder class but fix its name to `CampaignMatcher`; phase 3 fills it)
- Modify: `examples/*.py` using `resources=`, `depends_on=` or `requires=`
- Test: `packages/interloper-assets/tests/*` (run; adapt any test that reads `resource_types`)

**Interfaces:**
- Consumes: Task 4 decorators, Task 5 inference. Connectors that rely on an unbound config class with defaulted fields keep working: such a relation is self-filling and is instantiated at read time, as today.

- [ ] **Step 1: Run the assets suite to see the failures**

Run: `uv run --frozen pytest packages/interloper-assets -q -x`
Expected: FAIL at import: `source() got an unexpected keyword argument 'resources'`.

- [ ] **Step 2: Convert the connectors**

For each class-based source:

```python
@il.source(
    tags=["Advertising"],
    icon="logos:facebook",
    normalizer=FacebookActionsNormalizer(flatten_max_level=1),
)
class FacebookAds(il.Source):
    """Facebook Ads (Meta Marketing) advertising platform integration."""

    connection: FacebookAdsConnection
    account_id: str = il.FetchField(provider="connection.accounts", ...)
```

Asset methods keep `connection: FacebookAdsConnection` in their signature (now an inferred relation, trickled from the source). Demo source:

```python
    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def b(self, context: il.ExecutionContext, a: il.Upstream, x: il.Upstream | None = None) -> pd.DataFrame:
```

- [ ] **Step 3: Run the suites**

Run: `uv run --frozen pytest packages/interloper-assets packages/interloper-google-cloud packages/interloper-pandas -q`
Expected: PASS

- [ ] **Step 4: Check nothing retired survives in these packages**

Run: `grep -rn "resource_types\|depends_on\|resources=\|requires=\|il.Dependency\|upstreams=" packages/interloper-assets packages/interloper-google-cloud examples || echo clean`
Expected: `clean`

- [ ] **Step 5: Commit**

```bash
git add -A packages/interloper-assets examples packages/interloper-google-cloud
git commit -m "refactor(assets): declare connections by annotation and upstreams as il.Upstream

By Digitl"
```

---

### Task 9: Docs, plugin skills and public exports

**Files:**
- Rewrite: `docs/guide/dependencies.md` (sections: Inside a source, Declaring relations explicitly, Many upstreams, How wiring works, Reading upstream data, Rules the DAG enforces, Running one asset with its parents), `docs/guide/resources.md` (Injecting resources: annotation form; Defaults via `il.Relation(default=)`; Trickling), `docs/guide/specs.md` (the reference rule, one example with `{ref: id}`), `docs/extending/components.md` (Relations section: `Relation`, `Bound`, `collect`, `bind`, `trickle`, `validate_relations`), `docs/reference/decorators.md` (`relations=` and `destinations=`, `resources=` and `depends_on=` gone)
- Modify: `plugins/interloper/skills/interloper-source/SKILL.md`, `interloper-destination/SKILL.md`, `interloper-connection/SKILL.md`, `interloper-manifest/SKILL.md`, `interloper-upgrade/SKILL.md` (same vocabulary)
- Modify: `__init__.py` (`Relation`, `ComponentIdentity` exported; `Dependency`, `RelationDefinition`, `ResourceRef`, `AssetIdentity` removed)
- Test: `tests/test_public_api.py` if it exists, else the `__all__` assertions in `tests/component/test_base.py`

- [ ] **Step 1: Write the export test**

```python
def test_public_relation_names() -> None:
    assert il.Relation is Relation
    assert il.ComponentIdentity is ComponentIdentity
    for retired in ("Dependency", "RelationDefinition", "ResourceRef", "AssetIdentity"):
        assert not hasattr(il, retired)
```

- [ ] **Step 2: Rewrite the docs**

Each guide page shows the minimal form first (annotations), the explicit form second (`relations=`), and one manifest. Copy the examples from the spec, sections 5.5, 6 and 7, verbatim. Every code block must run; put them through `uv run --frozen python -c` where they are self-contained.

- [ ] **Step 3: Verify**

Run: `grep -rn "depends_on\|resource_types\|resources=\|il.Dependency\|RelationSlot\|upstreams:\|on_unbind\|slotted" docs plugins --include='*.md' | grep -v superpowers || echo clean`
Expected: `clean`

- [ ] **Step 4: Commit**

```bash
git add docs plugins packages/interloper-core/src/interloper/__init__.py packages/interloper-core/tests
git commit -m "docs: describe the Relation model, annotation and explicit forms, and spec references

By Digitl"
```

---

### Task 10: Whole-repo verification, squash, PR

**Files:** none new.

- [ ] **Step 1: Full checks**

Run: `uv run --frozen ruff check && uv run --frozen ty check && uv run --frozen pytest packages/interloper-core packages/interloper-assets packages/interloper-google-cloud packages/interloper-pandas -q`
Expected: all green. Record the test count in the ledger.

- [ ] **Step 2: Retired-name sweep over core-owned files**

Run: `grep -rn "resource_types\|relation_types\|RelationDefinition\|Dependency\b\|ResourceRef\|trickle_resources\|relation_definitions\|depends_on\|RelationSlot\|AssetIdentity\|declared_upstreams\|sibling_upstreams\|validate_upstreams\|_infer_resource_types\|_infer_upstreams\|destination_types\|on_unbind\|slotted" packages/interloper-core packages/interloper-assets docs plugins examples | grep -v superpowers || echo clean`
Expected: `clean`. Platform packages (`interloper-db`, `-api`, `-toolkit`, `-agent`, `-scheduler`, `-app`) still reference old names; phase 2 and 4 own them, and their test suites are expected to fail until then. State this in the PR body.

- [ ] **Step 3: Squash and push**

`git rebase -i origin/main` down to the logical `feat!:` commits (primitive; Component; kinds; assets; DAG; serialisation; connectors; docs), `git push --force-with-lease`, retitle PR #321 and replace its body with the spec's decisions table and the phase list. Only when Guillaume asks.
