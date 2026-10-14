# Core Phase 4: Component, Serializable and Decorators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One decorator class, one class-building path, ownership kept to the one kind that owns, no pushes into components nothing owns, and reconstruction without a component special case in the base layer.

**Architecture:** Every decorator is a `Decorator(anchor)` instance. It turns its target into a `ClassBody` (a class read as is, or a function read by the anchor's `body_from_function`) and hands it to `anchor.build_class(body, **settings)`, which sorts the settings on the anchor and creates one new class with everything in its namespace. `Source` alone reports owned components; `trickle` and `on_rebind` go, and a job's destinations reach its targets only in `Job.operations()`, on copies. `Serializable.from_init` is the step `Spec.reconstruct` delegates to.

**Tech Stack:** Python 3.12 (floor 3.10), Pydantic 2.12, pytest, ruff, ty, zensical (docs), uv workspace.

**Spec:** `docs/superpowers/specs/2026-10-14-core-component-design.md`

## Global Constraints

- No em dash character in any text: code, comments, docstrings, docs, messages, tests.
- Google-style docstrings, complete: every module, class, function and method, private helpers, `__init__` and dunders included; `Args:` lists every parameter, `Returns:`/`Raises:` whenever they apply (`Raises:` only for an explicit `raise` in the body). A decorator instance's attribute docstring is product copy: prose and an example, no mechanical sections.
- Beautiful shapes: no nested functions or closures as structure (the generated `data()` is the one function a decorator must generate), no context managers yielding callables; logic lives on the class that owns its data.
- Names written in full (`context`, `operation`, `destination`, never `ctx`, `op`, `dest`).
- Comments only for a non-obvious why; no attribute-level comments; no comments narrating the change.
- Tests mirror the source layout one-to-one (`src/interloper/<pkg>/<module>.py` is tested in `tests/<pkg>/test_<module>.py`); no new `test_<feature>.py`.
- Run uv with `UV_FROZEN=1`; never change `uv.lock`.
- Never commit, amend or push during the tasks; the work rests uncommitted until the finish step.
- No author-facing concept is added: no `owns`, no `lends`, no new hook an author overrides.
- Unchanged: every decorator's call signature and settings; class keys, kinds and classpaths; catalog definitions; the spec wire format (owned assets as a key-to-init map); relation semantics for owned assets; what any run executes.
- An error raised in `model_post_init` surfaces as pydantic's `ValidationError` carrying the message (a `ConfigError` is a `ValueError`); tests match on the message, as `Source`'s existing checks do.
- Ruff line length 120.
- Gate per task: Tasks 1, 3, 4, 5 and 6 end with `UV_FROZEN=1 make check-python` green. Task 2 replaces every decorator at once and ends with it green too.

## Review Focus

1. A decorated plain class given an invalid ClassVar setting (`@il.connection(tags="Cloud") class X:`) must fail at decoration, like a function or a subclass does. Pinned in Task 2 (`test_a_plain_class_setting_is_validated_too`).
2. A decorated subclass is a new class: its instances must round-trip through `to_spec`/`from_spec`, its classpath must resolve to the built class, and the catalog must see every key, kind and path exactly as before. Pinned in Task 2 (`test_a_decorated_subclass_round_trips` and the catalog snapshot comparison).
3. A decorated subclass of `Source` with class-body assets must still expose them on the class and the instance, and each asset's classpath must resolve. Pinned in Task 2 (`test_a_decorated_source_subclass_keeps_its_assets`).
4. A job whose target is an owned asset of a source without destinations must run that asset with the job's destinations while it keeps reading through its source (dataset, siblings). Pinned in Task 4 (`test_an_owned_asset_target_without_destinations_runs_with_the_jobs`).
5. Compiling a job twice must yield the same operation ids, and disabling an operation of one compilation (the scheduler's retry path) must not touch the job's targets. Pinned in Task 4 (`test_compiling_twice_yields_the_same_ids_and_leaves_the_targets_alone`).

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `packages/interloper-core/src/interloper/serializable/body.py` (new) | `ClassBody`, what a decorator's target contributes | 1 |
| `packages/interloper-core/src/interloper/serializable/base.py` | `Serializable`: settings sorting, `_classvars`, `build_class`, `body_from_function`, `from_init`; `Spec._load` | 1, 2, 5 |
| `packages/interloper-core/src/interloper/component/base.py` | `Component`: relation settings, `from_init`; `trickle`/`on_rebind` removed; `_owned_by_field` returns `{}` | 1, 2, 3, 4, 5 |
| `packages/interloper-core/src/interloper/component/decorator.py` | `Decorator` | 2 |
| `packages/interloper-core/src/interloper/{asset,source,connection,config,destination,schema}/decorator.py`, `packages/interloper-sql/src/interloper_sql/view/decorator.py` | the decorator instances | 2 |
| `packages/interloper-core/src/interloper/asset/base.py` | `Asset.body_from_function` (the generated `data()`) | 2 |
| `packages/interloper-core/src/interloper/source/base.py` | `Source.body_from_function`, `_owned_by_field`, the one-owner check; `on_rebind` removed | 2, 3, 4 |
| `packages/interloper-core/src/interloper/job/base.py` | `Job.operations()` and `_run_as`; `on_rebind` removed | 4 |
| docs, skills | surface | 6 |

---

### Task 1: ClassBody, settings sorted on the anchor

**Files:**
- Create: `packages/interloper-core/src/interloper/serializable/body.py`
- Modify: `packages/interloper-core/src/interloper/serializable/base.py`, `packages/interloper-core/src/interloper/serializable/__init__.py`, `packages/interloper-core/src/interloper/component/base.py`
- Test: `packages/interloper-core/tests/serializable/test_body.py` (new, mirrors `body.py`), `packages/interloper-core/tests/serializable/test_base.py`, `packages/interloper-core/tests/component/test_base.py`

**Interfaces:**
- Produces:
  - `ClassBody(name, module, qualname, doc, members={}, annotations={}, base=None)`, frozen dataclass; `ClassBody.of(target, anchor)`, `ClassBody.from_class(target, anchor)`, `ClassBody.from_function(fn, *, members, annotations=None)`; exported from `interloper.serializable`.
  - `Serializable._classvars() -> dict[str, Any]`, `Serializable._validate_classvars() -> None` (moved from `Component`, called from `Serializable.__init_subclass__`), `_RESERVED_CLASSVARS` (moved to `serializable/base.py`).
  - `Serializable._sort_settings(settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]`: namespace entries and their annotations.
  - `Component._sort_settings(...)`: the same, plus the `relations=` channel (an entry `"relations": dict[str, Relation]`), refusing `id` and relation names.
  - `Serializable.body_from_function(fn) -> ClassBody`: refuses (`TypeError`).
- Nothing calls them yet; `build_class` and the decorators are unchanged until Task 2.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-core/tests/serializable/test_body.py`:

```python
"""Tests for ``interloper.serializable.body``."""

from typing import Any

import pytest

import interloper as il
from interloper.serializable import ClassBody


class PlainConfig:
    """A plain class a decorator reads."""

    timeout: int = 30

    def helper(self) -> int:
        return 1


class TestFromClass:
    def test_a_plain_class_contributes_its_members_and_annotations(self):
        body = ClassBody.from_class(PlainConfig, il.Config)

        assert body.base is None
        assert (body.name, body.module, body.qualname) == ("PlainConfig", __name__, "PlainConfig")
        assert body.doc == "A plain class a decorator reads."
        assert body.members["timeout"] == 30
        assert callable(body.members["helper"])
        assert "__dict__" not in body.members
        assert body.annotations == {"timeout": int}

    def test_a_subclass_of_the_anchor_is_the_base_with_an_empty_body(self):
        class FakeTimedConfig(il.Config):
            """A config already on its anchor."""

            timeout: int = 30

        body = ClassBody.from_class(FakeTimedConfig, il.Config)

        assert body.base is FakeTimedConfig
        assert (body.members, body.annotations) == ({}, {})
        assert body.doc == "A config already on its anchor."


class TestOf:
    def test_a_class_is_read_as_a_class(self):
        assert ClassBody.of(PlainConfig, il.Config).members["timeout"] == 30

    def test_a_function_is_read_by_the_anchor(self):
        def probe() -> None:
            return None

        with pytest.raises(TypeError, match=r"Config decorates a class, not a function"):
            ClassBody.of(probe, il.Config)


class TestFromFunction:
    def test_takes_its_identity_from_the_function(self):
        def rows() -> list[dict[str, Any]]:
            """Rows of something."""
            return []

        body = ClassBody.from_function(rows, members={"x": 1}, annotations={"x": int})

        assert (body.name, body.module, body.doc) == ("rows", __name__, "Rows of something.")
        assert body.qualname.endswith("rows")
        assert (body.members, body.annotations, body.base) == ({"x": 1}, {"x": int}, None)
```

Append to `packages/interloper-core/tests/serializable/test_base.py`:

```python
class TestSortSettings:
    """A decorator's settings, sorted by the class they configure."""

    def test_a_classvar_stays_a_value_and_is_not_annotated(self):
        entries, annotations = il.Asset._sort_settings({"tags": ["Report"]})

        assert entries == {"tags": ["Report"]}
        assert annotations == {}

    def test_a_field_default_keeps_the_field_metadata(self):
        entries, annotations = il.Source._sort_settings({"dataset": "raw"})

        info = entries["dataset"]
        assert info.default == "raw"
        assert info.description == il.Source.model_fields["dataset"].description
        assert annotations == {"dataset": il.Source.model_fields["dataset"].annotation}

    def test_an_unknown_name_lists_the_accepted_ones(self):
        with pytest.raises(TypeError, match=r"Asset does not accept 'nope'") as error:
            il.Asset._sort_settings({"nope": 1})

        assert "'tags'" in str(error.value)
        assert "'dataset'" in str(error.value)

    def test_an_unknown_name_names_the_subclass_declaring_it(self):
        with pytest.raises(TypeError, match=r"Connection does not accept 'oauth'.*OAuthConnection declares it"):
            il.Connection._sort_settings({"oauth": None})

    def test_a_schema_routes_its_key(self):
        entries, _ = il.Schema._sort_settings({"key": "custom"})

        assert entries == {"key": "custom"}
```

In `packages/interloper-core/tests/component/test_base.py`, append to the class holding `test_settings_lists_every_public_classvar_with_its_type`:

```python
    def test_relations_are_sorted_into_the_namespace(self):
        relation = il.Relation("asset", "orders", optional=True)

        entries, _ = il.Asset._sort_settings({"relations": {"upstream": relation}})

        assert entries == {"relations": {"upstream": relation}}

    def test_the_identity_field_is_never_a_setting(self):
        with pytest.raises(TypeError, match=r"does not accept 'id'"):
            il.Asset._sort_settings({"id": "fixed"})

    def test_a_relation_name_points_at_the_relation_channel(self):
        with pytest.raises(TypeError, match=r'relations=\{"destinations": \.\.\.\}'):
            il.Asset._sort_settings({"destinations": [il.MemoryDestination]})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/serializable packages/interloper-core/tests/component/test_base.py -q -p no:cacheprovider`
Expected: FAIL (`ImportError: cannot import name 'ClassBody'`, `_sort_settings` missing).

- [ ] **Step 3: `ClassBody`**

`packages/interloper-core/src/interloper/serializable/body.py`:

```python
"""ClassBody: what a decorated class or function contributes to the class a decorator builds."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.serializable.base import Serializable


@dataclass(frozen=True)
class ClassBody:
    """What a decorated class or function contributes to the class a decorator builds.

    A decorator reads its target into a body and builds one new class from it
    (see `Serializable.build_class`). A class already extending the decorator's
    anchor contributes nothing but itself, as the base of the new class; any
    other target contributes members and annotations, and the anchor is the base.
    """

    name: str
    module: str
    qualname: str
    doc: str | None
    members: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)
    base: type[Serializable] | None = None

    @classmethod
    def of(cls, target: Any, anchor: type[Serializable]) -> ClassBody:
        """Read a decorator's target: a class as a class, a function the way the anchor says.

        Args:
            target: The decorated class or function.
            anchor: The kind the decorator builds.

        Returns:
            The body the decorator builds its class from.
        """
        if isinstance(target, type):
            return cls.from_class(target, anchor)
        return anchor.body_from_function(target)

    @classmethod
    def from_class(cls, target: type, anchor: type[Serializable]) -> ClassBody:
        """Read a decorated class.

        Args:
            target: The decorated class.
            anchor: The kind the decorator builds.

        Returns:
            An empty body based on *target* when it already extends *anchor*,
            else its members (dunders excluded) and its own annotations.
        """
        identity = {"name": target.__name__, "module": target.__module__, "qualname": target.__qualname__}
        if issubclass(target, anchor):
            return cls(**identity, doc=target.__doc__, base=target)
        members = {name: value for name, value in target.__dict__.items() if not name.startswith("__")}
        return cls(**identity, doc=target.__doc__, members=members, annotations=dict(inspect.get_annotations(target)))

    @classmethod
    def from_function(
        cls, fn: Callable[..., Any], *, members: dict[str, Any], annotations: dict[str, Any] | None = None
    ) -> ClassBody:
        """Build the body a function contributes, named after the function.

        Args:
            fn: The decorated function.
            members: The members the anchor makes of it (a ``data()``, asset classes, defaults).
            annotations: Annotations of those members, ``None`` for none.

        Returns:
            The body.
        """
        return cls(
            name=fn.__name__,
            module=fn.__module__,
            qualname=fn.__qualname__,
            doc=fn.__doc__,
            members=members,
            annotations=dict(annotations or {}),
        )
```

Export `ClassBody` from `interloper/serializable/__init__.py` (`__all__` too).

- [ ] **Step 4: Settings on `Serializable`**

In `packages/interloper-core/src/interloper/serializable/base.py`:

- Move `_RESERVED_CLASSVARS` (with its comment) from `component/base.py`, and `_classvars` and `_validate_classvars` (bodies unchanged) from `Component` to `Serializable`, under a `# -- Settings` divider. Imports: `from interloper.utils.annotations import classvar_type, is_classvar, type_adapter`, `from pydantic import ValidationError`.
- `Serializable.__init_subclass__` ends with `cls._validate_classvars()`; `Component.__init_subclass__` drops its own call (it keeps `_collect`).
- Add:

```python
    @classmethod
    def _sort_settings(cls, settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Turn a decorator's settings into the namespace entries of the class it builds.

        A public ClassVar (see `_classvars`) stays a value: the class's bases
        already annotate it, so it needs no annotation and is validated where
        the class is created. A field default becomes a copy of the field's
        ``FieldInfo`` with the new default (title, description and
        ``json_schema_extra`` kept), annotated with the field's type.

        Args:
            settings: The decorator's settings, name to value.

        Returns:
            The namespace entries, and the annotations to add for them.

        Raises:
            TypeError: If a name is both a ClassVar and a field, or neither.
        """
        classvars = cls._classvars()
        entries: dict[str, Any] = {}
        annotations: dict[str, Any] = {}
        for name, value in settings.items():
            if name in classvars and name in cls.model_fields:
                raise TypeError(
                    f"{cls.__name__} declares '{name}' both as a ClassVar and a field, "
                    f"so the decorator cannot tell which one to set"
                )
            if name in classvars:
                entries[name] = value
            elif name in cls.model_fields:
                info = copy.deepcopy(cls.model_fields[name])
                info.default = value
                info.default_factory = None
                entries[name] = info
                annotations[name] = cls.model_fields[name].annotation
            else:
                raise TypeError(cls._refusal(name, accepted=sorted({*classvars, *cls.model_fields})))
        return entries, annotations

    @classmethod
    def _refusal(cls, name: str, *, accepted: list[str]) -> str:
        """Say why a decorator setting is refused, naming the subclass that declares it when one does.

        Args:
            name: The refused setting.
            accepted: The settings this class accepts.

        Returns:
            The error message.
        """
        declaring = cls._subclass_declaring(name)
        hint = f"; {declaring.__name__} declares it, subclass it" if declaring is not None else ""
        return f"{cls.__name__} does not accept '{name}'{hint}; accepted: {accepted}"

    @classmethod
    def _subclass_declaring(cls, name: str) -> type[Serializable] | None:
        """The closest subclass declaring *name* as a ClassVar or a field, breadth first.

        Args:
            name: The setting looked for.

        Returns:
            The subclass, or ``None`` when none declares it.
        """
        frontier = list(cls.__subclasses__())
        while frontier:
            subclass = frontier.pop(0)
            if name in subclass._classvars() or name in subclass.model_fields:
                return subclass
            frontier.extend(subclass.__subclasses__())
        return None

    @classmethod
    def body_from_function(cls, fn: Callable[..., Any]) -> ClassBody:
        """Read a decorated function into a class body; a kind decorating functions overrides it.

        Args:
            fn: The decorated function.

        Raises:
            TypeError: Always: this kind decorates a class.
        """
        raise TypeError(f"{cls.__name__} decorates a class, not a function")
```

(`ClassBody` is imported under `TYPE_CHECKING` from `interloper.serializable.body`.)

- [ ] **Step 5: Relations on `Component`**

In `packages/interloper-core/src/interloper/component/base.py`, add under `# -- Definition` or a `# -- Settings` divider:

```python
    @classmethod
    def _sort_settings(cls, settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Sort a decorator's settings, the ``relations=`` channel included.

        Relations are written into the namespace's ``relations`` dict, which
        `_collect` merges when the class is created, so they win over the
        relations its annotations declare and are collected once.

        Args:
            settings: The decorator's settings, name to value; ``relations``
                maps relation names to a `Relation`, a component class, or a
                list of component classes narrowing a relation this class declares.

        Returns:
            The namespace entries, and the annotations to add for them.

        Raises:
            TypeError: If a setting names the identity field or a relation.
        """
        settings = dict(settings)
        relations = settings.pop("relations", None) or {}
        for name in settings:
            if name == "id":
                raise TypeError(f"{cls.__name__} does not accept 'id': an id is given to each instance")
            if name in cls.relations:
                raise TypeError(
                    f"{cls.__name__} declares '{name}' as a relation; pass it through relations={{\"{name}\": ...}}"
                )
        entries, annotations = super()._sort_settings(settings)
        if relations:
            entries["relations"] = cls._declared_relations(relations)
        return entries, annotations
```

and `_declared_relations(cls, relations) -> dict[str, Relation]`, the body of today's `component/decorator.py:_relations` with `anchor` read as `cls` and `_is_component(value)` written inline as `isinstance(value, type) and issubclass(value, Component)`. Its error messages stay word for word.

- [ ] **Step 6: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/serializable packages/interloper-core/tests/component -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 7: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green (the old decorator engine still runs, now reading `_classvars` from `Serializable`).

- [ ] **Step 8: Checkpoint (no commit)**

---

### Task 2: One build path, one Decorator

**Files:**
- Modify: `packages/interloper-core/src/interloper/serializable/base.py` (`build_class`), `packages/interloper-core/src/interloper/component/decorator.py`, `packages/interloper-core/src/interloper/component/__init__.py`, `packages/interloper-core/src/interloper/asset/base.py`, `packages/interloper-core/src/interloper/asset/decorator.py`, `packages/interloper-core/src/interloper/source/base.py`, `packages/interloper-core/src/interloper/source/decorator.py`, `packages/interloper-core/src/interloper/connection/decorator.py`, `packages/interloper-core/src/interloper/config/decorator.py`, `packages/interloper-core/src/interloper/destination/decorator.py`, `packages/interloper-core/src/interloper/schema/decorator.py`, `packages/interloper-sql/src/interloper_sql/view/decorator.py`
- Test: `packages/interloper-core/tests/component/test_decorator.py` (rewritten), `packages/interloper-core/tests/serializable/test_base.py` (`TestBuildClass`), `packages/interloper-core/tests/connection/test_decorator.py`, `packages/interloper-core/tests/source/test_decorator.py`, the other per-kind decorator tests as they stand

**Interfaces:**
- Consumes: `ClassBody`, `_sort_settings` (Task 1).
- Produces: `Serializable.build_class(body: ClassBody, **settings: Any) -> type[Self]`; `Decorator(anchor, **settings)` (generic, in `interloper.component`); `Asset.body_from_function`, `Source.body_from_function`; the decorators as instances, `reference(*, table, **overrides) -> Decorator[Reference]`.

- [ ] **Step 1: Snapshot the catalog before anything changes**

Run: `UV_FROZEN=1 uv run python -c "import json, interloper as il; print(json.dumps(il.Catalog.discover().dump(), sort_keys=True, default=str))" > .superpowers/catalog-before.json`
Expected: a JSON file of every catalog definition (key, kind, path, schemas, relations).

- [ ] **Step 2: Write the failing tests**

Rewrite `packages/interloper-core/tests/component/test_decorator.py` (keep its module note on `from __future__`):

```python
"""Tests for ``interloper.component.decorator``."""

# Note: no ``from __future__ import annotations``. Decorated functions'
# parameter annotations must be real classes for relation inference.

from typing import Any

import pytest
from typing_extensions import assert_type

import interloper as il
from interloper.component import Decorator


class DecoratorConnection(il.Connection):
    """Connection fixture used as a relation target."""

    token: str = il.SecretField(default="")


class TestForms:
    def test_bare_builds_the_target(self):
        @il.asset
        def rows() -> list[dict[str, Any]]:
            return []

        assert issubclass(rows, il.Asset)

    def test_with_settings_returns_a_decorator_carrying_them(self):
        configured = il.asset(tags=["Report"])

        assert isinstance(configured, Decorator)
        assert configured.settings == {"tags": ["Report"]}

    def test_settings_accumulate(self):
        configured = il.asset(tags=["Report"])(name="Rows")

        assert configured.settings == {"tags": ["Report"], "name": "Rows"}


class TestSettings:
    def test_a_classvar_reaches_the_built_class(self):
        @il.asset(tags=["Report"], icon="carbon:data-table", name="Rows")
        def rows() -> list[dict[str, Any]]:
            return []

        assert (rows.tags, rows.icon, rows.name) == (["Report"], "carbon:data-table", "Rows")

    def test_a_maturity_reaches_the_definition(self):
        @il.destination(maturity="alpha")
        class FakeAlphaDestination(il.MemoryDestination):
            pass

        assert FakeAlphaDestination.definition().maturity is il.Maturity.ALPHA

    def test_an_invalid_classvar_raises_at_decoration(self):
        with pytest.raises(TypeError, match=r"rows\.maturity: Input should be"):

            @il.asset(maturity="gamma")
            def rows() -> list[dict[str, Any]]:
                return []

    def test_a_plain_class_setting_is_validated_too(self):
        with pytest.raises(TypeError, match=r"FakePlain\.tags: Input should be a valid list"):

            @il.connection(tags="Cloud")
            class FakePlain:
                host: str = "x"

    def test_a_field_default_reaches_instances(self):
        @il.asset(dataset="raw")
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows().dataset == "raw"

    def test_an_unknown_name_is_refused_at_decoration(self):
        with pytest.raises(TypeError, match=r"does not accept 'bogus'"):

            @il.source(bogus=False)
            def probe():
                return []

    def test_oauth_on_a_plain_connection_names_the_subclass(self):
        with pytest.raises(TypeError, match=r"OAuthConnection declares it"):

            @il.connection(oauth=None)
            class FakePlainOAuth:
                pass

    @pytest.mark.parametrize("name", ["kind", "internal_fields", "asset_types", "model_config"])
    def test_a_reserved_name_is_never_a_setting(self, name):
        with pytest.raises(TypeError, match=r"does not accept"):
            il.Source._sort_settings({name: None})

    def test_a_subclass_widens_what_is_accepted(self):
        @il.destination(base_path="/data")
        class FakeCSV(il.CSVDestination):
            pass

        assert FakeCSV().base_path == "/data"


class TestBuiltClass:
    def test_a_decorated_subclass_is_a_new_class_and_the_original_is_untouched(self):
        class FakeOriginal(il.MemoryDestination):
            """A destination written by hand."""

        built = il.destination(tags=["Cloud"])(FakeOriginal)

        assert built is not FakeOriginal and issubclass(built, FakeOriginal)
        assert FakeOriginal.tags == il.MemoryDestination.tags
        assert built.tags == ["Cloud"]
        identity = (built.__name__, built.key, built.kind, built.classpath(), built.__doc__)
        assert identity == (
            "FakeOriginal",
            FakeOriginal.key,
            FakeOriginal.kind,
            FakeOriginal.classpath(),
            "A destination written by hand.",
        )

    def test_relations_are_collected_once_and_win_over_annotations(self):
        @il.asset(relations={"connection": il.Relation(DecoratorConnection, optional=True)})
        def rows(connection: DecoratorConnection) -> list[dict[str, Any]]:
            return []

        assert rows.relations["connection"].optional is True

    def test_relations_narrow_through_the_channel(self):
        @il.asset(relations={"destinations": [il.MemoryDestination]})
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.relations["destinations"].keys == [il.MemoryDestination.key]


class TestTyping:
    def test_a_subclass_keeps_its_type_and_a_plain_class_is_the_anchor(self):
        @il.connection(name="Typed")
        class FakeTyped(DecoratorConnection):
            pass

        @il.connection
        class FakeUntyped:
            host: str = "x"

        assert_type(FakeTyped, type[FakeTyped])
        assert_type(FakeUntyped, type[il.Connection])
        assert_type(il.asset(tags=["x"]), Decorator[il.Asset])
```

In `packages/interloper-core/tests/serializable/test_base.py`, `TestBuildClass` keeps its behaviour tests (field defaults reach instances, parent default untouched, explicit instance value wins, class-body key survives decorator field defaults, decorator key wins, name derived without a key, genuine subclass derives its own key) and adds:

```python
    def test_a_class_body_key_survives_a_decorator_classvar(self):
        @il.connection(name="Search Ads 360")
        class FakeSearchAds360Connection(il.Connection):
            key = "search_ads_360_connection"

        assert FakeSearchAds360Connection.key == "search_ads_360_connection"

    def test_a_decorated_subclass_round_trips(self):
        built = il.destination(tags=["Cloud"])(FakeRoundTripDestination)
        instance = built()

        restored = il.Destination.from_spec(instance.to_spec())

        assert type(restored) is built
        assert il.Serializable.resolve_path(built.classpath()) is built
```

with a module-level fixture (resolution needs a module-level class):

```python
class FakeRoundTripDestination(il.MemoryDestination):
    """A destination a test decorates after the fact."""
```

`TestBuildClass` goes through the decorators only, so its existing tests need no change.

In `packages/interloper-core/tests/source/test_decorator.py`, add:

```python
    def test_a_decorated_source_subclass_keeps_its_assets(self):
        built = il.source(tags=["Shop"])(FakeDecoratedShop)

        assert built.orders is FakeDecoratedShop.orders
        assert type(built().orders).key == "orders"
        assert il.Serializable.resolve_path(built.orders.classpath()) is built.orders
```

with a module-level fixture:

```python
class FakeDecoratedShop(il.Source):
    """A source decorated after the fact."""

    class Orders(il.Asset):
        """Orders."""

        def data(self) -> list[dict[str, Any]]:
            return []
```

In `packages/interloper-core/tests/connection/test_decorator.py`, the `oauth` test's match becomes `r"OAuthConnection declares it"`.

- [ ] **Step 3: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/component/test_decorator.py packages/interloper-core/tests/serializable/test_base.py packages/interloper-core/tests/sourc?/test_decorator.py packages/interloper-core/tests/connection/test_decorator.py -q -p no:cacheprovider`
Expected: FAIL (`Decorator` missing, `build_class` signature, `test_a_plain_class_setting_is_validated_too`).

- [ ] **Step 4: `build_class`**

Replace `Serializable.build_class` and delete `_override_defaults` (its body now lives in `_sort_settings`):

```python
    @classmethod
    def build_class(cls, body: ClassBody, **settings: Any) -> type[Self]:
        """Build a new class from a decorated target's body and a decorator's settings.

        The one construction every decorator goes through: decorators build
        classes, they never modify one. Everything goes into the namespace of
        one new class, so the metaclass sees field defaults as fields and
        ``__init_subclass__`` collects relations and validates settings where
        they are written. A target already extending this class is the base of
        the new one, which keeps its key and kind (``rebuild``).

        Args:
            body: What the decorated target contributes.
            **settings: The decorator's settings (see `_sort_settings`).

        Returns:
            The new class.
        """
        entries, annotations = cls._sort_settings(settings)
        base = body.base or cls
        namespace: dict[str, Any] = {
            **body.members,
            **entries,
            "__module__": body.module,
            "__qualname__": body.qualname,
            "__annotations__": {**annotations, **body.annotations},
        }
        if body.doc:
            namespace["__doc__"] = body.doc
        return cast("type[Self]", type(base)(body.name, (base,), namespace, rebuild=body.base is not None))
```

The metaclass is called explicitly (`type(base)`), so `AssetMeta` builds an asset and the `rebuild` keyword reaches `__init_subclass__`.

- [ ] **Step 5: `Decorator`**

Rewrite `packages/interloper-core/src/interloper/component/decorator.py`:

```python
"""Decorator: the one way a decorator builds a class of its kind."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar, overload

from interloper.serializable import ClassBody, Serializable

SerializableT = TypeVar("SerializableT", bound=Serializable)
SubclassT = TypeVar("SubclassT", bound=Serializable)


class Decorator(Generic[SerializableT]):
    """A decorator building classes of one kind, used bare or with settings.

    Called with a target (a class or a function), it reads the target into a
    `ClassBody` and builds a new class of its kind from it with the settings it
    carries. Called with settings, it returns a decorator carrying them, so the
    parenthesised form needs no nested function:

    ```py
    asset = Decorator(Asset)

    @asset
    def rows(): ...

    @asset(tags=["Report"], relations={"destinations": [BigQueryDestination]})
    def users_stats(): ...
    ```

    What the settings may be is the kind's own: its public ClassVars, its
    field defaults and, for a component, ``relations=``. A class that already
    extends the kind is the one consulted, so a subclass's own settings
    (``oauth`` on an ``OAuthConnection``) are accepted.
    """

    def __init__(self, anchor: type[SerializableT], **settings: Any) -> None:
        """Make a decorator for one kind.

        Args:
            anchor: The kind the decorator builds.
            **settings: The settings every class it builds receives.
        """
        self.anchor = anchor
        self.settings = settings

    @overload
    def __call__(self, target: type[SubclassT], /) -> type[SubclassT]: ...
    @overload
    def __call__(self, target: type | Callable[..., Any], /) -> type[SerializableT]: ...
    @overload
    def __call__(self, /, *, relations: dict[str, Any] | None = ..., **overrides: Any) -> Decorator[SerializableT]: ...
    def __call__(self, target: Any = None, /, **settings: Any) -> Any:
        """Build a class from a target, or return a decorator carrying more settings.

        Args:
            target: The decorated class or function; ``None`` in the
                parenthesised form.
            **settings: More settings, added to the ones this decorator carries.

        Returns:
            The built class, or the decorator carrying the settings.
        """
        settings = {**self.settings, **settings}
        if target is None:
            return Decorator(self.anchor, **settings)
        body = ClassBody.of(target, self.anchor)
        return (body.base or self.anchor).build_class(body, **settings)
```

Export `Decorator` from `interloper/component/__init__.py` (`__all__` too). If the component package's `__init__` would import `serializable` before `component.base` in a cycle, import `Decorator` last there.

- [ ] **Step 6: `Asset.body_from_function`**

In `packages/interloper-core/src/interloper/asset/base.py` (imports `functools`; `from interloper.serializable import ClassBody` at runtime):

```python
    @classmethod
    def body_from_function(cls, fn: Callable[..., Any]) -> ClassBody:
        """Read a decorated function into an asset body: the function becomes ``data()``.

        A function whose first parameter is ``self`` is a method asset: it
        receives the asset's source as ``self``. The generated ``data()`` wraps
        the function in the `functools.wraps` sense, so relation inference reads
        the original's annotations through ``__wrapped__`` and the stamped
        ``__signature__`` is what the parameters read as; an async function
        gives an async ``data()``, which the run awaits on its loop.

        Args:
            fn: The decorated function.

        Returns:
            The body carrying the generated ``data()``.
        """
        signature = inspect.signature(fn)
        method = next(iter(signature.parameters), None) == "self"
        if inspect.iscoroutinefunction(fn):

            async def data(self: Asset, **kwargs: Any) -> Any:
                return await fn(*([self.source] if method else []), **kwargs)

        else:

            def data(self: Asset, **kwargs: Any) -> Any:
                return fn(*([self.source] if method else []), **kwargs)

        functools.update_wrapper(data, fn)
        if not method:
            self_parameter = inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
            signature = signature.replace(parameters=[self_parameter, *signature.parameters.values()])
        data.__signature__ = signature  # ty: ignore[invalid-assignment]
        return ClassBody.from_function(fn, members={"data": data})
```

`View`, `Reference` and `IbisView` inherit it.

- [ ] **Step 7: `Source.body_from_function`**

In `packages/interloper-core/src/interloper/source/base.py`:

```python
    @classmethod
    def body_from_function(cls, fn: Callable[..., Any]) -> ClassBody:
        """Read a decorated function into a source body: what it returns are the assets.

        The function is called once, for the asset classes it returns (one or a
        list); its annotated parameters become configuration fields, their
        defaults the field defaults.

        Args:
            fn: The decorated function.

        Returns:
            The body carrying ``asset_types`` and the fields.
        """
        returned = fn()
        asset_types = returned if isinstance(returned, list) else [returned] if isinstance(returned, type) else []
        members: dict[str, Any] = {"asset_types": asset_types}
        annotations: dict[str, Any] = {}
        hints = getattr(fn, "__annotations__", {})
        for name, parameter in inspect.signature(fn).parameters.items():
            if name in ("self", "context", "kwargs") or name not in hints:
                continue
            annotations[name] = hints[name]
            if parameter.default is not inspect.Parameter.empty:
                members[name] = parameter.default
        return ClassBody.from_function(fn, members=members, annotations=annotations)
```

- [ ] **Step 8: The decorators become instances**

Each decorator module keeps a module docstring and becomes its instance plus an attribute docstring carrying today's prose and examples (the `Args:`/`Returns:` sections become prose: what the settings are, what is returned):

- `asset/decorator.py`: `asset = Decorator(Asset)`, `view = Decorator(View)`, and

```python
def reference(*, table: str, **overrides: Any) -> Decorator[Reference]:
    """Create a Reference subclass from a decorated function: an asset over a table that already exists.

    (today's docstring prose and example, unchanged)

    Args:
        table: The physical table name, used as written, wildcard and all.
        **overrides: Definition metadata and behaviour as for `asset`
            (``schema``, ``tags``, ``dataset``, ``partitioning``, ...).

    Returns:
        A decorator turning the function into a Reference subclass.
    """
    return Decorator(Reference, table_name=table, **overrides)
```

- `source/decorator.py`: `source = Decorator(Source)`.
- `connection/decorator.py`: `connection = Decorator(Connection)` (the `oauth` check goes: `_refusal` names `OAuthConnection`).
- `config/decorator.py`: `config = Decorator(Config)`. `destination/decorator.py`: `destination = Decorator(Destination)`. `schema/decorator.py`: `schema = Decorator(Schema)`.
- `interloper_sql/view/decorator.py`: `ibis_view = Decorator(IbisView)`.

Removed: `decorate`, `declare`, `_route`, `_relations`, `_is_component`, `_IDENTITY_FIELD`, `ComponentT`, `build_asset_class`, `_build_view_class`, `_build_reference_class`, `_build_ibis_view_class`, `_build_source`, `_build_source_from_fn`, `_build_source_from_class`, `_build_connection`, `_build_config`, `_build_destination`, the `ConnectionT`/`ConfigT`/`DestinationT` TypeVars and the overload triplets. `create_model` is no longer imported.

- [ ] **Step 9: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-sql packages/interloper-assets -q -p no:cacheprovider > .superpowers/t2.log 2>&1; tail -3 .superpowers/t2.log`
Expected: PASS.

- [ ] **Step 10: Compare the catalog**

Run: `UV_FROZEN=1 uv run python -c "import json, interloper as il; print(json.dumps(il.Catalog.discover().dump(), sort_keys=True, default=str))" > .superpowers/catalog-after.json && diff -q .superpowers/catalog-before.json .superpowers/catalog-after.json && echo identical`
Expected: `identical`.

- [ ] **Step 11: The docs render the decorators**

Run: `UV_FROZEN=1 uv run zensical build --strict > .superpowers/docs.log 2>&1; echo $?; grep -c "Create an Asset subclass from a decorated function" site/reference/assets/index.html`
Expected: `0`, then a count of at least `1`. If the attribute docstring does not render, record a ruling and fall back to thin module functions carrying the docstring and returning `Decorator(...)(target)`, keeping `Decorator` as the one implementation.

- [ ] **Step 12: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green (ruff, ty on every package, every suite).

- [ ] **Step 13: Checkpoint (no commit)**

---

### Task 3: Ownership is Source's

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/base.py` (`_owned_by_field`, `owned`), `packages/interloper-core/src/interloper/source/base.py` (`_owned_by_field`, `model_post_init`)
- Test: `packages/interloper-core/tests/component/test_base.py` (`TestOwned`), `packages/interloper-core/tests/source/test_base.py`

**Interfaces:**
- Produces: `Component._owned_by_field() -> dict[str, list[Component]]` returns `{}`; `Source._owned_by_field()` returns `{"assets": list(self.assets)}`; `Component.owned` unchanged in signature.

- [ ] **Step 1: Write the failing tests**

Replace `TestOwned` in `packages/interloper-core/tests/component/test_base.py`:

```python
class TestOwned:
    def test_a_component_owns_nothing_even_when_a_held_one_points_at_it(self) -> None:
        child = FakeOtherComponent(value="c")
        holder = FakeComponent(child=child)
        child.parent = holder

        assert holder.owned == []
        assert "child" in (holder.to_spec().init or {})
```

(the second assertion: the held component travels as an ordinary field value, not as an owned map).

Append to the source tests in `packages/interloper-core/tests/source/test_base.py` (in the class holding `test_model_post_init_skips_auto_instantiation_when_assets_supplied`):

```python
    def test_a_source_owns_its_assets(self):
        source = FakeSourceWithAssets()

        assert source.owned == source.assets
        assert all(asset.parent is source for asset in source.assets)

    def test_an_asset_of_another_source_is_refused(self):
        other = FakeSourceWithAssets()

        with pytest.raises(ValidationError, match=r"already belongs to source"):
            FakeSourceWithAssets(assets=[other.assets[0]])

    def test_a_copy_owns_copies_of_the_assets(self):
        source = FakeSourceWithAssets()

        copy = source(dataset="other")

        assert [asset.id for asset in copy.assets] == [asset.id for asset in source.assets]
        assert all(asset.parent is copy for asset in copy.assets)
        assert all(asset.parent is source for asset in source.assets)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/component/test_base.py packages/interloper-core/tests/sourc?/test_base.py -q -p no:cacheprovider -k "Owned or owns or another_source or copy_owns"`
Expected: FAIL (the holder owns the child today; the foreign asset is taken).

- [ ] **Step 3: Implement**

`Component._owned_by_field`:

```python
    def _owned_by_field(self) -> dict[str, list[Component]]:
        """The components this one owns, keyed by the field that holds them; none for a component.

        Ownership is the part-of relationship, and a source's assets are its one
        case: `Source` overrides this. An owned component travels inside its
        owner's spec and is copied with it.

        Returns:
            Field name to the owned components it holds; empty.
        """
        return {}
```

The `owned` property docstring says: the components this one owns (a source's assets), from `_owned_by_field`.

`Source`:

```python
    def _owned_by_field(self) -> dict[str, list[Component]]:
        """A source owns its assets.

        Returns:
            ``{"assets": ...}``, the assets in order.
        """
        return {"assets": list(self.assets)}
```

and in `Source.model_post_init`, the wiring loop (the spec's message suggested giving the source a copy, but a copy keeps its owner, so the message only states the fact):

```python
        for asset in self.assets:
            if asset.parent is not None and asset.parent is not self:
                raise ConfigError(f"Asset '{asset.key}' already belongs to source '{asset.parent.qualified_key}'")
            asset.parent = self
```

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-db -q -p no:cacheprovider > .superpowers/t3.log 2>&1; tail -3 .superpowers/t3.log`
Expected: PASS.

- [ ] **Step 5: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 6: Checkpoint (no commit)**

---

### Task 4: No pushes; a job's destinations, where the job runs

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/base.py` (remove `trickle`, `on_rebind`, the call in `_replace_binding`, docstring mentions), `packages/interloper-core/src/interloper/source/base.py` (remove `on_rebind`), `packages/interloper-core/src/interloper/job/base.py`
- Test: `packages/interloper-core/tests/component/test_base.py` (remove `TestOnRebind`, `TestTrickle`), `packages/interloper-core/tests/source/test_base.py`, `packages/interloper-core/tests/job/test_base.py`

**Interfaces:**
- Produces: `Job.operations()` unchanged in signature; `Job._run_as(target) -> Source | Asset`.

- [ ] **Step 1: Write the failing tests**

Replace `TestWorkloadDefaults` in `packages/interloper-core/tests/job/test_base.py`:

```python
class TestWorkloadDefaults:
    """A job's destinations are the default for its targets, where the job runs."""

    @staticmethod
    def _destinations(job: il.Job) -> dict[str, list[il.Destination]]:
        return {operation.key: list(operation.destinations) for operation in job.operations()}

    def test_a_target_without_destinations_runs_with_the_jobs(self):
        destination = FakeJobDestination()
        target = FakeSource()

        job = il.Job(targets=[target], destinations=[destination])

        assert all(found == [destination] for found in self._destinations(job).values())
        assert target.destinations == []

    def test_a_targets_own_destinations_win(self):
        own = FakeJobDestination()
        job = il.Job(targets=[FakeSource(destinations=[own])], destinations=[FakeOtherJobDestination()])

        assert all(found == [own] for found in self._destinations(job).values())

    def test_an_owned_asset_target_keeps_its_sources_destinations(self):
        own = FakeJobDestination()
        asset = FakeSource(destinations=[own]).assets[0]

        job = il.Job(targets=[asset], destinations=[FakeOtherJobDestination()])

        assert self._destinations(job) == {asset.key: [own]}
        assert asset.bound("destinations") == []

    def test_an_owned_asset_target_without_destinations_runs_with_the_jobs(self):
        destination = FakeJobDestination()
        source = FakeSource()
        asset = source.assets[0]

        [operation] = il.Job(targets=[asset], destinations=[destination]).operations()

        assert list(operation.destinations) == [destination]
        assert operation.parent is source
        assert operation.dataset == asset.dataset
        assert asset.bound("destinations") == []

    def test_a_standalone_asset_target_runs_with_the_jobs(self):
        destination = FakeJobDestination()

        job = il.Job(targets=[FakeStandaloneAsset()], destinations=[destination])

        assert all(found == [destination] for found in self._destinations(job).values())

    def test_destinations_given_after_construction_still_apply(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeSource()])

        job.destinations = [destination]

        assert all(found == [destination] for found in self._destinations(job).values())

    def test_two_jobs_sharing_a_target_each_run_it_with_their_own(self):
        target = FakeSource()
        first, second = FakeJobDestination(), FakeOtherJobDestination()

        one = il.Job(targets=[target], destinations=[first])
        two = il.Job(targets=[target], destinations=[second])

        assert all(found == [first] for found in self._destinations(one).values())
        assert all(found == [second] for found in self._destinations(two).values())
        assert target.destinations == []

    def test_compiling_twice_yields_the_same_ids_and_leaves_the_targets_alone(self):
        target = FakeSource()
        job = il.Job(targets=[target], destinations=[FakeJobDestination()])

        first, second = job.operations(), job.operations()
        first[0].enabled = False

        assert [operation.id for operation in first] == [operation.id for operation in second]
        assert second[0].enabled
        assert all(asset.enabled for asset in target.assets)
```

In `packages/interloper-core/tests/source/test_base.py`: `test_trickles_into_bound_destinations` and `test_destination_bound_after_construction_still_trickles` become:

```python
    def test_a_destination_keeps_its_own_connection(self):
        class FakeConnectedDestination(FakeDestination):
            connection: FakeConnection | None = il.Relation(FakeConnection, optional=True)

        destination = FakeConnectedDestination()
        source = FakeConnectedSource(connection=FakeConnection(token="secret"), destinations=[destination])

        assert destination.connection is None
        assert source.destinations[0].connection is None
```

(with `FakeConnectedSource` lifted to module level, declaring `connection: FakeConnection`). `test_destination_assigned_after_construction_still_trickles` stays as is (assets read destinations through the source; rename it `test_a_destination_assigned_after_construction_reaches_the_assets`).

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/job/test_base.py packages/interloper-core/tests/sourc?/test_base.py -q -p no:cacheprovider`
Expected: FAIL (`target.destinations` is the job's today; the destination receives the source's connection).

- [ ] **Step 3: Implement**

`Component`: delete `trickle` and `on_rebind`; `_replace_binding` ends at `self._bound[name] = deduplicated`; docstrings mentioning `on_rebind` or `trickle` (`__init__`, `_targets`, `bind`, `unbind`, `Relation.__set__` in `relation.py`) drop the mention. `Source.on_rebind` is deleted.

`Job`:

```python
    def operations(self) -> list[Operation]:
        """The targets' operations, each target run with this job's bindings where it resolves none.

        Returns:
            Every operation the job's targets provide.
        """
        return [operation for target in self.targets for operation in self._run_as(target).operations()]

    def _run_as(self, target: Source | Asset) -> Source | Asset:
        """The target as this job runs it: a copy carrying the job's bindings it lacks, or itself.

        For every relation the job binds other than ``targets``, a target that
        declares a relation of that name and resolves nothing under it (so a
        source's own destinations, and an owned asset's source-provided ones,
        win) receives the job's targets it accepts: every one for a ``many``
        relation, the first otherwise. The target itself is never modified.

        Args:
            target: One of the job's targets.

        Returns:
            The target, or a copy of it (same ids) carrying what it lacks.
        """
        lent: dict[str, Any] = {}
        for name in type(self).relations:
            relation = type(target).relations.get(name)
            if name == "targets" or relation is None or target._targets(name):
                continue
            accepted = [
                held for held in self._targets(name) if relation.accepts(held.kind, held.identity, owner=target.identity)
            ]
            if accepted:
                lent[name] = accepted if relation.many else accepted[0]
        return target(**lent) if lent else target
```

The `Job` class docstring's last paragraph says: a job carries workload-level defaults; every relation the job holds is the default for the same relation on any target that resolves none, applied where the job runs (`operations`), never written into the target.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-scheduler packages/interloper-db packages/interloper-assets -q -p no:cacheprovider > .superpowers/t4.log 2>&1; tail -3 .superpowers/t4.log`
Expected: PASS (the campaign-matcher manifest test reads the destinations through the DAG's operations).

- [ ] **Step 5: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 6: Checkpoint (no commit)**

---

### Task 5: Reconstruction without a component special case

**Files:**
- Modify: `packages/interloper-core/src/interloper/serializable/base.py` (`Spec.reconstruct`, `Spec._load`, `Serializable.from_init`), `packages/interloper-core/src/interloper/component/base.py` (`Component.from_init`)
- Test: `packages/interloper-core/tests/serializable/test_base.py`, `packages/interloper-core/tests/component/test_base.py`

**Interfaces:**
- Produces: `Serializable.from_init(init: dict[str, Any], context: SerializationContext) -> Self`; `Component.from_init(...)`; `Spec._load(value, catalog, context) -> Any`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-core/tests/serializable/test_base.py`:

```python
class TestFromInit:
    def test_a_plain_serializable_is_built_from_its_init(self):
        context = SerializationContext()

        normalizer = il.Normalizer.from_init({"flatten_max_level": 2}, context)

        assert normalizer.flatten_max_level == 2
        assert context.components == {}
```

Append to `packages/interloper-core/tests/component/test_base.py`:

```python
class TestFromInit:
    def test_a_component_holds_its_references_and_is_registered(self) -> None:
        context = SerializationContext()

        widget = Widget.from_init({"connection": {"ref": "c1"}}, context)

        assert context.components[widget.id] is widget
        assert widget.bound("connection") is None
```

(import `SerializationContext` from `interloper.serializable` where not yet imported).

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/serializable/test_base.py packages/interloper-core/tests/component/test_base.py -q -p no:cacheprovider -k FromInit`
Expected: FAIL (`from_init` missing).

- [ ] **Step 3: Implement**

```python
    @classmethod
    def from_init(cls, init: dict[str, Any], context: SerializationContext) -> Self:
        """Build an instance from a spec's loaded init, as one step of reconstructing a document.

        Args:
            init: Constructor keyword arguments, nested specs already built.
            context: The document being read; a plain serializable needs nothing from it.

        Returns:
            The instance.
        """
        return cls(**init)
```

`Component.from_init`:

```python
    @classmethod
    def from_init(cls, init: dict[str, Any], context: SerializationContext) -> Self:
        """Build a component from a loaded init, holding its references back for the document to bind.

        Args:
            init: Constructor keyword arguments, nested specs already built and
                every ``{"ref": id}`` still a mapping.
            context: The document being read: it keeps the held references and
                the instance, so a later reference finds it.

        Returns:
            The component, its references not yet bound.
        """
        instance = cls(**context.hold(init))
        context.add(instance)
        return instance
```

`Spec.reconstruct` becomes:

```python
        owns = context is None
        context = SerializationContext(resolve=resolve) if context is None else context
        cls = self._resolve(catalog)
        init: dict[str, Any] = {"id": self.id} if self.id else {}
        init.update({name: self._load(value, catalog, context) for name, value in (self.init or {}).items()})
        instance = cls.from_init(init, context)
        if owns:
            context.bind()
        return instance
```

with `_resolve(catalog) -> type[Serializable]` (a `key` through `Component.resolve_key`, imported lazily as today; a `path` through `Serializable.resolve_path`) and `_load(value, catalog, context) -> Any` (today's closure: a nested spec mapping is reconstructed, a mapping or list walked, anything else returned). The docstring's description of the two passes stays.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-db -q -p no:cacheprovider > .superpowers/t5.log 2>&1; tail -3 .superpowers/t5.log`
Expected: PASS.

- [ ] **Step 5: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 6: Checkpoint (no commit)**

---

### Task 6: Docs, skills, live verification

**Files:**
- Modify: `docs/extending/components.md`, `docs/guide/sources.md`, `docs/guide/jobs.md`, `docs/guide/specs.md`, `plugins/interloper/skills/interloper-upgrade/SKILL.md`, any other file Step 1 finds

- [ ] **Step 1: Find every stale mention**

Run: `grep -rnE "build_class\(|decorate\(|declare\(|build_asset_class|on_rebind|trickle|_owned_by_field|component\.decorator import" docs plugins examples README.md packages/*/src --include='*.md' --include='*.py' | grep -v docs/superpowers`
Expected: only the files listed above, and `packages/interloper-core/src/interloper/serializable/base.py` / `component/base.py` lines this phase wrote.

- [ ] **Step 2: Rewrite**

- `extending/components.md`:
  - the Serializable bullet on `build_class` describes `build_class(body, **settings)`: one new class from a `ClassBody`, everything in its namespace;
  - the operations table loses `trickle`; the `on_rebind` extension point and its example go;
  - "Writing a kind" step 4 goes (renumber);
  - "Writing a decorator" becomes: `report = Decorator(Report)` with an attribute docstring, what the settings are (the anchor's public ClassVars, field defaults, `relations=`), `body_from_function` for a kind that decorates functions, and that nothing is hand-listed;
  - the paragraph on relation assignment drops "whatever the owner cascades into its children is cascaded again".
- `guide/sources.md`: the sentence on trickling into destinations becomes: a destination configures its own connection; an asset reads its source's destinations, nothing is written into them.
- `guide/jobs.md`: cascading is "a job's destinations are the default for every target that resolves none, applied when the job runs; the targets themselves are never changed", and a job subclass's further relation cascades the same way into its targets.
- `guide/specs.md`: "the job's cascade into every target that has none" stays true; reword to "the job's destinations are the default for every target that has none".

- [ ] **Step 3: Upgrade table**

Add one row above the phase 3 row of `plugins/interloper/skills/interloper-upgrade/SKILL.md`:

| unreleased (core phase 4) | a destination taking its source's connection; a job writing its destinations into its targets (`job.targets[0].destinations`); `Component.trickle`, `Component.on_rebind`; `interloper.component.decorator.decorate`/`declare`, `build_asset_class`, `Serializable.build_class(decorated, classvars=, fields=)`, `Component._classvars`; giving a source another source's asset | bind `connection` on the destination; the job's destinations apply when it runs (`job.operations()`, `il.DAG(job)`); nothing replaces the hook, a component is fields plus relations; `Decorator(Kind)` (the public decorators are its instances, same call forms), `Serializable.build_class(ClassBody, **settings)`, `Serializable._classvars`; build the source's own assets (a copy keeps its owner) |

- [ ] **Step 4: Full check and docs build**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.
Run: `UV_FROZEN=1 uv run zensical build --strict > .superpowers/docs.log 2>&1; echo $?`
Expected: `0`.

- [ ] **Step 5: Live verification**

- From `dev/`: `UV_FROZEN=1 uv run interloper run -f /Users/g/Dev/internal/interloper-manifests/manifests/demo.yaml --dry-run` and `UV_FROZEN=1 uv run interloper run -f ../examples/campaign_matcher.yaml --dry-run` (with `GCP_SERVICE_ACCOUNT_KEY=placeholder`): each prints its plan; the campaign matcher's assets list the job's BigQuery destination.
- Backend on :3100 (the `DEV_ENV` variables and `interloper app --api --cron --worker --reaper`, no `--dev`; read `alembic_version` first and leave it as found), mint a session, run the demo source for one day through the API: the run succeeds with every operation completed. Tear down and delete the session row.

- [ ] **Step 6: Checkpoint (no commit)**
