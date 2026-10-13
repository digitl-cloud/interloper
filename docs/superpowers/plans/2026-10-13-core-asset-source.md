# Core Phase 2: Asset and Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One cascade rule from a source to its assets (read on demand, never copied), one way to reach an asset, `Asset` divided by owner, and one reconfiguration on `Component`.

**Architecture:** Inherited settings are Pydantic fields marked `x-inherit` with a read-through descriptor that asks the owner (`Component.inherited`); relations read through the owner the same way. Asset classes are their own descriptor through `AssetMeta.__get__`. Materializing is one pipeline in `Asset` (`_produce` then `_land`) whose kind-specific steps are three hooks (`_upstream`, `_output`, `_land`), backed by two value types (`Output`, `Upstream`). `Component.__call__(**changes)` replaces both hand-written `__call__`s.

**Tech Stack:** Python 3.12 (floor 3.10), Pydantic 2.12, pytest, ruff, ty, uv workspace.

**Spec:** `docs/superpowers/specs/2026-10-13-core-asset-source-design.md`

## Global Constraints

- No em dash character in any text: code, comments, docstrings, docs, messages, tests.
- Google-style docstrings, complete: every module, class, function and method, private helpers, `__init__` and dunders included; `Args:` lists every parameter, `Returns:`/`Yields:`/`Raises:` whenever they apply (`Raises:` only for an explicit `raise` in the body). Component docstrings that ship as product copy (decorated assets and sources) stay prose.
- Names written in full (`context`, `destination`, `connection`, never `ctx`, `dest`, `conn`).
- Comments only for a non-obvious why; no attribute-level comments; no comments narrating the change.
- Tests mirror the source layout one-to-one: `src/interloper/<pkg>/<module>.py` is tested in `tests/<pkg>/test_<module>.py`. No new `test_<feature>.py` files.
- Run uv with `UV_FROZEN=1`; never change `uv.lock`.
- Never commit, amend or push. Work rests uncommitted until Guillaume asks.
- Persisted contracts unchanged: the spec YAML shape (`assets` as key to init, `{ref: id}`), the definition JSON shape, event type strings, the CLI. One `x-*` extra is added: `x-inherit`.
- Every task ends with `UV_FROZEN=1 make check-python` green (ruff, ty, pytest).
- Ruff line length 120.

## Review Focus

1. A subclass redeclaring an inherited field (what `@il.asset(dataset=...)` and `@il.reference(dataset=...)` generate) loses the `x-inherit` marker on its own `FieldInfo`; it must keep reading through its owner, templates included. Pinned in Task 1 (`test_a_subclass_redeclaring_the_field_keeps_inheriting`) and Task 2 (`test_a_dataset_template_names_a_field_of_the_source`).
2. Platform rows persist blank own values (`"dataset": ""`, `"materialization_strategy": null`); hydrating them must inherit, not pin a blank. Pinned in Task 2 (`test_a_blank_persisted_value_inherits`).
3. The multi-process runner pickles operations; a pickled and restored source must keep reaching its assets and reading through itself. Pinned in Task 4 (`test_a_pickled_source_keeps_its_assets_and_their_reads`).
4. Reconfiguring a source whose assets bind each other must point the copy's assets at each other, not at the original's, and share every outside target rather than duplicate it. Pinned in Task 7 (`test_a_copy_rewires_siblings_and_shares_outside_targets`).
5. A view referencing an upstream absent from the run's DAG must still reference it (a reference needs no node), while a reading asset skips such an upstream with its warning. Pinned in Task 6 (`test_a_view_references_an_upstream_absent_from_the_dag`).

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `packages/interloper-core/src/interloper/resource/fields.py` | `inherit=True` flag on the field factories | 1 |
| `packages/interloper-core/src/interloper/component/inherited.py` (new) | `InheritedSetting`, the read-through descriptor, and its installation | 1 |
| `packages/interloper-core/src/interloper/component/base.py` | `Component.inherited`, installation hook, `_targets`, `resolve`, `validate_relations`, `to_spec`, `__call__` | 1, 3, 7 |
| `packages/interloper-core/src/interloper/component/relation.py` | `Relation.__get__` reads through the owner | 3 |
| `packages/interloper-core/src/interloper/serializable/base.py` | `_fields_init` serializes stored values | 1 |
| `packages/interloper-core/src/interloper/operation/base.py` | `retry` marked inherited | 2 |
| `packages/interloper-core/src/interloper/asset/base.py` | `AssetMeta`, inherited fields, `_parameters`, `default_destination`, `_produce`, the three hooks, `_upstreams` | 2, 4, 5, 6, 7 |
| `packages/interloper-core/src/interloper/asset/output.py` (new) | `Output` and `Output.from_result` | 5 |
| `packages/interloper-core/src/interloper/asset/upstream.py` | `Upstream.read`, `Upstream.reference` | 6 |
| `packages/interloper-core/src/interloper/asset/view.py` | `View` on the hooks | 5, 6 |
| `packages/interloper-core/src/interloper/source/base.py` | `Source.inherited`, construction, collection, removals | 2, 3, 4, 7 |
| `packages/interloper-core/src/interloper/source/decorator.py` | stop stamping the owning source | 4 |
| `packages/interloper-sql/src/interloper_sql/view/base.py` | `IbisView` on the hooks | 6 |
| docs, plugin skills, interloper-assets tests | surface | 8 |

---

### Task 1: Inherited settings

**Files:**
- Modify: `packages/interloper-core/src/interloper/resource/fields.py` (`_extra`)
- Create: `packages/interloper-core/src/interloper/component/inherited.py`
- Modify: `packages/interloper-core/src/interloper/component/base.py` (`__pydantic_init_subclass__`, new `inherited`)
- Modify: `packages/interloper-core/src/interloper/serializable/base.py` (`_fields_init`)
- Test: `packages/interloper-core/tests/component/test_inherited.py` (new, mirrors the new module), `packages/interloper-core/tests/resource/test_fields.py`

**Interfaces:**
- Produces: `InheritedSetting(name: str)` descriptor; `InheritedSetting.install(component: type[Component]) -> None`; `Component.inherited(self, child: Component, name: str, own: Any) -> Any`; field factories accept `inherit=True` (renders `"x-inherit": True`); `Serializable._fields_init` reads `self.__dict__`.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-core/tests/component/test_inherited.py`:

```python
"""Tests for ``interloper.component.inherited``."""

from typing import Any

from pydantic import Field

import interloper as il
from interloper.component.base import Component
from interloper.component.inherited import InheritedSetting


class Holder(Component):
    """An owner declaring the settings its children inherit."""

    label: str | None = None
    retries: int | None = None


class Plain(Component):
    """An owner declaring none of them."""


class Held(Component):
    """A child inheriting two settings."""

    label: str | None = il.InputField(default=None, inherit=True)
    retries: int | None = Field(default=None, json_schema_extra={"x-inherit": True})
    note: str | None = None


class Narrowed(Held):
    """A subclass redeclaring an inherited field, as a decorator does."""

    label: str | None = "declared"


def _held(owner: Component | None = None, cls: type[Held] = Held, **values: Any) -> Held:
    held = cls(**values)
    held.parent = owner
    return held


class TestRead:
    def test_reads_its_own_value(self):
        assert _held(Holder(label="owner"), label="own").label == "own"

    def test_reads_the_owners_value_while_unset(self):
        assert _held(Holder(label="owner")).label == "owner"

    def test_follows_the_owner_after_construction(self):
        owner = Holder(label="before")
        held = _held(owner)
        owner.label = "after"
        assert held.label == "after"

    def test_a_standalone_component_reads_none_while_unset(self):
        assert Held().label is None

    def test_a_blank_string_is_unset(self):
        assert _held(Holder(label="owner"), label="").label == "owner"

    def test_an_owner_without_the_field_hands_nothing(self):
        assert _held(Plain()).retries is None

    def test_an_unmarked_field_never_reads_through(self):
        held = _held(Holder(label="owner"))
        assert held.note is None

    def test_the_class_reads_none(self):
        assert Held.label is None


class TestOwnerSeam:
    def test_the_owner_receives_the_stored_value(self):
        received: list[tuple[str, Any]] = []

        class Recording(Holder):
            def inherited(self, child: Component, name: str, own: Any) -> Any:
                received.append((name, own))
                return "decided"

        assert _held(Recording(), label="own").label == "decided"
        assert received == [("label", "own")]


class TestStorage:
    def test_dump_and_spec_see_stored_values_only(self):
        held = _held(Holder(label="owner", retries=3))
        assert held.model_dump()["label"] is None
        assert "label" not in (held.to_spec().init or {})
        assert "retries" not in (held.to_spec().init or {})

    def test_an_assignment_stores_an_own_value(self):
        held = _held(Holder(label="owner"))
        held.label = "own"
        assert held.__dict__["label"] == "own"
        assert "label" in held.model_fields_set


class TestInstallation:
    def test_every_marked_field_gets_the_descriptor(self):
        assert isinstance(vars(Held)["label"], InheritedSetting)
        assert isinstance(vars(Held)["retries"], InheritedSetting)
        assert "note" not in vars(Held) or not isinstance(vars(Held)["note"], InheritedSetting)

    def test_a_subclass_redeclaring_the_field_keeps_inheriting(self):
        assert isinstance(vars(Narrowed)["label"], InheritedSetting)
        assert _held(Holder(label="owner"), cls=Narrowed).label == "declared"
        assert _held(Holder(label="owner"), cls=Narrowed, label=None).label == "owner"
```

Append to `packages/interloper-core/tests/resource/test_fields.py`:

```python
def test_inherit_marks_the_field():
    field = il.InputField(default=None, inherit=True)
    assert field.json_schema_extra["x-inherit"] is True


def test_a_field_is_not_inherited_by_default():
    field = il.InputField(default=None)
    assert "x-inherit" not in field.json_schema_extra
```

(Add `import interloper as il` at the top of `test_fields.py` if it is not imported yet.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/component/test_inherited.py packages/interloper-core/tests/resource/test_fields.py -q`
Expected: collection error `ModuleNotFoundError: No module named 'interloper.component.inherited'`; the field tests fail with `TypeError` (unexpected keyword `inherit`) or a missing key.

- [ ] **Step 3: Add the flag to `_extra`**

In `packages/interloper-core/src/interloper/resource/fields.py`, inside `_extra`, after the `discriminator` lines:

```python
    if kwargs.pop("inherit", False):
        extra["x-inherit"] = True
```

Add to the docstring bullet list, after the `discriminator=True` bullet:

```
    - ``inherit=True`` becomes ``x-inherit``: the field is a setting an owned
      component reads from its owner while it leaves it unset (see
      `InheritedSetting`).
```

and extend the `Returns:` line to name ``x-inherit``.

- [ ] **Step 4: Create the descriptor module**

`packages/interloper-core/src/interloper/component/inherited.py`:

```python
"""Inherited settings: fields an owned component reads through its owner while it leaves them unset.

A field marked ``x-inherit`` (``inherit=True`` on a field factory) stores only
the component's own value, ``None`` while unset. Reading it asks the owner,
through `Component.inherited`, so nothing is copied down and nothing goes
stale; validation, serialization, equality and copies see the stored value
alone, since Pydantic keeps it in the instance ``__dict__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.component.base import Component

_MARKER = "x-inherit"


class InheritedSetting:
    """The read-through descriptor installed over one inherited field.

    A data descriptor, so it wins over the instance ``__dict__`` on reads while
    Pydantic keeps writing there: the stored value is the override, and the
    read is the effective value.
    """

    def __init__(self, name: str) -> None:
        """Bind the descriptor to the field it reads.

        Args:
            name: The field name, under which the value is stored.
        """
        self.name = name

    def __get__(self, instance: Component | None, owner: type | None = None) -> Any:
        """Read the effective value: the owner decides, or the stored value stands.

        Args:
            instance: The component read, or ``None`` for a read on the class.
            owner: The class the descriptor is installed on; unused, part of
                the descriptor protocol. Defaults to ``None``.

        Returns:
            ``None`` on the class. On an owned component, what its owner hands
            it (see `Component.inherited`), given the stored value with a blank
            string read as unset; on a standalone one, the stored value.
        """
        if instance is None:
            return None
        own = instance.__dict__.get(self.name)
        if own == "":
            own = None
        parent = instance.parent
        if parent is None:
            return own
        return parent.inherited(instance, self.name, own)

    def __set__(self, instance: Component, value: Any) -> None:
        """Store an own value, as Pydantic does for a field assignment.

        Args:
            instance: The component written to.
            value: The new own value; ``None`` means unset.
        """
        instance.__dict__[self.name] = value

    @classmethod
    def install(cls, component: type[Component]) -> None:
        """Install the descriptor over every inherited field of a component class.

        A field is inherited when any class in the MRO marks it ``x-inherit``:
        a subclass redeclaring the field (what a decorator generates for
        ``@il.asset(dataset=...)``) carries a fresh ``FieldInfo`` without the
        marker, and must keep reading through its owner.

        Args:
            component: The class Pydantic has just built.
        """
        marked = {
            name
            for base in component.__mro__
            for name, field in getattr(base, "__pydantic_fields__", {}).items()
            if isinstance(field.json_schema_extra, dict) and field.json_schema_extra.get(_MARKER)
        }
        for name in marked & component.model_fields.keys():
            setattr(component, name, cls(name))
```

- [ ] **Step 5: Install it and add the owner seam on `Component`**

In `packages/interloper-core/src/interloper/component/base.py`:

1. Import: `from interloper.component.inherited import InheritedSetting`.
2. At the end of `__pydantic_init_subclass__` (after the annotation loop), add `InheritedSetting.install(cls)`, and add to its docstring: "Every field marked ``x-inherit`` then gets its read-through descriptor (see `InheritedSetting`)."
3. Add, in the `# -- Relations --` section's neighbour `# -- Identity --` section right after `parent`'s setter:

```python
    def inherited(self, child: Component, name: str, own: Any) -> Any:
        """What an owned component reads for one of its inherited settings.

        The one seam a cascade goes through: an owned component never holds a
        copy of what it inherits, it asks its owner on every read. The base
        hands down this component's own effective value; an owner that names
        part of its children's location overrides it (see `Source.inherited`).

        Args:
            child: The owned component reading the setting.
            name: The setting's field name.
            own: The child's stored value, ``None`` while unset.

        Returns:
            *own* when set, else this component's own value under *name* when
            it declares such a field, else ``None``.
        """
        if own is not None:
            return own
        if name in type(self).model_fields:
            return getattr(self, name)
        return None
```

- [ ] **Step 6: Serialize stored values**

In `packages/interloper-core/src/interloper/serializable/base.py`, `_fields_init`: replace `value = getattr(self, name)` with `value = self.__dict__.get(name)`, and add to the docstring: "Stored values are read, not attributes, so an inherited setting left unset is omitted rather than written as whatever the owner hands down."

- [ ] **Step 7: Run the tests to verify they pass**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/component packages/interloper-core/tests/resource packages/interloper-core/tests/serializable -q`
Expected: PASS.

- [ ] **Step 8: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: ruff and ty "All checks passed!", pytest all passed. No field is marked yet, so no existing behaviour changes.

- [ ] **Step 9: Checkpoint (no commit; the work rests uncommitted by standing rule)**

---

### Task 2: Settings cascade from a source to its assets

**Files:**
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (field declarations)
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (`retry`)
- Modify: `packages/interloper-core/src/interloper/source/base.py` (`dataset`, `inherited`, `model_post_init`, `_resolve` removed, `__call__` dataset branch)
- Test: `packages/interloper-core/tests/source/test_base.py`

**Interfaces:**
- Consumes: `inherit=True` and `Component.inherited` (Task 1).
- Produces: `Asset.dataset: str | None`, `Asset.default_destination_key: str | None`, `Asset.materialization_strategy: MaterializationStrategy | None`, `Asset.normalizer`, `Operation.retry`, `Operation.enabled`, all inherited; `Source.dataset: str | None` (default `None`); `Source.enabled: bool = True` (internal); `Source.inherited(child, name, own)`; `Source._check_locations() -> None`.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/source/test_base.py`, add after the existing fixtures (module level; the file has no `from __future__ import annotations`, keep it that way):

```python
@il.source
class Cascade(il.Source):
    """Source whose method assets exercise inheritance."""

    region: str = il.InputField(default="eu")

    @il.asset(materialization_strategy=MaterializationStrategy.RECONCILE)
    def explicit(self) -> list[dict[str, Any]]:
        """Asset declaring the default strategy explicitly."""
        return []

    @il.asset
    def plain(self) -> list[dict[str, Any]]:
        """Asset declaring nothing."""
        return []

    @il.asset(dataset="raw_{region}")
    def templated(self) -> list[dict[str, Any]]:
        """Asset whose dataset names a field of the source."""
        return []
```

and a test class (replacing nothing yet; existing tests are reconciled in Step 5):

```python
class TestInheritance:
    def test_an_explicit_reconcile_survives_a_strict_source(self):
        source = Cascade(materialization_strategy=MaterializationStrategy.STRICT)
        assert source.explicit.materialization_strategy is MaterializationStrategy.RECONCILE
        assert source.plain.materialization_strategy is MaterializationStrategy.STRICT

    def test_a_reconfigured_default_destination_key_reaches_the_assets(self):
        assert Cascade()(default_destination_key="warehouse").plain.default_destination_key == "warehouse"

    def test_a_reconfigured_normalizer_reaches_the_assets(self):
        normalizer = Normalizer()
        assert Cascade()(normalizer=normalizer).plain.normalizer is normalizer

    def test_a_retry_set_after_construction_reaches_the_assets(self):
        source = Cascade()
        source.retry = il.RetryPolicy(max_attempts=3)
        assert source.plain.retry == il.RetryPolicy(max_attempts=3)

    def test_a_dataset_set_after_construction_reaches_the_assets(self):
        source = Cascade()
        source.dataset = "other"
        assert source.plain.dataset == "other"

    def test_a_spec_carries_the_assets_own_values_only(self):
        payload = Cascade(dataset="raw").to_spec().init["assets"]["plain"]
        inherited = {"dataset", "default_destination_key", "materialization_strategy", "normalizer", "retry"}
        assert inherited.isdisjoint(payload)

    def test_an_assets_dataset_defaults_to_the_source_key(self):
        source = Cascade()
        assert source.dataset is None
        assert source.plain.dataset == Cascade.key

    def test_a_dataset_template_names_a_field_of_the_source(self):
        assert Cascade(region="us").templated.dataset == "raw_us"

    def test_a_blank_persisted_value_inherits(self):
        source = Cascade(dataset="raw", assets={"plain": {"dataset": "", "materialization_strategy": None}})
        assert source.plain.dataset == "raw"
        assert source.plain.materialization_strategy is MaterializationStrategy.RECONCILE

    def test_a_disabled_source_disables_its_assets(self):
        source = Cascade(enabled=False)
        assert source.plain.enabled is False
        assert source.plain.__dict__["enabled"] is True

    def test_an_asset_disabled_alone_stays_disabled(self):
        assert Cascade().plain(enabled=False).enabled is False

    def test_the_source_enabled_flag_is_not_a_form_field(self):
        assert "enabled" not in Cascade.config_schema().get("properties", {})

    def test_a_standalone_asset_reads_its_own_values(self):
        @il.asset
        def lonely() -> list[dict[str, Any]]:
            return []

        asset = lonely()
        assert asset.dataset is None
        assert asset.materialization_strategy is None
        assert asset.normalizer is None
```

If `il.RetryPolicy` is not exported, import `from interloper.retry import RetryPolicy` and use it.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source/test_base.py::TestInheritance -q`
Expected: FAIL. At least the explicit-reconcile, reconfigured key and normalizer, after-construction, spec, source-key and standalone tests fail on today's eager copy.

- [ ] **Step 3: Mark the asset settings and retry**

In `packages/interloper-core/src/interloper/asset/base.py`, replace the four state fields:

```python
    dataset: str | None = Field(default=None, json_schema_extra={"x-inherit": True})
    default_destination_key: str | None = Field(default=None, json_schema_extra={"x-inherit": True})
    materialization_strategy: MaterializationStrategy | None = SelectField(
        default=None,
        inherit=True,
        label="Materialization Strategy",
        description="How this asset's data is checked against its schema; inherited from its source when unset.",
        info=(
            "'Reconcile' coerces data to the schema (or infers a schema when "
            "none is declared); 'Strict' requires a schema and fails on any "
            "mismatch. Both write the schema's types."
        ),
    )
    normalizer: Normalizer | None = Field(default=None, json_schema_extra={"x-inherit": True})
```

and update `Asset.__call__`'s annotations for `dataset` and `default_destination_key` (still `None` meaning unchanged until Task 7).

In `packages/interloper-core/src/interloper/operation/base.py`, add `"x-inherit": True` to the `json_schema_extra` dicts of both `retry` and `enabled`, and add to the class docstring paragraph on `enabled`: "``enabled`` and ``retry`` are inherited: an owned operation reads them through its owner, which decides (a source's assets run only while the source is enabled; one left without a retry policy uses its owner's)."

- [ ] **Step 4: Rework the source's construction**

In `packages/interloper-core/src/interloper/source/base.py`:

1. `dataset` becomes:

```python
    dataset: str | None = InputField(
        default=None,
        description="Defaults to the source key when left empty",
        section="Destination",
    )
```

2. Add the source's own switch, after `retry`, and list it in `internal_fields` (`frozenset({"assets", "normalizer", "select", "enabled"})`, `select` leaving in Task 4):

```python
    enabled: bool = Field(default=True, description="Whether this source's assets execute")
```

3. Add, in the `# -- Assets --` section:

```python
    def inherited(self, child: Component, name: str, own: Any) -> Any:
        """What one of this source's assets reads for an inherited setting.

        The dataset is part of an asset's location, which the source names
        (as `table` names the rest): the asset's own dataset, else this
        source's, else this source's key, with any ``{field}`` filled from
        this source's fields by `format`. Every other setting but ``enabled``
        is handed down as `Component.inherited` does.

        An asset runs only while it and this source are both enabled, which is
        how ``source(enabled=False)`` puts a read-only source in a run.

        Args:
            child: The asset reading the setting.
            name: The setting's field name.
            own: The asset's stored value, ``None`` while unset.

        Returns:
            The effective value of the setting for *child*.
        """
        if name == "dataset":
            return self.format(own or self.dataset or self.key)
        if name == "enabled":
            return bool(own) and self.enabled
        return super().inherited(child, name, own)
```

4. In `model_post_init`, replace `self._resolve()` with:

```python
        for asset in self.assets:
            asset.parent = self
        self._check_locations()
```

and update its docstring's first paragraph to: "Build the source's assets, own them, check where they land, and wire them to each other."

5. Delete `_resolve` and add in `# -- Internals --`:

```python
    def _check_locations(self) -> None:
        """Check that this source's dataset and every materializing asset's table are valid identifiers.

        A reference names an existing table verbatim, wildcards included, so
        its table is not checked.
        """
        validate_key(self.dataset or self.key)
        for asset in self.assets:
            if not isinstance(asset, Reference):
                validate_key(asset.table)
```

6. In `__call__`, replace the dataset block (the loop re-pointing assets) with:

```python
        if dataset is not None:
            copy.dataset = dataset
```

and update the `dataset` arg doc to "Replacement dataset; assets that set none read it through the copy." The `enabled` branch (`copy.assets = [asset(enabled=enabled) ...]`) becomes `copy.enabled = enabled`, the assets reading it through the copy.

7. `MaterializationStrategy` may become unused in `source/base.py` only if no other line uses it; keep the import while `materialization_strategy` is declared.

- [ ] **Step 5: Reconcile the existing source tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source/test_base.py -q`
Then rewrite each failing existing test so it asserts the effective read instead of a stored copy. Known cases:
- `TestIdentity.test_dataset_default_equals_source_key` becomes:

```python
    def test_dataset_defaults_to_unset_and_assets_read_the_source_key(self):
        source = FakeSourceWithAssets()
        assert source.dataset is None
        assert source.fake_first.dataset == FakeSourceWithAssets.key  # ty: ignore[unresolved-attribute]
```

- `TestResolution.test_default_strategy_is_reconcile_and_overrides_nothing` and `test_legacy_null_strategy_still_hydrates`: assert the asset reads `RECONCILE` from the source and that its stored value (`asset.__dict__["materialization_strategy"]`) is `None`.
- Any test asserting an asset's payload in `to_spec()` contains `dataset`, `default_destination_key` or `materialization_strategy` copied from the source: assert the key is absent instead.
Rename `TestResolution` to `TestInheritedSettings` and fold `TestRetryInheritance` into it (its three tests keep passing). Each rewrite is a ledger ruling.

- [ ] **Step 6: Reconcile consumers**

Run: `grep -rn 'dataset == ""\|default_destination_key == ""' packages/*/tests packages/*/src`
Every assertion that an asset's dataset or default destination key is `""` becomes `is None`. Run the db and destination suites:
`UV_FROZEN=1 uv run pytest packages/interloper-db packages/interloper-core/tests/destination packages/interloper-assets -q`
Expected: PASS after the same kind of rewrite (payload no longer carries copied values). Ledger each.

- [ ] **Step 7: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 8: Checkpoint (no commit)**

---

### Task 3: Relations read through the owner

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/base.py` (`_targets` new, `resolve`, `validate_relations`, `to_spec`, `_trickled` removed, `on_rebind` and `trickle` docstrings)
- Modify: `packages/interloper-core/src/interloper/component/relation.py` (`__get__`)
- Modify: `packages/interloper-core/src/interloper/source/base.py` (`on_rebind`, `__call__`)
- Test: `packages/interloper-core/tests/source/test_base.py`

**Interfaces:**
- Consumes: `Component.parent`.
- Produces: `Component._targets(name: str) -> list[Component]`; `Relation.__get__` returns `_targets` shaped by `many`; `Component.resolve` falls back after the owner.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/source/test_base.py`:

```python
class TestRelationInheritance:
    def test_an_asset_reads_the_sources_connection_without_holding_it(self):
        connection = FakeConnection(token="t")
        source = Shop(connection=connection)
        assert source.orders.connection is connection  # ty: ignore[unresolved-attribute]
        assert source.orders.bound("connection") is None  # ty: ignore[unresolved-attribute]

    def test_a_connection_rebound_on_the_source_reaches_the_asset(self):
        source = Shop(connection=FakeConnection(token="a"))
        replacement = FakeConnection(token="b")
        source.connection = replacement
        assert source.orders.connection is replacement  # ty: ignore[unresolved-attribute]

    def test_an_assets_own_binding_wins(self):
        own = FakeConnection(token="own")
        source = Shop(connection=FakeConnection(token="source"))
        source.orders.connection = own  # ty: ignore[unresolved-attribute]
        assert source.orders.connection is own  # ty: ignore[unresolved-attribute]

    def test_destinations_read_through_the_source(self):
        destination = FakeDestination()
        source = Shop(connection=FakeConnection(token="t"), destinations=[destination])
        assert source.orders.destinations == [destination]  # ty: ignore[unresolved-attribute]
        assert source.orders.bound("destinations") == []  # ty: ignore[unresolved-attribute]

    def test_a_target_the_assets_relation_does_not_accept_is_not_inherited(self):
        class Narrowed(il.Source):
            class Picky(il.Asset):
                destinations: list[il.Destination] = il.Relation(
                    "destination", "fake_destination", many=True, optional=True
                )

                def data(self) -> Any:  # pragma: no cover
                    return []

        source = Narrowed(destinations=[FakeOtherDestination()])
        assert source.picky.destinations == []  # ty: ignore[unresolved-attribute]

    def test_a_relation_filled_through_the_source_validates(self):
        source = Shop(connection=FakeConnection(token="t"))
        source.orders.validate_relations()  # ty: ignore[unresolved-attribute]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source/test_base.py::TestRelationInheritance -q`
Expected: FAIL on the `bound(...) is None` and `bound("destinations") == []` assertions (today's trickle binds onto the asset).

- [ ] **Step 3: Add `_targets` and read through it**

In `packages/interloper-core/src/interloper/component/base.py`, in `# -- Relations --`:

```python
    def _targets(self, name: str) -> list[Component]:
        """What one of this component's relations holds: its own binding, else what its owner hands it.

        An owned component never holds a copy of its owner's bindings; it
        reads them here. The owner's targets under the same name are filtered
        by what this relation `accepts`, every accepted one for a ``many``
        relation and the first otherwise, the filter `trickle` applies.

        Args:
            name: The relation name as declared on the class.

        Returns:
            The own targets when any are bound; otherwise the owner's accepted
            targets; empty when neither holds any.
        """
        own = self._bound.get(name, [])
        owner = self._parent
        if own or owner is None or name not in type(owner).relations:
            return list(own)
        relation = self._relation(name)
        accepted = [
            target for target in owner._targets(name) if relation.accepts(target.kind, target.identity, owner=self.identity)
        ]
        return accepted if relation.many else accepted[:1]
```

`resolve` becomes:

```python
        relation = self._relation(name)
        targets = self._targets(name)
        if relation.many:
            return targets
        return targets[0] if targets else relation.fallback()
```

with its docstring's first paragraph: "Unlike `bound`, this reads through the owner (see `_targets`) and then falls back to what the relation can fill itself with, so a relation with a fallback works without a binding."

In `validate_relations`, replace `targets = self._bound.get(name, [])` with `targets = self._targets(name)` and add to the docstring: "A relation filled through the owner counts as bound."

In `packages/interloper-core/src/interloper/component/relation.py`, `__get__`:

```python
        if instance is None:
            return self
        targets = instance._targets(self.name)
        if self.many:
            return targets
        return targets[0] if targets else None
```

with the docstring's `Returns:` reading "This relation on class access; otherwise what the instance holds or reads through its owner (see `Component._targets`), a list for a ``many`` relation and a single component or ``None`` otherwise."

- [ ] **Step 4: Stop pushing into assets**

In `packages/interloper-core/src/interloper/source/base.py`, `on_rebind` becomes:

```python
    def on_rebind(self, name: str) -> None:
        """Trickle this source's bindings into its destinations whenever one changes.

        Its assets need nothing: they read what they leave unbound through
        the source.

        Args:
            name: The relation name whose binding changed; every relation the
                source holds trickles, so the name itself is not read.
        """
        for destination in self.destinations:
            self.trickle(destination)
```

In `__call__`, the loop over `copy.assets` keeps only `asset.parent = copy` (delete the `_trickled` unbinding). Update the `**relations` arg doc: "Replacement targets for the copy's declared relations; assets that bind none read them through the copy."

In `component/base.py`: delete `_trickled`; in `to_spec` delete the two lines popping trickled names and the sentence about it in the docstring. In the `on_rebind` and `trickle` docstrings, replace the example "the way a source trickles its bindings into its assets and destinations" with "the way a source trickles its bindings into its destinations and a job into its targets".

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source packages/interloper-core/tests/component packages/interloper-core/tests/job packages/interloper-core/tests/dag -q`
Expected: PASS. Existing tests named `*_trickles_*` that assert `bound(...)` on an asset are rewritten to assert the attribute read; ledger each.

- [ ] **Step 6: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 7: Checkpoint (no commit)**

---

### Task 4: One way to reach an asset

**Files:**
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (`AssetMeta`, `class Asset(..., metaclass=AssetMeta)`)
- Modify: `packages/interloper-core/src/interloper/source/base.py` (`model_config`, `_collect_asset_types`, removals)
- Modify: `packages/interloper-core/src/interloper/source/decorator.py` (no stamping)
- Test: `packages/interloper-core/tests/source/test_base.py`, `packages/interloper-core/tests/asset/test_base.py`, `packages/interloper-core/tests/asset/test_view.py`, `packages/interloper-sql/tests/view/test_base.py`, `packages/interloper-assets/tests/facebook_ads_star_schema/test_source.py`, `packages/interloper-scheduler/tests/test_executor.py`

**Interfaces:**
- Produces: `AssetMeta.__get__` (class read returns the class, `Source` instance read returns the live asset, any other instance read returns the class); `Source.asset_types` collected once; every asset class reachable on its source under its key.
- Removes: `AssetRef`, `Source.__getattr__`, `Source.asset_def`, `Source.select`, `Source._apply_select`.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/source/test_base.py`, replace the `AssetRef`, `__getattr__` and `asset_def` tests in `TestAssets` (`test_nested_asset_classes_replaced_by_asset_ref_descriptors`, `test_getattr_returns_asset_instance_by_key`, `test_getattr_raises_for_unknown_key`, `test_asset_def_returns_definition_with_source_key`, `test_asset_def_raises_for_unknown_key`) with:

```python
class TestReachingAnAsset:
    def test_the_class_read_is_the_asset_class(self):
        assert Cascade.plain.key == "plain"
        assert isinstance(Cascade.plain, type)

    def test_the_instance_read_is_the_live_asset(self):
        source = Cascade()
        assert source.plain is source.assets[1]
        assert source.plain.parent is source

    def test_a_nested_asset_class_is_reachable_under_its_key(self):
        source = Shop(connection=FakeConnection(token="t"))
        assert source.orders is source.Orders

    def test_a_listed_asset_is_reachable_under_its_key(self):
        @il.asset
        def listed() -> list[dict[str, Any]]:
            return []

        class Listing(il.Source):
            asset_types: ClassVar[list[type[il.Asset]]] = [listed]

        assert Listing.listed is listed  # ty: ignore[unresolved-attribute]
        assert Listing().listed.parent is not None  # ty: ignore[unresolved-attribute]

    def test_another_holder_reads_the_class(self):
        class Holder:
            asset_cls = Cascade.plain

        assert Holder().asset_cls is Cascade.plain

    def test_a_standalone_asset_is_untouched(self):
        @il.asset
        def lonely() -> list[dict[str, Any]]:
            return []

        assert lonely().parent is None
        assert lonely.classpath().endswith(".lonely")

    def test_an_asset_owned_by_another_source_is_refused(self):
        with pytest.raises(TypeError, match="already belongs to source"):

            class Thief(il.Source):
                asset_types: ClassVar[list[type[il.Asset]]] = [Cascade.plain]

    def test_an_asset_key_taken_by_another_attribute_is_refused(self):
        @il.asset
        def region() -> list[dict[str, Any]]:
            return []

        with pytest.raises(TypeError, match="the name is taken"):

            class Clash(il.Source):
                region = "eu"
                asset_types: ClassVar[list[type[il.Asset]]] = [region]

    def test_a_pickled_source_keeps_its_assets_and_their_reads(self):
        import pickle

        source = pickle.loads(pickle.dumps(Cascade(dataset="raw")))
        assert source.plain.parent is source
        assert source.plain.dataset == "raw"

    def test_select_asset_def_and_getattr_are_gone(self):
        assert not hasattr(il.Source, "asset_def")
        assert "select" not in il.Source.model_fields
        assert "__getattr__" not in vars(il.Source)
```

Note on `test_an_asset_key_taken_by_another_attribute_is_refused`: the class body defines `region = "eu"` as a plain class attribute; if Pydantic rejects the un-annotated attribute before `_collect_asset_types` runs, annotate it `region: ClassVar[str] = "eu"`, keeping the collision.

In `TestDefinition.test_partitionings_are_the_partitioned_assets_own`, stop sharing `FakeSourceWithAssets`'s asset classes (the one shared class in the suite):

```python
        class FakeUndated(il.Asset):
            def data(self) -> list[dict[str, Any]]:
                return []

        class FakeMixedSource(il.Source):
            asset_types: ClassVar[list[type[il.Asset]]] = [FakeDaily, FakeUndated]
```

Delete `TestSelect`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source/test_base.py::TestReachingAnAsset -q`
Expected: FAIL (`AssetRef` returned at class level for body assets is fine, but the ownership and collision tests, the removals test and `Holder().asset_cls` identity fail).

- [ ] **Step 3: Add `AssetMeta`**

In `packages/interloper-core/src/interloper/asset/base.py` (imports: `from typing import overload`, `from pydantic._internal._model_construction import ModelMetaclass`), above `class Asset`:

```python
class AssetMeta(ModelMetaclass):
    """The metaclass of every asset class: an asset class read off a source instance is that source's asset.

    An asset class is its own descriptor, as a `Relation` is. Read on a class it
    is the class itself, which is what composite import paths
    (``module:Shop.orders``) walk; read on a `Source` instance it is that
    instance's live asset of the same key; read on any other instance it is the
    class, as for any class attribute.
    """

    @overload
    def __get__(cls, instance: None, owner: type | None = None) -> type[Asset]: ...
    @overload
    def __get__(cls, instance: Source, owner: type | None = None) -> Asset: ...
    @overload
    def __get__(cls, instance: object, owner: type | None = None) -> type[Asset]: ...
    def __get__(cls, instance: object, owner: type | None = None) -> Any:
        """Resolve to the class, or to a source instance's live asset of this key.

        Args:
            instance: The object the attribute is read on, ``None`` on a class.
            owner: The class the attribute is read through; unused, part of
                the descriptor protocol. Defaults to ``None``.

        Returns:
            The live asset when *instance* is a source, else this class.

        Raises:
            AttributeError: If the source holds no asset of this key (a source
                narrowed to some of its assets by an override map).
        """
        from interloper.source.base import Source

        if not isinstance(instance, Source):
            return cls
        for asset in instance.assets:
            if asset.key == cls.key:
                return asset
        raise AttributeError(f"Source '{type(instance).__name__}' has no asset with key '{cls.key}'")
```

and declare `class Asset(Operation, metaclass=AssetMeta):`. If `ty` reports the third overload as overlapping the second, drop the third overload and ledger the ruling.

- [ ] **Step 4: Collect once in `Source`**

In `packages/interloper-core/src/interloper/source/base.py`:

1. `model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(IgnoredDescriptor, Relation, AssetMeta))` on `Source` (import `ConfigDict` from pydantic, `AssetMeta` from `interloper.asset.base`).
2. Replace `_collect_asset_types` with:

```python
    @classmethod
    def _collect_asset_types(cls) -> None:
        """Gather the source's asset classes, own each, and make each reachable under its key.

        The asset classes are the ones ``asset_types`` lists (which the
        function form of `source` fills) followed by the ones written in the
        class body, each once. A class collecting none inherits its parent's.
        Each collected class is owned by this source and installed as a class
        attribute under its key; an asset class is its own descriptor (see
        `AssetMeta`), so ``Shop.orders`` is the class and ``Shop().orders`` the
        live asset.

        Raises:
            TypeError: If an asset class already belongs to another source, or
                if its key names another attribute of this source.
        """
        listed: list[type[Asset]] = list(cls.__dict__.get("asset_types", []))
        declared = [value for value in cls.__dict__.values() if isinstance(value, type) and issubclass(value, Asset)]
        collected = listed + [asset_cls for asset_cls in declared if asset_cls not in listed]
        if not collected:
            return
        for asset_cls in collected:
            owner = asset_cls._source_type
            if owner is not None and owner is not cls:
                raise TypeError(f"Asset '{asset_cls.__name__}' already belongs to source '{owner.__name__}'")
            current = cls.__dict__.get(asset_cls.key, asset_cls)
            if current is not asset_cls:
                raise TypeError(
                    f"Source '{cls.__name__}' cannot expose asset '{asset_cls.key}': the name is taken by {current!r}"
                )
            asset_cls._source_type = cls
            setattr(cls, asset_cls.key, asset_cls)
        cls.asset_types = collected
```

and update `__init_subclass__`'s docstring to "Collect the source's assets at definition time (see `_collect_asset_types`)."
3. Delete `AssetRef`, `__getattr__`, `asset_def`, `select`, `_apply_select`, the `select` lines in `model_post_init`, and `"select"` from `internal_fields` (which keeps `"assets"`, `"normalizer"` and `"enabled"`). Update the class docstring's example to show `source.users` working through `AssetMeta`.
4. In `packages/interloper-core/src/interloper/source/decorator.py`, delete both `for asset_cls in ...: asset_cls._source_type = source_cls` loops, and the docstring sentence about stamping.

- [ ] **Step 5: Replace `select=` in the suites**

Run: `grep -rln "select=\[" packages/*/tests` (and the scheduler's `EmptyWorkload(select=[])`).
- `EmptyWorkload(select=[])` becomes `EmptyWorkload()`.
- Every source built with `select=[]` to make all of its assets read-only is built with `enabled=False` instead: `Shop(account_id="1", destinations=[warehouse], enabled=False)` (Task 2 made a disabled source disable its assets). Affected: `interloper-core/tests/asset/test_view.py`, `interloper-sql/tests/view/test_base.py`, `interloper-assets/tests/facebook_ads_star_schema/test_source.py`.

- [ ] **Step 6: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests packages/interloper-sql packages/interloper-assets packages/interloper-scheduler packages/interloper-db -q`
Expected: PASS.

- [ ] **Step 7: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green. `ty` may now flag `# ty: ignore[unresolved-attribute]` comments that are no longer needed on `@il.asset` method access (the instance read now types as `Asset`); remove the ones it reports as unused.

- [ ] **Step 8: Checkpoint (no commit)**

---

### Task 5: Output as a value, the signature read once, `default_destination`

**Files:**
- Create: `packages/interloper-core/src/interloper/asset/output.py`
- Modify: `packages/interloper-core/src/interloper/asset/base.py`
- Modify: `packages/interloper-core/src/interloper/asset/view.py`
- Modify: `packages/interloper-core/src/interloper/asset/__init__.py` (export `Output`)
- Test: `packages/interloper-core/tests/asset/test_output.py` (new, mirrors the new module), `packages/interloper-core/tests/asset/test_base.py`, `packages/interloper-core/tests/asset/test_view.py`

**Interfaces:**
- Produces: `Output(data: Any, schema: type[Schema] | None = None)` frozen dataclass with `Output.from_result(result: Any, asset: Asset) -> Output`; `Asset._parameters: ClassVar[tuple[str, ...]]`; `Asset.default_destination` property (`Destination`, `AssetError` when none); `Asset._output(result: Any) -> Output`; `async Asset._produce(window: PartitionWindow | None, dag: DAG | None, metadata: dict[str, Any]) -> Output`; `async Asset._land(output: Output, window: PartitionWindow | None, metadata: dict[str, Any]) -> None`; `View.default_destination` (exactly one, else `ConfigError`).
- Removes: `Asset._unwrap`, `_normalize_and_conform`, `_conform`, `_infer_schema`, `_effective_schema`, `_destinations`, `_validate_destination`, `_read_destination`, `_destination_write`; `View._normalize_and_conform`, `View._view_destination`.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-core/tests/asset/test_output.py`: move `TestConformEdgeCases` out of `tests/asset/test_base.py` and rewrite it over `Output.from_result`:

```python
"""Tests for ``interloper.asset.output``."""

from typing import Any

import pytest
from pydantic import BaseModel

import interloper as il
from interloper.asset.output import Output
from interloper.errors import AssetError
from interloper.normalizer import MaterializationStrategy


class Row(il.Schema):
    user_id: int
    name: str


@il.asset
def untyped() -> Any:
    return None


@il.asset(schema=Row)
def typed() -> Any:
    return None


@il.asset(schema=Row, materialization_strategy=MaterializationStrategy.STRICT)
def strict() -> Any:
    return None


class TestRows:
    def test_none_is_no_rows(self):
        assert Output.from_result(None, untyped()).data == []

    def test_a_generator_is_consumed(self):
        assert Output.from_result(({"a": i} for i in range(2)), untyped()).data == [{"a": 0}, {"a": 1}]

    def test_a_model_is_dumped(self):
        class Item(BaseModel):
            a: int

        assert Output.from_result([Item(a=1)], untyped()).data == [{"a": 1}]

    def test_a_dict_is_one_row(self):
        assert Output.from_result({"a": 1}, untyped()).data == [{"a": 1}]


class TestConform:
    def test_non_tabular_data_without_a_schema_passes_through(self):
        payload = object()
        output = Output.from_result(payload, untyped())
        assert output.data is payload
        assert output.schema is None

    def test_non_tabular_data_with_a_schema_is_an_actionable_error(self):
        with pytest.raises(AssetError, match="declares a schema but returned data that cannot be checked"):
            Output.from_result(object(), typed())

    def test_strict_returns_the_validated_data_and_its_schema(self):
        rows = [{"user_id": 1, "name": "x"}]
        output = Output.from_result(rows, strict())
        assert output.data == rows
        assert output.schema is Row

    def test_strict_without_a_schema_is_refused(self):
        @il.asset(materialization_strategy=MaterializationStrategy.STRICT)
        def loose() -> Any:
            return None

        with pytest.raises(AssetError, match="requires a schema"):
            Output.from_result([{"a": 1}], loose())

    def test_without_a_schema_the_schema_is_inferred(self):
        output = Output.from_result([{"user_id": 1, "name": "a"}], untyped())
        assert [spec.name for spec in output.schema.field_specs()] == ["user_id", "name"]

    def test_failed_inference_leaves_no_schema(self, monkeypatch: pytest.MonkeyPatch):
        from interloper.representation import RowsRepresentation

        monkeypatch.setattr(
            RowsRepresentation, "infer", lambda self, data: (_ for _ in ()).throw(RuntimeError("cannot infer"))
        )
        output = Output.from_result([{"a": 1}], untyped())
        assert output.data == [{"a": 1}]
        assert output.schema is None

    def test_the_normalizer_receives_the_rows(self):
        received: list[Any] = []

        class Recording(il.Normalizer):
            def normalize(self, data: Any) -> Any:
                received.append(data)
                return data

        Output.from_result({"a": 1}, untyped()(normalizer=Recording()))
        assert received == [[{"a": 1}]]
```

If `il.Schema` or `il.Normalizer` is not exported under those names, import them from `interloper.schema` and `interloper.normalizer`. If `Normalizer` is not subclassable this way, reuse the existing normalizer test fixture from `tests/asset/test_base.py`.

In `packages/interloper-core/tests/asset/test_base.py`:
- Delete `TestConformEdgeCases` (moved).
- `test_auto_without_schema_infers_effective_schema` becomes a check through the destination: capture the `IOContext.schema` a recording destination receives (the pattern `test_iocontext_carries_schema_to_destination` already uses) and assert its field names are `["user_id", "name"]`.
- The three `_validate_destination` tests are deleted (the check is the binding's, covered in `tests/component/test_base.py`).
- The `_read_destination` tests become `default_destination` tests:

```python
    def test_default_destination_key_selects_the_default_destination(self):
        first, second = FakeDestination(), FakeOtherDestination()
        asset = FakeAsset(destinations=[first, second], default_destination_key=second.key)
        assert asset.default_destination is second

    def test_the_first_destination_is_the_default_without_a_key(self):
        first, second = FakeDestination(), FakeOtherDestination()
        assert FakeAsset(destinations=[first, second]).default_destination is first

    def test_no_destination_is_an_error(self):
        with pytest.raises(AssetError, match="No destination found"):
            FakeAsset().default_destination
```

- Add the race test:

```python
class TestConcurrentRuns:
    async def test_two_runs_of_one_instance_keep_their_own_schemas(self):
        import asyncio

        seen: list[list[str]] = []

        class Recording(il.MemoryDestination):
            def write(self, context: il.IOContext, data: Any) -> None:
                seen.append([spec.name for spec in context.schema.field_specs()])

        @il.asset
        def shifting(context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"left": 1}] if context.metadata["side"] == "left" else [{"right": 1}]

        asset = shifting(destinations=[Recording()])
        await asyncio.gather(
            asset.materialize_async(metadata={"side": "left"}),
            asset.materialize_async(metadata={"side": "right"}),
        )
        assert sorted(seen) == [["left"], ["right"]]
```

- Add the signature record test:

```python
class TestParameters:
    def test_the_signature_is_recorded_once_in_order(self):
        class Upstreamed(il.Asset):
            def data(self, context: il.ExecutionContext, orders: il.Upstream, **extra: Any) -> Any:
                return []

        assert Upstreamed._parameters == ("context", "orders")

    def test_a_subclass_inherits_the_record(self):
        class Base(il.Asset):
            def data(self, context: il.ExecutionContext) -> Any:
                return []

        class Child(Base):
            pass

        assert Child._parameters == ("context",)
```

In `packages/interloper-core/tests/asset/test_view.py`, any reference to `_view_destination` becomes `default_destination`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/asset -q`
Expected: collection error for `interloper.asset.output`; `default_destination`, `_parameters` and concurrent-run tests fail.

- [ ] **Step 3: Create `Output`**

`packages/interloper-core/src/interloper/asset/output.py`:

```python
"""Output: what one run of an asset produced, its data and the schema it was checked against."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from interloper.errors import AssetError, RepresentationError
from interloper.normalizer import MaterializationStrategy
from interloper.representation import Representation, View
from interloper.schema import Schema
from interloper.telemetry.tracer import tracer
from interloper.utils.data import is_empty

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True)
class Output:
    """What one run of an asset produced.

    A value, so a run's schema travels with its data from conform to the write
    instead of resting on the asset between the two.

    Attributes:
        data: The rows, conformed to the schema when there is one, or an opaque
            object no representation matches.
        schema: The schema the data was checked against: the declared one, the
            inferred one, or ``None`` when there is neither.
    """

    data: Any
    schema: type[Schema] | None = None

    @classmethod
    def from_result(cls, result: Any, asset: Asset) -> Output:
        """Shape what ``data()`` returned into the asset's output: rows, normalized, then conformed.

        Normalization (when a normalizer is set) reshapes the data; conform
        then enforces the declared schema according to the materialization
        strategy, whether or not a normalizer ran, so a declared schema is
        always a checked contract.

        Args:
            result: The value ``data()`` returned.
            asset: The asset whose normalizer, schema and strategy apply.

        Returns:
            The output.
        """
        data = cls._rows(result)
        attributes = asset._span_attributes()
        if asset.normalizer is not None:
            with tracer().start_as_current_span("interloper.normalizer.normalize", attributes=attributes):
                data = asset.normalizer.normalize(data)
        with tracer().start_as_current_span("interloper.asset.conform", attributes=attributes):
            return cls._conform(data, asset)

    @staticmethod
    def _rows(result: Any) -> Any:
        """Turn the Python shapes ``data()`` may return besides a table into rows.

        The one place the framework is lenient about what an asset returns: a
        generator is consumed, a pydantic model or a list of them is dumped, a
        lone dict becomes one row, and ``None`` is no rows. Everything else,
        tables included, is returned untouched.

        Args:
            result: The raw value returned by ``data()``.

        Returns:
            The rows, or the result unchanged when it is not one of those shapes.
        """
        if result is None:
            return []
        if isinstance(result, Iterator):
            result = list(result)
        if isinstance(result, BaseModel):
            return [result.model_dump()]
        if isinstance(result, dict):
            return [result]
        if isinstance(result, list) and result and isinstance(result[0], BaseModel):
            return [item.model_dump() if isinstance(item, BaseModel) else item for item in result]
        return result

    @classmethod
    def _conform(cls, data: Any, asset: Asset) -> Output:
        """Enforce the asset's schema according to its materialization strategy.

        With a schema, ``RECONCILE`` aligns and coerces while ``STRICT``
        refuses extra, missing or invalid columns. Without one, ``RECONCILE``
        infers a schema and ``STRICT`` is an error. Data no representation
        matches passes through without a schema and is an error with one.

        Args:
            data: The rows, normalized when a normalizer is set.
            asset: The asset whose schema and strategy apply.

        Returns:
            The conformed output.

        Raises:
            AssetError: If ``STRICT`` is set without a schema, or if a schema
                is declared but the data is not tabular.
        """
        strategy = asset.materialization_strategy
        schema = asset.schema
        if schema is None and strategy is MaterializationStrategy.STRICT:
            raise AssetError(f"Asset '{asset.key}': strategy='strict' requires a schema.")
        try:
            view = Representation.of(data)
        except RepresentationError as error:
            if schema is None:
                return cls(data)
            raise AssetError(
                f"Asset '{asset.key}' declares a schema but returned data that cannot be checked against it: {error}"
            ) from error
        if schema is None:
            with tracer().start_as_current_span("interloper.asset.infer_schema", attributes=asset._span_attributes()):
                return cls(data, cls._infer(view))
        with tracer().start_as_current_span("interloper.representation.reconcile", attributes=asset._span_attributes()):
            return cls(view.reconcile(schema, strict=strategy is MaterializationStrategy.STRICT), schema)

    @staticmethod
    def _infer(view: View) -> type[Schema] | None:
        """Best-effort schema inference, metadata for destinations that must never fail a run.

        Args:
            view: The data bound to its representation.

        Returns:
            The inferred schema, or ``None`` when the data is empty or
            inference fails.
        """
        if is_empty(view.data):
            return None
        try:
            return view.infer()
        except Exception:  # noqa: BLE001 - inference is best-effort metadata
            return None
```

Export it: add `from interloper.asset.output import Output` and `"Output"` to `asset/__init__.py`'s `__all__`.

- [ ] **Step 4: Rework `Asset`**

In `packages/interloper-core/src/interloper/asset/base.py`:

1. Declare `_parameters: ClassVar[tuple[str, ...]] = ()` beside `_source_type`.
2. In `_collect`, record the parameters and keep the inference; the loop becomes:

```python
        hints = cls._data_hints()
        inferred: dict[str, Relation] = {}
        parameters: list[str] = []
        for name, parameter in signature.parameters.items():
            if name == "self" or parameter.kind in _VARIADIC_KINDS:
                continue
            parameters.append(name)
            if name in _RESERVED_PARAMETERS or name in cls.relations:
                continue
            hint = hints.get(name, parameter.annotation)
            relation = cls._infer_relation(name, hint, optional=parameter.default is None)
            inferred[name] = relation.model_copy(update={"name": name})
        cls._parameters = tuple(parameters)
```

and add to the docstring: "The parameters the call fills are recorded, in order, in ``_parameters``, so a run never reads the signature again."
3. `_build_kwargs` iterates `type(self)._parameters` instead of `inspect.signature(self.data).parameters`, dropping its own `("self", "kwargs")` skip; update its docstring ("One value per recorded parameter (see `_collect`)").
4. Add the property, replacing `_read_destination`:

```python
    @property
    def default_destination(self) -> Destination:
        """The destination downstream readers load this asset from.

        The destination whose key is ``default_destination_key`` when one is
        set and bound, else the first bound destination.

        Raises:
            AssetError: If the asset has no destination at all.
        """
        destinations: list[Destination] = self.destinations
        if not destinations:
            raise AssetError(f"No destination found for upstream asset '{self.key}'")
        preferred = next(
            (destination for destination in destinations if destination.key == self.default_destination_key), None
        )
        return preferred or destinations[0]
```

5. Replace the bodies of `run_async` and `materialize_async`, and add `_produce`, `_output` and `_land` (the latter replacing `_destination_write`):

The two entry points keep their signatures and docstrings. `run_async`'s body becomes:

```python
        window = self.effective_window(PartitionWindow.of(window))
        return (await self._produce(window, dag, metadata or {})).data
```

`materialize_async`'s body becomes:

```python
        if not self.enabled:
            return None
        window = self.effective_window(PartitionWindow.of(window))
        metadata = metadata or {}
        output = await self._produce(window, dag, metadata)
        await self._land(output, window, metadata)
        return output.data
```

The three new methods:

```python

    async def _produce(self, window: PartitionWindow | None, dag: DAG | None, metadata: dict[str, Any]) -> Output:
        """Run ``data()`` over the resolved inputs and shape what it returned.

        Args:
            window: The window this asset consumes, already narrowed.
            dag: DAG the upstream assets are looked up in.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            The run's output.
        """
        self._check_window(window)
        context = ExecutionContext(self, window, metadata)
        kwargs = await self._build_kwargs(context, window, dag)
        with Step(StepKind.ASSET_DATA, self.event_metadata(metadata, window), subject=f"'{self.key}'"):
            result = await invoke(self.data, **kwargs)
        # Shaping is CPU-bound (pandas/pyarrow); offload it so it never blocks the event loop.
        return await asyncio.to_thread(self._output, result)

    def _output(self, result: Any) -> Output:
        """Shape what ``data()`` returned into this asset's output (see `Output.from_result`).

        A kind whose output is not data overrides it (see `View`).

        Args:
            result: The value ``data()`` returned.

        Returns:
            The output.
        """
        return Output.from_result(result, self)

    async def _land(self, output: Output, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Write the output to every destination, one observed step each.

        An empty output is skipped with a warning. A kind that lands
        elsewhere overrides it (see `View`).

        Args:
            output: The run's output.
            window: The window the asset consumed, ``None`` for an unpartitioned asset.
            metadata: Run-level metadata carried onto the emitted events.
        """
        destinations: list[Destination] = self.destinations
        if not destinations:
            return
        if is_empty(output.data):
            EventBus.emit(
                EventType.LOG,
                metadata=self.event_metadata(
                    metadata,
                    window,
                    level="WARNING",
                    message=(
                        f"Asset '{self.key}' produced no data; skipping write to {len(destinations)} destination(s)"
                    ),
                ),
            )
            return
        context = IOContext(asset=self, window=window, metadata=metadata, schema=output.schema or self.schema)
        rows = len(output.data) if isinstance(output.data, Sized) else None
        for destination in destinations:
            step = Step(
                StepKind.DEST_WRITE,
                self.event_metadata(metadata, window, destination_key=destination.key),
                subject=f"'{self.key}'",
            )
            with step:
                await invoke(destination.write, context, output.data)
                step.record(rows=rows)
```

`run_async`'s docstring body paragraph changes to "Resolves the context and the inputs, runs ``data()`` and shapes its result (see `_produce`); a sync ``data()`` is offloaded to a thread, an async one awaited." The other docstrings stay.
6. `_destination_read` reads `upstream.default_destination`; `partition_row_counts` reads `self.default_destination`.
7. Delete `_unwrap`, `_normalize_and_conform`, `_conform`, `_infer_schema`, `_effective_schema` (the `PrivateAttr`), `_destinations`, `_validate_destination`, `_read_destination`, `_destination_write`, and the imports only they used (`Iterator`, `BaseModel`, `PrivateAttr`, `RepresentationError`, `Representation`, `View`, `MaterializationStrategy` if unused after the field keeps using it, `DestinationError`). Import `Output` from `interloper.asset.output`.

- [ ] **Step 5: Keep `View` on the new names**

In `packages/interloper-core/src/interloper/asset/view.py`:

```python
    @property
    def default_destination(self) -> Destination:
        """The one destination this view is created in.

        Raises:
            ConfigError: If the view does not have exactly one destination.
        """
        destinations: list[Destination] = self.destinations
        if len(destinations) != 1:
            raise ConfigError(f"View '{self.qualified_key}' needs exactly one destination, has {len(destinations)}")
        return destinations[0]

    def _output(self, result: Any) -> Output:
        """Leave a definition alone: it is not data.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The definition as the output's data, without a schema.
        """
        return Output(self._definition(result))
```

Delete `_normalize_and_conform` and `_view_destination`; `materialize_async` and `_read_upstreams` call `self.default_destination`; `validate_relations` reads `self.destinations`. `materialize_async` builds its definition with `self._definition(await self.run_async(...))` as today (Task 6 removes it).

- [ ] **Step 6: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/asset packages/interloper-core/tests/runner packages/interloper-sql -q`
Expected: PASS.

- [ ] **Step 7: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 8: Checkpoint (no commit)**

---

### Task 6: Upstreams read themselves; the kinds on three hooks

**Files:**
- Modify: `packages/interloper-core/src/interloper/asset/upstream.py`
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (`_upstream`, `_upstreams`)
- Modify: `packages/interloper-core/src/interloper/asset/view.py`
- Modify: `packages/interloper-sql/src/interloper_sql/view/base.py`
- Test: `packages/interloper-core/tests/asset/test_upstream.py`, `packages/interloper-core/tests/asset/test_view.py`, `packages/interloper-sql/tests/view/test_base.py`

**Interfaces:**
- Consumes: `Asset.default_destination`, `Output`, `_land`, `_produce` (Task 5).
- Produces: `async Upstream.read(upstream: Asset, reader: Asset, window: PartitionWindow | None, metadata: dict[str, Any]) -> Upstream`; `Upstream.reference(upstream: Asset, destination: Destination) -> Upstream`; `async Asset._upstream(target: Asset, window: PartitionWindow | None, metadata: dict[str, Any]) -> Upstream`; `async Asset._upstreams(name: str, targets: list[Asset], dag: DAG | None, window: PartitionWindow | None, metadata: dict[str, Any]) -> list[Upstream]`; `View._land`; `IbisView._upstream`, `IbisView._output`.
- Removes: `Asset._read_upstreams`, `Asset._destination_read`; `View.materialize_async`, `View._read_upstreams`, `View._definition`; `IbisView._read_upstreams`, `IbisView._definition`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-core/tests/asset/test_upstream.py` (add `from interloper.events import Event, EventBus` to its imports):

```python
@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


class TestRead:
    async def test_reads_the_upstreams_default_destination(self):
        destination = il.MemoryDestination()

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return [{"id": 1}]

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        upstream = orders(destinations=[destination])
        await upstream.materialize_async()
        leg = await Upstream.read(upstream, revenue(), None, {})
        assert leg.asset is upstream
        assert leg.records == [{"id": 1}]

    async def test_missing_data_is_none_with_a_warning(self):
        warnings_seen: list[Event] = []

        def handler(event: Event) -> None:
            if event.metadata.get("level") == "WARNING":
                warnings_seen.append(event)

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        @il.asset
        def revenue(orders: il.Upstream) -> list[dict[str, Any]]:
            return []

        EventBus.subscribe(handler)
        try:
            leg = await Upstream.read(orders(destinations=[il.MemoryDestination()]), revenue(), None, {})
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(handler)
        assert leg.data is None
        assert any("found no data in upstream" in event.metadata.get("message", "") for event in warnings_seen)


class TestReference:
    def test_the_data_is_the_destinations_reference(self):
        class Referencing(il.MemoryDestination):
            def reference(self, asset: il.Asset) -> Any:
                return f"ref:{asset.key}"

        @il.asset
        def orders() -> list[dict[str, Any]]:
            return []

        leg = Upstream.reference(orders(), Referencing())
        assert leg.data == "ref:orders"
```

Append to `packages/interloper-core/tests/asset/test_view.py`:

```python
    def test_a_view_references_an_upstream_absent_from_the_dag(self):
        @il.asset
        def unrelated() -> list[dict[str, Any]]:
            return []

        warehouse = RecordingWarehouse(id="warehouse")
        shop = Shop(destinations=[warehouse], enabled=False)
        reporting = Reporting(destinations=[warehouse])
        reporting.order_totals.bind("orders", shop.orders)
        definition = il.run(reporting.order_totals.run_async(None, il.DAG(unrelated()), {}))
        assert '"orders"' in definition
```

(Add it to `TestRun`.)

In `packages/interloper-sql/tests/view/test_base.py`, `test_a_definition_compiles_for_every_dialect` becomes:

```python
    definition = il.run(view.run_async(None, dag, {}))
    assert "SELECT" in definition.compile(dialect)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/asset/test_upstream.py packages/interloper-core/tests/asset/test_view.py packages/interloper-sql/tests/view -q`
Expected: FAIL (`Upstream.read` and `Upstream.reference` missing; the IbisView run returns the raw expression, which has no `compile`).

- [ ] **Step 3: Give `Upstream` its constructors**

In `packages/interloper-core/src/interloper/asset/upstream.py` (imports: `Step`, `StepKind`, `EventBus`, `EventType` from `interloper.events`; `IOContext` from `interloper.destination`; `AssetError`, `DataNotFoundError`, `format_exception` from `interloper.errors`; `telemetry_attributes` from `interloper.telemetry`; `invoke` from `interloper.utils.concurrency`; `Destination` and `PartitionWindow` under `TYPE_CHECKING`):

```python
    @classmethod
    async def read(
        cls, upstream: Asset, reader: Asset, window: PartitionWindow | None, metadata: dict[str, Any]
    ) -> Upstream:
        """Read an upstream's data from its default destination for the window it consumes.

        Nothing materialized where the destination looks is not a failure: the
        upstream is handed over with ``data=None`` and a warning event, and the
        reader decides what that means. Any other failure fails the reader.

        Args:
            upstream: The asset whose data is read.
            reader: The asset reading it, whose identity the events carry.
            window: The run's window, narrowed here to what *upstream* consumes.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            The upstream with its data.

        Raises:
            AssetError: If the read fails for any reason other than missing data.
        """
        destination = upstream.default_destination
        window = upstream.effective_window(window)
        context = IOContext(asset=upstream, window=window, metadata=metadata, schema=upstream.schema)
        step = Step(
            StepKind.DEST_READ,
            reader.event_metadata(metadata, window, destination_key=destination.key),
            subject=f"'{upstream.key}'",
            attributes={telemetry_attributes.UPSTREAM_KEY: upstream.key},
        )
        try:
            with step:
                return cls(asset=upstream, data=await invoke(destination.read, context))
        except DataNotFoundError:
            EventBus.emit(
                EventType.LOG,
                metadata=reader.event_metadata(
                    metadata,
                    window,
                    level="WARNING",
                    message=(
                        f"Asset '{reader.key}' found no data in upstream '{upstream.qualified_key}' at {window}; "
                        "it is passed with data=None"
                    ),
                ),
            )
            return cls(asset=upstream, data=None)
        except Exception as error:
            raise AssetError(
                f"Failed to load data from upstream asset '{upstream.key}': {format_exception(error)}"
            ) from error

    @classmethod
    def reference(cls, upstream: Asset, destination: Destination) -> Upstream:
        """Refer to an upstream's table instead of reading it, for a kind that reads no data.

        Args:
            upstream: The asset referred to.
            destination: The destination whose handle on the upstream is wanted.

        Returns:
            The upstream with the destination's reference as its data.
        """
        return cls(asset=upstream, data=destination.reference(upstream))
```

- [ ] **Step 4: The upstream hook and its loop on `Asset`**

In `packages/interloper-core/src/interloper/asset/base.py`, replace `_read_upstreams` and `_destination_read` with:

```python
    async def _upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        window: PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Hand every upstream bound to one relation to `_upstream`, through the DAG's own nodes.

        DAG membership is by id, so each upstream is taken from the DAG's node
        when it is there: for a mini-DAG that node is a read-only copy of the
        binding. An upstream absent from the DAG is skipped with a warning when
        this asset reads data, since there is no run to read it for; a kind
        that only refers to its upstreams refers to the binding itself.

        Args:
            name: The ``data()`` parameter the upstreams are for.
            targets: The bound upstream assets.
            dag: The DAG the upstreams are looked up in, ``None`` when the run
                was given none.
            window: The run's window.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            One `Upstream` per upstream handed over, in binding order.
        """
        upstreams: list[Upstream] = []
        for target in targets:
            node = dag.operation_map.get(target.id) if dag is not None else target
            if node is None and self.reads_upstream_data:
                EventBus.emit(
                    EventType.LOG,
                    metadata=self.event_metadata(
                        metadata,
                        window,
                        level="WARNING",
                        message=(
                            f"Asset '{self.key}' upstream '{target.qualified_key}' for parameter '{name}' "
                            "is not in the DAG; it is skipped"
                        ),
                    ),
                )
                continue
            upstreams.append(await self._upstream(cast("Asset", node or target), window, metadata))
        return upstreams

    async def _upstream(self, target: Asset, window: PartitionWindow | None, metadata: dict[str, Any]) -> Upstream:
        """Hand one upstream to ``data()``: its data, or a reference to it for a kind that reads none.

        Args:
            target: The upstream asset, the DAG's node when it has one.
            window: The run's window.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            The upstream as ``data()`` receives it.
        """
        if self.reads_upstream_data:
            return await Upstream.read(target, self, window, metadata)
        return Upstream.reference(target, self.default_destination)
```

`_build_kwargs` calls `self._upstreams(...)` where it called `self._read_upstreams(...)`. Remove now-unused imports (`DataNotFoundError`, `format_exception`, `IOContext` if `_land` no longer needs it: it does, keep it).

- [ ] **Step 5: Put `View` on the hooks**

In `packages/interloper-core/src/interloper/asset/view.py`: delete `materialize_async`, `_read_upstreams` and `_definition`; `_output` returns `Output(result)`; add:

```python
    async def _land(self, output: Output, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Create or replace the view in its one destination, as an observed write.

        Args:
            output: The run's output, its data the view's definition.
            window: The run's window; a view is unpartitioned, so ``None``.
            metadata: Run-level metadata carried onto the emitted events.
        """
        destination = self.default_destination
        step = Step(
            StepKind.DEST_WRITE,
            self.event_metadata(metadata, window, destination_key=destination.key),
            subject=f"view '{self.key}'",
        )
        with step:
            await invoke(destination.create_view, self, output.data)
```

Update the class docstring: "A view varies from an asset through its hooks alone: `reads_upstream_data` makes each upstream a reference, `_output` keeps the definition as it is, and `_land` creates the view." Remove imports no longer used (`Upstream`, `DAG`, `Partition`, `cast` when unused).

- [ ] **Step 6: Put `IbisView` on the hooks**

In `packages/interloper-sql/src/interloper_sql/view/base.py`:
- `_read_upstreams` becomes `_upstream(self, target, window, metadata) -> Upstream`: call `upstream = await super()._upstream(target, window, metadata)`, then apply today's per-upstream body (the `TableReference` check, the schema check, the `ibis.table(...)` construction) to that one upstream and return the new `Upstream`.
- `_definition` becomes `_output(self, result: Any) -> Output` with the same checks, returning `Output(IbisDefinition(result))`.
- Docstrings: `Args:`/`Returns:`/`Raises:` follow the new signatures. Import `Output` from `interloper.asset.output` (or `interloper.asset`).

- [ ] **Step 7: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/asset packages/interloper-core/tests/runner packages/interloper-core/tests/dag packages/interloper-sql packages/interloper-assets -q`
Expected: PASS.

- [ ] **Step 8: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 9: Checkpoint (no commit)**

---

### Task 7: One reconfiguration

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/base.py` (`__call__`)
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (delete `__call__`, `_UNSET`)
- Modify: `packages/interloper-core/src/interloper/source/base.py` (delete `__call__`)
- Test: `packages/interloper-core/tests/component/test_base.py`, `packages/interloper-core/tests/source/test_base.py`, `packages/interloper-core/tests/asset/test_base.py`

**Interfaces:**
- Produces: `Component.__call__(self, **changes: Any) -> Self`.
- Removes: `Asset.__call__`, `Source.__call__`, `asset._UNSET`.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/source/test_base.py`, replace `TestReconfiguration` with:

```python
class TestReconfiguration:
    def test_naming_a_field_changes_it_on_a_copy(self):
        source = Cascade()
        copy = source(dataset="raw")
        assert copy.dataset == "raw"
        assert source.dataset is None

    def test_a_wrong_value_is_refused(self):
        with pytest.raises(ValidationError):
            Cascade()(region=42)

    def test_an_unknown_name_is_refused(self):
        with pytest.raises(TypeError, match="no field or relation"):
            Cascade()(bogus=1)

    def test_a_disabled_copy_disables_its_assets(self):
        copy = Cascade()(enabled=False)
        assert copy.plain.enabled is False

    def test_an_inherited_none_inherits_again(self):
        source = Cascade(dataset="raw")
        asset = source.plain(dataset="own")
        assert asset(dataset=None).dataset == "raw"

    def test_naming_a_relation_rebinds_it(self):
        replacement = FakeConnection(token="b")
        copy = Shop(connection=FakeConnection(token="a"))(connection=replacement)
        assert copy.orders.connection is replacement  # ty: ignore[unresolved-attribute]

    def test_none_clears_an_optional_relation(self):
        copy = Shop(connection=FakeConnection(token="a"), destinations=[FakeDestination()])(destinations=None)
        assert copy.destinations == []

    def test_a_copy_rewires_siblings_and_shares_outside_targets(self):
        connection = FakeConnection(token="t")
        source = Shop(connection=connection)
        copy = source(dataset="raw")
        assert copy.revenue.orders is copy.orders  # ty: ignore[unresolved-attribute]
        assert copy.orders is not source.orders  # ty: ignore[unresolved-attribute]
        assert copy.orders.parent is copy  # ty: ignore[unresolved-attribute]
        assert copy.connection is connection

    def test_the_original_is_left_alone(self):
        source = Shop(connection=FakeConnection(token="a"))
        source(connection=FakeConnection(token="b"))
        assert source.connection.token == "a"
```

In `packages/interloper-core/tests/asset/test_base.py`, the reconfiguration tests that passed `None` to mean "unchanged" or used the `normalizer` sentinel are rewritten to the rule (name means change); `asset(enabled=False)` tests keep passing. Add:

```python
    def test_normalizer_none_clears_without_a_sentinel(self):
        asset = FakeAsset(normalizer=il.Normalizer())
        assert asset(normalizer=None).normalizer is None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/source/test_base.py::TestReconfiguration -q`
Expected: FAIL (`Source.__call__` does not accept `region` or `bogus`, raises nothing for a wrong value, ...).

- [ ] **Step 3: `Component.__call__`**

In `packages/interloper-core/src/interloper/component/base.py` (import `deepcopy` from `copy`), a new `# -- Reconfiguration --` section before `# -- Serialization & resolution --`:

```python
    def __call__(self, **changes: Any) -> Self:
        """Return a reconfigured copy: naming something changes it, leaving it out keeps it.

        A field name sets the field, validated as an assignment would be; on
        an inherited field ``None`` means "inherit again". A relation name
        rebinds through the write path: one component, a list, or ``None`` to
        clear. The copy keeps its own bindings (a new dict, the same targets)
        and its owner; the components it owns are copied and owned by the
        copy, still bound to each other, while every outside target they bind
        is shared, not duplicated.

        Args:
            **changes: Field and relation names to their new values.

        Returns:
            The reconfigured copy.

        Raises:
            TypeError: If a name is neither a field nor a declared relation.
        """
        cls = type(self)
        unknown = sorted(name for name in changes if name not in cls.model_fields and name not in cls.relations)
        if unknown:
            raise TypeError(f"{cls.__name__} has no field or relation named: {', '.join(unknown)}")
        copy = self.model_copy()
        copy._bound = {name: list(targets) for name, targets in self._bound.items()}
        owned = self._owned_by_field()
        if owned:
            children = [child for kids in owned.values() for child in kids]
            memo: dict[int, Any] = {id(self): copy}
            for component in (self, *children):
                for targets in component._bound.values():
                    for target in targets:
                        if all(target is not child for child in children):
                            memo[id(target)] = target
            for field in owned:
                copy.__dict__[field] = deepcopy(self.__dict__[field], memo)
        for name, value in changes.items():
            if name in cls.model_fields:
                cls.__pydantic_validator__.validate_assignment(copy, name, value)
        for name, value in changes.items():
            if name in cls.relations:
                setattr(copy, name, value)
        return copy
```

- [ ] **Step 4: Delete the two hand-written `__call__`s**

Delete `Asset.__call__` and `_UNSET` from `asset/base.py`, and `Source.__call__` from `source/base.py`, with their now-unused imports (`Self` where unused). The DAG's `upstream(enabled=False)` and `target(enabled=True)` keep working through `Component.__call__`.

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests -q`
Expected: PASS. Rewrite any remaining test that relied on `None` meaning "unchanged". Ledger each.

- [ ] **Step 6: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 7: Checkpoint (no commit)**

---

### Task 8: Surface, docs and live verification

**Files:**
- Modify: `docs/guide/sources.md`, `docs/guide/resources.md`, `docs/guide/decorators.md`, `docs/guide/assets.md`, `docs/guide/dependencies.md`, `docs/guide/views.md`, `docs/extending/components.md`
- Modify: `plugins/interloper/skills/interloper-upgrade/SKILL.md` and any skill naming `select`, `asset_def`, `AssetRef` or trickling into assets
- Modify: interloper-assets tests carrying now-unneeded `# ty: ignore[unresolved-attribute]`

- [ ] **Step 1: Find every stale mention**

Run: `grep -rnE "select=|\.select\b|asset_def|AssetRef|trickl|_read_upstreams|_normalize_and_conform|_definition\(|_view_destination|_read_destination|_effective_schema|reconfigur|repoint" docs plugins examples --include='*.md' --include='*.py' | grep -v docs/superpowers`
Expected: the list the spec's section 7 names (sources.md 117, 126, 143, 177; resources.md 57, 100, 136; decorators.md 125 to 137; assets.md 149; dependencies.md 248; extending/components.md 129 to 143), plus anything new.

- [ ] **Step 2: Rewrite the guides**

- `sources.md`: replace the trickling and repointing paragraphs with the cascade rule: an asset reads what it leaves unset from its source, settings and relations, on every read; a dataset may name a source field in braces; `source.orders` is the live asset and `Shop.orders` the class; declaring assets in the class body is the typed form; the `assets` override map narrows a source (replacing the `select` example); delete the `asset_def` sentence.
- `resources.md`: the trickling section becomes "Reading through the source": an unbound relation on an asset reads the source's accepted targets; a source still trickles into its destinations.
- `decorators.md`: the reconfiguration table becomes one rule for every component (name changes it, `None` clears a relation and makes an inherited field inherit again, unknown names are refused).
- `assets.md`: reconfiguration example in the new rule; one paragraph on `default_destination`.
- `dependencies.md` line 248: "a source's assets read its bindings through it" replaces "a source trickles into its assets".
- `views.md`: name the three hooks if it describes extension points.
- `extending/components.md`: document `inherit=True`, `Component.inherited`, and the three asset hooks; the `on_rebind` example trickles into destinations only.

- [ ] **Step 3: Upgrade table and skills**

Add one row to the table in `plugins/interloper/skills/interloper-upgrade/SKILL.md`, after the phase 1 row:

| unreleased (core phase 2) | `Source(select=[...])`; `Source.asset_def(key)`; `AssetRef`; `asset(normalizer=...)` sentinel and `None` meaning "unchanged" in `asset(...)`/`source(...)`; assets holding copies of their source's dataset, default destination key, normalizer, retry and strategy; `View._read_upstreams`/`_definition`, `IbisView._read_upstreams`/`_definition` | `Source(enabled=False)` for a read-only source, the `assets` override map to narrow one; `Shop.asset_types` and `Shop.<key>.definition()`; `Shop.<key>` (class) and `source.<key>` (live asset); `component(**changes)`: naming changes, `None` clears a relation or makes an inherited field inherit again; an asset runs only while it and its source are enabled; inherited fields read through the source on every read, specs carry overrides only; `_upstream`/`_output`/`_land` |

Update any other skill text naming the removed members the same way.

- [ ] **Step 4: Remove unneeded type ignores**

Run: `UV_FROZEN=1 uv run ty check 2>&1 | grep -i "unused" | head`
Remove every `# ty: ignore[unresolved-attribute]` it reports as unused, across packages.

- [ ] **Step 5: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 6: Live verification**

Use the project `verify` skill: `INTERLOPER_SERVER_PORT=3100 make dev-up`, then through the API: the catalog serves the demo source with its assets; an asset's `config_schema` marks `materialization_strategy` with `x-inherit`; a run of the demo source for one day completes; the demo source's stored asset configs carry no copied dataset. Tear down and delete the minted session row.

- [ ] **Step 7: Checkpoint (no commit)**
