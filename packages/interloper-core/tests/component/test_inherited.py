"""Tests for ``interloper.component.inherited``."""

from typing import Any

import pytest
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

    def test_the_class_has_no_value_like_any_pydantic_field(self):
        with pytest.raises(AttributeError):
            Held.label

    def test_a_subclass_keeps_the_parents_default(self):
        class Defaulted(Component):
            flag: bool = Field(default=True, json_schema_extra={"x-inherit": True})

        class Child(Defaulted):
            pass

        assert Child.model_fields["flag"].default is True


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
        assert not isinstance(vars(Held).get("note"), InheritedSetting)

    def test_a_subclass_redeclaring_the_field_keeps_inheriting(self):
        assert isinstance(vars(Narrowed)["label"], InheritedSetting)
        assert _held(Holder(label="owner"), cls=Narrowed).label == "declared"
        assert _held(Holder(label="owner"), cls=Narrowed, label=None).label == "owner"
