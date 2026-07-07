"""Tests for ``interloper.component.registry``."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
from pydantic import Field

import interloper as il
from interloper.component.base import Component, RelationDefinition
from interloper.component.registry import KindRegistry


class FakeKind(Component):
    """A satellite-style kind with its own relation vocabulary."""

    sensitive: ClassVar[bool] = True
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "link": RelationDefinition(kinds=["asset"], field="links", slotted=True),
    }

    links: dict[str, Any] = Field(default_factory=dict)


class FakeConcreteKind(FakeKind):
    """A concrete class of the fake kind (inherits kind ``fake_kind``)."""


class TestBuiltins:
    """The framework's kinds are registered on package import."""

    def test_builtin_kinds_present(self):
        for kind in ("source", "asset", "destination", "resource", "connection", "config", "job"):
            assert kind in il.KINDS

    def test_runnable_kinds(self):
        for kind, expected in (("job", True), ("source", True), ("asset", True), ("connection", False)):
            assert il.KINDS.runnable(kind) is expected

    def test_sensitive_follows_the_resource_subtree(self):
        assert il.KINDS.sensitive("connection") is True
        assert il.KINDS.sensitive("config") is True
        assert il.KINDS.sensitive("source") is False
        assert il.KINDS.sensitive("job") is False

    def test_relation_vocabulary_from_anchors(self):
        assert set(il.KINDS.relation_types("asset")) == {"resource", "destination", "dependency"}
        assert il.KINDS.relation_types("job")["target"].kinds == ["source", "asset"]
        assert il.KINDS.relation_types("nope") == {}


class TestRegistration:
    """Anchor resolution and idempotency."""

    def test_registering_a_subclass_anchors_the_kind(self):
        registry = KindRegistry()
        registry.register(FakeConcreteKind)
        assert registry.get("fake_kind") is FakeKind

    def test_first_anchor_wins(self):
        registry = KindRegistry()
        registry.register(FakeKind)
        registry.register(FakeConcreteKind)
        assert registry.get("fake_kind") is FakeKind

    def test_misdeclared_relation_field_is_rejected(self):
        class Broken(Component):
            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "link": RelationDefinition(kinds=["asset"], field="linkss"),
            }

        registry = KindRegistry()
        with pytest.raises(ValueError, match="no such field"):
            registry.register(Broken)

    def test_kind_properties_flow_from_the_anchor(self):
        registry = KindRegistry()
        registry.register(FakeConcreteKind)
        assert registry.sensitive("fake_kind") is True
        assert registry.relation_types("fake_kind")["link"].slotted is True

    def test_unregistered_kind(self):
        registry = KindRegistry()
        assert registry.get("fake_kind") is None
        assert "fake_kind" not in registry
        assert registry.sensitive("fake_kind") is False
