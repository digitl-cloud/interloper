"""Tests for ``interloper.component.registry``."""

from __future__ import annotations

from typing import ClassVar

import interloper as il
from interloper.component.base import Component, RelationDefinition
from interloper.component.registry import KindRegistry


class FakeSensor(Component):
    """A satellite-style kind with its own relation vocabulary."""

    sensitive: ClassVar[bool] = True
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "watches": RelationDefinition(kinds=["asset"], slotted=True),
    }


class FakeNestSensor(FakeSensor):
    """A concrete class of the fake kind (inherits kind ``fake_sensor``)."""


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
        registry.register(FakeNestSensor)
        assert registry.get("fake_sensor") is FakeSensor

    def test_first_anchor_wins(self):
        registry = KindRegistry()
        registry.register(FakeSensor)
        registry.register(FakeNestSensor)
        assert registry.get("fake_sensor") is FakeSensor

    def test_kind_properties_flow_from_the_anchor(self):
        registry = KindRegistry()
        registry.register(FakeNestSensor)
        assert registry.sensitive("fake_sensor") is True
        assert registry.relation_types("fake_sensor")["watches"].slotted is True

    def test_unregistered_kind(self):
        registry = KindRegistry()
        assert registry.get("fake_sensor") is None
        assert "fake_sensor" not in registry
        assert registry.sensitive("fake_sensor") is False
