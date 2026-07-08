"""Tests for ``interloper.hook.base``."""

from typing import Any, ClassVar

import pytest

import interloper as il


class FakeNotifyHook(il.Hook):
    """Concrete hook capturing its firings."""

    fired: ClassVar[list[il.HookContext]] = []

    def fire(self, context: il.HookContext) -> None:
        type(self).fired.append(context)


class FakeSource(il.Source):
    """Source fixture to watch."""

    class One(il.Asset):
        """Single asset."""

        def data(self) -> Any:  # pragma: no cover
            return [{"x": 1}]


class TestDefinition:
    """The hook kind self-describes like every other kind."""

    def test_kind_and_registration(self):
        assert il.Hook.kind == "hook"
        assert "hook" in il.KINDS
        assert il.KINDS.get("hook") is il.Hook
        assert il.KINDS.runnable("hook") is False
        assert il.KINDS.sensitive("hook") is False

    def test_vocabulary(self):
        relations = il.KINDS.relation_types("hook")
        assert relations["watch"].field == "watches"
        assert relations["watch"].kinds == ["source", "asset", "job"]
        assert relations["target"].field == "targets"
        assert relations["resource"].slotted is True

    def test_state_model(self):
        assert il.KINDS.state_model("hook") is il.HookState
        assert il.KINDS.state_model("job") is il.JobState
        assert il.KINDS.state_model("source") is None

    def test_config_schema_hides_relation_fields(self):
        schema = il.Hook.config_schema()
        assert "events" in schema["properties"]
        assert "enabled" in schema["properties"]
        assert "watches" not in schema["properties"]
        assert "targets" not in schema["properties"]

    def test_builtins_always_in_catalog(self):
        from interloper.catalog.base import _with_builtins

        components = _with_builtins({})
        assert "trigger_hook" in components
        assert "webhook_hook" in components


class TestFire:
    """The fire contract."""

    def test_base_fire_is_abstract(self):
        with pytest.raises(NotImplementedError):
            il.Hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))

    def test_concrete_hook_receives_context(self):
        FakeNotifyHook.fired.clear()
        hook = FakeNotifyHook(watches=[FakeSource()], events=["run_failed"])
        context = il.HookContext(event_type="run_failed", component_id="c1", run_id="r1")
        hook.fire(context)
        assert FakeNotifyHook.fired == [context]

    def test_default_events(self):
        assert il.Hook().events == ["run_failed"]


class TestSpecRoundTrip:
    """Hooks serialize like every other component."""

    def test_round_trip_preserves_watches(self):
        hook = FakeNotifyHook(watches=[FakeSource()], events=["run_completed"])
        restored = FakeNotifyHook.from_spec(hook.to_spec())
        assert restored.events == ["run_completed"]
        assert [type(w).key for w in restored.watches] == ["fake_source"]
