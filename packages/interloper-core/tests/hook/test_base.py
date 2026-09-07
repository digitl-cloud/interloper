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
        assert not issubclass(il.KINDS["hook"], il.Operation)
        assert il.KINDS["hook"].sensitive is False

    def test_anchor_relations(self):
        # The anchor is only an observer; TriggerHook extends it with `targets`.
        relations = il.KINDS["hook"].relations
        assert set(relations) == {"watches"}
        watches = relations["watches"]
        assert (watches.kinds(), watches.many, watches.optional, watches.on_delete) == (
            ["source", "asset", "job"],
            True,
            True,
            "detach",
        )

    def test_trigger_hook_extends_the_relations(self):
        relations = il.TriggerHook.relations
        targets = relations["targets"]
        assert (targets.kinds(), targets.many, targets.optional, targets.on_delete) == (
            ["source", "asset", "job"],
            True,
            True,
            "block",
        )
        assert relations["watches"].kinds() == ["source", "asset", "job"]  # inherited, not replaced

    def test_webhook_hook_has_no_targets(self):
        assert "targets" not in il.WebhookHook.relations
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            il.WebhookHook(url="https://x.test", targets=[])  # type: ignore[call-arg]  # ty: ignore[unknown-argument]

    def test_state_model(self):
        assert il.KINDS["hook"].state_model is il.HookState
        assert il.KINDS["job"].state_model is il.JobState
        assert il.KINDS["source"].state_model is None

    def test_definition_describes_state(self):
        assert set(il.Hook.definition().state_schema["properties"]) == {"last_fired_at", "last_run_id"}
        assert set(il.Job.definition().state_schema["properties"]) == {"next_run_at", "last_run_at"}
        assert il.Source.definition().state_schema == {}

    def test_config_schema_hides_relation_fields(self):
        schema = il.Hook.config_schema()
        assert "events" in schema["properties"]
        assert "enabled" in schema["properties"]
        assert "watches" not in schema["properties"]
        assert "targets" not in schema["properties"]

    def test_hooks_declared_in_every_catalog(self):
        from interloper.catalog import Catalog

        components = Catalog.from_paths([]).components
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

    def test_watches_bind_from_the_constructor(self):
        source = FakeSource()
        assert FakeNotifyHook(watches=[source]).watches == [source]

    def test_a_destination_is_not_watchable(self):
        from interloper.errors import ConfigError

        with pytest.raises(ConfigError, match="does not accept"):
            FakeNotifyHook(watches=[il.MemoryDestination()])  # ty: ignore[invalid-argument-type]


class TestTriggerFire:
    """The trigger hook acts on its targets through the injected capability."""

    def test_fire_triggers_every_target(self):
        triggered: list[str] = []
        job = il.Job()
        hook = il.TriggerHook(watches=[FakeSource()], targets=[job])
        hook.fire(il.HookContext(event_type="run_completed", component_id="c1", trigger=triggered.append))
        assert triggered == [job.id]

    def test_fire_without_a_trigger_capability_is_an_error(self):
        from interloper.errors import ConfigError

        hook = il.TriggerHook(targets=[il.Job()])
        with pytest.raises(ConfigError, match="trigger capability"):
            hook.fire(il.HookContext(event_type="run_completed", component_id="c1"))


class TestSpecRoundTrip:
    """Hooks serialize like every other component."""

    def test_round_trip_preserves_the_hook_config(self):
        hook = FakeNotifyHook(events=["run_completed"])
        restored = FakeNotifyHook.from_spec(hook.to_spec())
        assert restored.events == ["run_completed"]

    @pytest.mark.xfail(strict=True, reason="Task 7: relations are not serialised yet")
    def test_round_trip_preserves_watches(self):
        hook = FakeNotifyHook(watches=[FakeSource()], events=["run_completed"])
        restored = FakeNotifyHook.from_spec(hook.to_spec())
        assert [type(w).key for w in restored.watches] == ["fake_source"]
