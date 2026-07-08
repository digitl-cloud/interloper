"""Tests for ``interloper.hook.trigger``."""

from typing import Any

import pytest

import interloper as il
from interloper.errors import ConfigError


class FakeTargetAsset(il.Asset):
    """Asset fixture used as a trigger target."""

    def data(self) -> Any:  # pragma: no cover
        return [{"x": 1}]


class TestTriggerHook:
    def test_fires_trigger_capability_per_target(self):
        a, b = FakeTargetAsset(), FakeTargetAsset()
        triggered: list[str] = []
        hook = il.TriggerHook(targets=[a, b])
        hook.fire(il.HookContext(event_type="run_completed", component_id="c1", trigger=triggered.append))
        assert triggered == [a.id, b.id]

    def test_missing_capability_raises(self):
        with pytest.raises(ConfigError, match="without a trigger capability"):
            il.TriggerHook(targets=[FakeTargetAsset()]).fire(
                il.HookContext(event_type="run_completed", component_id="c1")
            )
