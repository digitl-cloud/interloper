"""Tests for ``interloper.runner.base``."""

from __future__ import annotations

import pytest

from interloper.errors import ConfigError
from interloper.runner import build_runner
from interloper.runner.base import runners
from interloper.runner.serial import SerialRunner


class TestRegistry:
    """Entry-point discovery of runners."""

    def test_all_workspace_runners_are_discovered(self):
        # Built-ins register through core's own pyproject; docker/k8s through
        # theirs — asserting the discovery end to end.
        assert {"serial", "multi_thread", "multi_process", "docker", "kubernetes"} <= set(runners())

    def test_registry_maps_keys_to_classes(self):
        registry = runners()
        assert registry["serial"] is SerialRunner
        assert registry["docker"].__name__ == "DockerRunner"
        assert registry["kubernetes"].__name__ == "KubernetesRunner"

    def test_k8s_is_a_compat_alias_for_kubernetes(self):
        # Pre-existing configs use runner.type=k8s; both keys resolve to the
        # same class so deployed values keep working.
        registry = runners()
        assert registry["k8s"] is registry["kubernetes"]


class TestBuildRunner:
    """Key resolution and kwargs forwarding."""

    def test_resolves_class_and_forwards_kwargs(self):
        cls, kwargs = build_runner("serial", {"fail_fast": True})
        assert cls is SerialRunner
        assert kwargs == {"fail_fast": True}

    def test_unknown_type_raises_actionable_error(self):
        with pytest.raises(ConfigError, match=r"Unknown runner: 'ray'.*available.*docker.*serial"):
            build_runner("ray")
