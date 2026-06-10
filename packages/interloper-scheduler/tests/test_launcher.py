"""Tests for the launcher registry."""

from __future__ import annotations

import pytest
from interloper.errors import ConfigError
from interloper.settings import LauncherSettings, PostgresSettings, RunnerSettings

from interloper_scheduler.launcher import InProcessLauncher, build_launcher, launchers


class TestRegistry:
    """Entry-point discovery of launchers."""

    def test_all_workspace_launchers_are_discovered(self):
        # Every launcher — including the built-in — registers through the
        # entry-point group; this asserts discovery end to end.
        registry = launchers()
        assert {"in_process", "docker", "kubernetes"} <= set(registry)

    def test_registry_maps_keys_to_classes(self):
        registry = launchers()
        assert registry["in_process"] is InProcessLauncher
        assert registry["docker"].__name__ == "DockerLauncher"
        assert registry["kubernetes"].__name__ == "KubernetesLauncher"


class TestBuildLauncher:
    """Settings-driven construction through the registry."""

    def test_builds_in_process_launcher(self):
        launcher = build_launcher(
            LauncherSettings(type="in_process"),
            postgres=PostgresSettings(),
            runner=RunnerSettings(type="serial"),
            catalog=None,  # in-process launcher does not consume the catalog
        )
        assert isinstance(launcher, InProcessLauncher)
        assert launcher._runner_type == "serial"

    def test_unknown_type_raises_actionable_error(self):
        with pytest.raises(ConfigError, match=r"Unknown launcher: 'nomad'.*available.*docker.*in_process.*kubernetes"):
            build_launcher(
                LauncherSettings(type="nomad"),
                postgres=PostgresSettings(),
                runner=RunnerSettings(),
                catalog=None,
            )
