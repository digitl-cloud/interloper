"""Tests for the launcher registry."""

from __future__ import annotations

import pytest
from interloper.errors import ConfigError
from interloper.settings import LauncherSettings, PostgresSettings, RunnerSettings

from interloper_scheduler.launcher import LAUNCHERS, InProcessLauncher, Launcher


class TestRegistry:
    """Entry-point discovery of launchers."""

    def test_all_workspace_launchers_are_discovered(self):
        # Every launcher — including the built-in — registers through the
        # entry-point group; this asserts discovery end to end.
        assert {"in_process", "docker", "kubernetes"} <= set(LAUNCHERS.keys())

    def test_registry_maps_keys_to_classes(self):
        assert LAUNCHERS["in_process"] is InProcessLauncher
        assert LAUNCHERS["docker"].__name__ == "DockerLauncher"
        assert LAUNCHERS["kubernetes"].__name__ == "KubernetesLauncher"


class TestFromSettings:
    """Settings-driven construction through the registry."""

    def test_builds_in_process_launcher(self):
        launcher = Launcher.from_settings(
            LauncherSettings(type="in_process"),
            postgres=PostgresSettings(),
            runner=RunnerSettings(type="serial"),
            catalog=None,  # in-process launcher does not consume the catalog
        )
        assert isinstance(launcher, InProcessLauncher)
        assert launcher._runner_type == "serial"

    def test_unknown_type_raises_actionable_error(self):
        with pytest.raises(ConfigError, match=r"Unknown launcher: 'nomad'.*available.*docker.*in_process.*kubernetes"):
            Launcher.from_settings(
                LauncherSettings(type="nomad"),
                postgres=PostgresSettings(),
                runner=RunnerSettings(),
                catalog=None,
            )
