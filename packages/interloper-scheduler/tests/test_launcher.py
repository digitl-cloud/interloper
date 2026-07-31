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


class TestInProcessLauncherTelemetry:
    """The launch thread inherits the launch-time OTel context."""

    def test_launch_thread_attaches_launch_context(self, span_exporter, monkeypatch):
        import threading
        from uuid import uuid4

        from interloper.telemetry.tracer import tracer
        from opentelemetry import trace as otel_trace

        from interloper_scheduler.executor import RunExecutor

        captured: dict[str, int] = {}
        done = threading.Event()

        def fake_execute(self: RunExecutor, run_id: object) -> bool:
            captured["trace_id"] = otel_trace.get_current_span().get_span_context().trace_id
            done.set()
            return True

        monkeypatch.setattr(RunExecutor, "execute", fake_execute)

        launcher = InProcessLauncher(store=object())  # ty: ignore[invalid-argument-type]  # store is never touched
        with tracer().start_as_current_span("interloper.run.launch") as span:
            launcher.launch(uuid4())

        assert done.wait(5)
        assert captured["trace_id"] == span.get_span_context().trace_id
