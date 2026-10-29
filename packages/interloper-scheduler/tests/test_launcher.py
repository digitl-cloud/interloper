"""Tests for the launcher registry."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.dag import DAGSpec
from interloper.errors import ConfigError
from interloper.settings import LauncherSettings, ReaperSettings, RunnerSettings, ServerSettings
from interloper_db import RunStatus
from interloper_db.models import Component, Run

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
            server=ServerSettings(),
            runner=RunnerSettings(type="serial"),
            reaper=ReaperSettings(),
            catalog=None,  # in-process launcher does not consume the catalog
        )
        assert isinstance(launcher, InProcessLauncher)
        assert launcher._runner_type == "serial"

    def test_unknown_type_raises_actionable_error(self):
        with pytest.raises(ConfigError, match=r"Unknown launcher: 'nomad'.*available.*docker.*in_process.*kubernetes"):
            Launcher.from_settings(
                LauncherSettings(type="nomad"),
                server=ServerSettings(),
                runner=RunnerSettings(),
                reaper=ReaperSettings(),
                catalog=None,
            )


class _Solo(il.Asset):
    """One row."""

    def data(self) -> list[dict[str, Any]]:
        return [{"x": 1}]


def _dispatched_run() -> Run:
    return Run(id=uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED)


def _spec() -> DAGSpec:
    return il.DAG(_Solo(id=str(uuid4()), destinations=[il.MemoryDestination()])).to_spec()


class TestInProcessLauncher:
    """A run launched in this process reports straight through the store's ingest."""

    def test_a_launched_run_reports_every_event_through_ingest(self) -> None:
        delivered: list[tuple[UUID, list[il.Event]]] = []
        done = threading.Event()

        def ingest(run_id: UUID, batch: il.EventBatch) -> bool:
            delivered.append((run_id, list(batch.events)))
            if any(event.type is il.EventType.RUN_COMPLETED for event in batch.events):
                done.set()
            return True

        store = SimpleNamespace(runs=SimpleNamespace(ingest=ingest))
        launcher = InProcessLauncher(runner_type="serial", store=store)  # ty: ignore[invalid-argument-type]
        run = _dispatched_run()

        launcher.launch(run, _spec(), "ilr_unused")

        assert done.wait(5)
        events = [event for _, batch in delivered for event in batch]
        assert {run_id for run_id, _ in delivered} == {run.id}
        assert events[0].type is il.EventType.RUN_STARTED

    def test_the_launch_thread_carries_the_launch_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from interloper.events import EventRecorder
        from interloper.telemetry.tracer import tracer
        from opentelemetry import trace as otel_trace

        captured: dict[str, int] = {}
        done = threading.Event()

        def finish(self: EventRecorder, *args: Any) -> None:
            captured["trace_id"] = otel_trace.get_current_span().get_span_context().trace_id
            done.set()

        monkeypatch.setattr(EventRecorder, "finish", finish)
        launcher = InProcessLauncher(store=SimpleNamespace(runs=SimpleNamespace(ingest=lambda *args: True)))  # ty: ignore[invalid-argument-type]

        with tracer().start_as_current_span("interloper.launcher.launch") as span:
            launcher.launch(_dispatched_run(), _spec(), "ilr_unused")

        assert done.wait(5)
        assert captured["trace_id"] == span.get_span_context().trace_id


class TestCommand:
    """The argv a container launcher starts a run's pod with."""

    def test_it_runs_the_spec_file_with_the_runs_ids_and_partition(self) -> None:
        run = _dispatched_run()
        run.partition_key = "2026-10-26"

        argv = Launcher.command(run, "@/interloper/run/spec.json")

        assert argv[:5] == ["interloper", "run", "--format", "inline", "@/interloper/run/spec.json"]
        assert f"--run-id={run.id}" in argv
        assert json.loads(argv[argv.index("--metadata") + 1])["run_id"] == str(run.id)
        assert argv[-2:] == ["--date", "2026-10-26"]

    def test_an_unpartitioned_run_has_no_date(self) -> None:
        assert "--date" not in Launcher.command(_dispatched_run(), "@/spec.json")

    def test_the_run_observes_now_on_its_jobs_clock(self) -> None:
        run = _dispatched_run()
        run.target = Component(org_id=run.org_id, kind="job", key="cron_job", config={"timezone": "Asia/Kolkata"})
        before = datetime.now(timezone.utc)

        argv = Launcher.command(run, "@/spec.json")

        [flag] = [argument for argument in argv if argument.startswith("--observed-at=")]
        observed_at = datetime.fromisoformat(flag.removeprefix("--observed-at="))
        assert observed_at.utcoffset() == timedelta(hours=5, minutes=30)
        assert before <= observed_at <= datetime.now(timezone.utc)


class TestDiagnoseDefault:
    """The base launcher cannot introspect its runs."""

    def test_it_has_no_diagnosis(self):
        # The reaper then records its own reason alone.
        class MinimalLauncher(Launcher):
            """Launcher implementing only the required half."""

            def launch(self, run, spec, token):  # pragma: no cover - unused
                raise NotImplementedError

        assert MinimalLauncher().diagnose(uuid4()) is None
