"""Tests for the launcher registry."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner.spec import RunSpec
from interloper.settings import LauncherSettings, ReaperSettings, RunnerSettings, ServerSettings
from interloper_db import NotFoundError, RunStatus
from interloper_db.models import Run

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


def _spec(run_id: UUID) -> RunSpec:
    dag = il.DAG(_Solo(id=str(uuid4()), destinations=[il.MemoryDestination()])).to_spec()
    return RunSpec(dag=dag, metadata={"run_id": str(run_id), "org_id": "org-1"})


def _wait_for(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Wait until *condition* holds, as a launched run's thread gets there on its own.

    Args:
        condition: What to wait for.
        timeout: Seconds before giving up.
    """
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "the run's thread never got there"
        time.sleep(0.01)


class _Store:
    """The store a launched run loads its spec from and reports through, recording every batch."""

    def __init__(self, spec: RunSpec | Exception | None) -> None:
        """Serve *spec*, or raise it.

        Args:
            spec: What ``runs.spec`` returns, or raises.
        """
        self.batches: list[tuple[UUID, list[il.Event]]] = []
        self.done = threading.Event()
        self.runs = SimpleNamespace(spec=self._spec, ingest=self._ingest)
        self._served = spec

    def _spec(self, run_id: UUID) -> RunSpec | None:
        if isinstance(self._served, Exception):
            raise self._served
        return self._served

    def _ingest(self, run_id: UUID, batch: il.EventBatch) -> bool:
        self.batches.append((run_id, list(batch.events)))
        if any(event.type in (il.EventType.RUN_COMPLETED, il.EventType.RUN_FAILED) for event in batch.events):
            self.done.set()
        return True

    @property
    def events(self) -> list[il.Event]:
        """Every event delivered, in order."""
        return [event for _, batch in self.batches for event in batch]


class TestInProcessLauncher:
    """A run launched in this process loads its spec and reports straight through the store."""

    def test_a_launched_run_loads_its_spec_and_reports_every_event_through_ingest(self) -> None:
        run = _dispatched_run()
        store = _Store(_spec(run.id))
        launcher = InProcessLauncher(runner_type="serial", store=store)  # ty: ignore[invalid-argument-type]

        launcher.launch(run, "ilr_unused")

        assert store.done.wait(5)
        assert {run_id for run_id, _ in store.batches} == {run.id}
        assert store.events[0].type is il.EventType.RUN_STARTED
        assert store.events[0].metadata["org_id"] == "org-1"

    @pytest.mark.parametrize("served", [NotFoundError("Run targets a component that was deleted"), None])
    def test_a_run_that_cannot_be_built_fails_with_the_reason(self, served: Exception | None) -> None:
        run = _dispatched_run()
        store = _Store(served)
        launcher = InProcessLauncher(runner_type="serial", store=store)  # ty: ignore[invalid-argument-type]

        launcher.launch(run, "ilr_unused")

        assert store.done.wait(5)
        (failed,) = store.events
        assert failed.type is il.EventType.RUN_FAILED
        assert ("deleted" if served else "selects no operations") in failed.metadata["error"]

    def test_a_run_ended_elsewhere_ends_its_thread_quietly(self, caplog: pytest.LogCaptureFixture) -> None:
        run = _dispatched_run()
        store = _Store(_spec(run.id))
        store.runs.ingest = lambda run_id, batch: store._ingest(run_id, batch) and False
        launcher = InProcessLauncher(runner_type="serial", store=store)  # ty: ignore[invalid-argument-type]

        with caplog.at_level(logging.INFO, logger="interloper_scheduler.launcher"):
            launcher.launch(run, "ilr_unused")
            assert store.done.wait(5)
            _wait_for(
                lambda: any(
                    record.name == "interloper_scheduler.launcher" and "was ended elsewhere" in record.getMessage()
                    for record in caplog.records
                )
            )

        ended = [record for record in caplog.records if record.name == "interloper_scheduler.launcher"]
        assert [(record.levelno, "was ended elsewhere" in record.getMessage()) for record in ended][-1] == (
            logging.INFO,
            True,
        )

    def test_a_run_that_breaks_is_logged_with_its_traceback(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        from interloper.events import EventRecorder

        def finish(self: EventRecorder, *args: Any) -> None:
            raise RuntimeError("recorder bug")

        monkeypatch.setattr(EventRecorder, "finish", finish)
        run = _dispatched_run()
        launcher = InProcessLauncher(runner_type="serial", store=_Store(_spec(run.id)))  # ty: ignore[invalid-argument-type]

        with caplog.at_level(logging.ERROR, logger="interloper_scheduler.launcher"):
            launcher.launch(run, "ilr_unused")
            _wait_for(lambda: "did not complete" in caplog.text)

        (record,) = [record for record in caplog.records if "did not complete" in record.getMessage()]
        assert record.exc_info is not None and "recorder bug" in str(record.exc_info[1])

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
        run = _dispatched_run()
        launcher = InProcessLauncher(store=_Store(_spec(run.id)))  # ty: ignore[invalid-argument-type]

        with tracer().start_as_current_span("interloper.launcher.launch") as span:
            launcher.launch(run, "ilr_unused")

        assert done.wait(5)
        assert captured["trace_id"] == span.get_span_context().trace_id


class TestCommand:
    """The argv a container launcher starts a run's pod with."""

    def test_the_pod_executes_the_run_by_its_id(self) -> None:
        run = _dispatched_run()
        run.partition_key = "2026-10-26"

        # The DAG, the partition and the metadata are the run's own, which the pod loads.
        assert Launcher.command(run) == ["interloper", "launch", str(run.id)]


class TestDiagnoseDefault:
    """The base launcher cannot introspect its runs."""

    def test_it_has_no_diagnosis(self):
        # The reaper then records its own reason alone.
        class MinimalLauncher(Launcher):
            """Launcher implementing only the required half."""

            def launch(self, run, token):  # pragma: no cover - unused
                raise NotImplementedError

        assert MinimalLauncher().diagnose(uuid4()) is None
