"""Tests for ``interloper.runner.base``."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import pytest

import interloper as il
from interloper.errors import ConfigError, PartitionError
from interloper.events import Event
from interloper.partitioning.time import TimePartition, TimePartitionConfig
from interloper.runner.async_runner import AsyncRunner
from interloper.runner.base import RUNNERS
from interloper.runner.results import ExecutionStatus
from interloper.runner.serial import SerialRunner
from interloper.settings import RunnerSettings


class TestRegistry:
    """Entry-point discovery of runners."""

    def test_all_workspace_runners_are_discovered(self):
        # Built-ins register through core's own pyproject; docker/k8s through
        # theirs — asserting the discovery end to end.
        assert {"async", "serial", "multi_process", "docker", "kubernetes"} <= set(RUNNERS.keys())

    def test_registry_maps_keys_to_classes(self):
        registry = dict(RUNNERS.items())
        assert registry["async"] is AsyncRunner
        assert registry["serial"] is SerialRunner
        assert registry["docker"].__name__ == "DockerRunner"
        assert registry["kubernetes"].__name__ == "KubernetesRunner"

    def test_k8s_is_a_compat_alias_for_kubernetes(self):
        # Pre-existing configs use runner.type=k8s; both keys resolve to the
        # same class so deployed values keep working.
        registry = dict(RUNNERS.items())
        assert registry["k8s"] is registry["kubernetes"]

    def test_serial_is_the_async_engine_with_one_slot(self):
        # In-process execution is the single async-native engine; ``serial``
        # is simply that engine bounded to one concurrency slot.
        assert issubclass(SerialRunner, AsyncRunner)
        assert SerialRunner().max_workers == 1


class TestFromSettings:
    """Settings-driven construction through the registry."""

    def test_constructs_the_configured_runner(self):
        runner = il.Runner.from_settings(RunnerSettings(type="async", config={"max_workers": 2}))
        assert type(runner) is AsyncRunner
        assert runner.max_workers == 2

    def test_default_type_is_async(self):
        assert type(il.Runner.from_settings(RunnerSettings())) is AsyncRunner

    def test_unknown_type_raises_actionable_error(self):
        with pytest.raises(ConfigError, match=r"Unknown runner: 'ray'.*available.*async.*serial"):
            il.Runner.from_settings(RunnerSettings(type="ray"))

    def test_subclass_scoped_resolution(self):
        with pytest.raises(ConfigError, match="does not resolve to a SerialRunner"):
            SerialRunner.from_settings(RunnerSettings(type="async"))


class TestOnEventScoping:
    """``on_event`` only receives its own run's events.

    The EventBus is a process-wide singleton, so when two runs execute
    concurrently in one process (the in-process launcher) an unscoped
    subscription would deliver each run's events to *both* handlers — and
    the scheduler's handler persists whatever it receives under its own
    run_id, cross-attributing events between runs.
    """

    async def test_concurrent_runs_do_not_cross_deliver(self):
        il.MemoryDestination.clear()

        # Each asset waits for the other run's asset to start, forcing the
        # two runs (and their bus subscriptions) to overlap in time.
        started = {"one": asyncio.Event(), "two": asyncio.Event()}

        @il.asset()
        async def ping() -> list[dict[str, Any]]:
            started["one"].set()
            await asyncio.wait_for(started["two"].wait(), timeout=5)
            return [{"x": 1}]

        @il.asset()
        async def pong() -> list[dict[str, Any]]:
            started["two"].set()
            await asyncio.wait_for(started["one"].wait(), timeout=5)
            return [{"x": 2}]

        events_one: list[Event] = []
        events_two: list[Event] = []
        await asyncio.gather(
            AsyncRunner(on_event=events_one.append).run(
                il.DAG(ping(id="ping", destinations=[il.MemoryDestination()])),
                metadata={"run_id": "run-one"},
            ),
            AsyncRunner(on_event=events_two.append).run(
                il.DAG(pong(id="pong", destinations=[il.MemoryDestination()])),
                metadata={"run_id": "run-two"},
            ),
        )

        assert events_one and all(e.metadata.get("run_id") == "run-one" for e in events_one)
        assert events_two and all(e.metadata.get("run_id") == "run-two" for e in events_two)

    async def test_run_id_is_generated_and_events_still_delivered(self):
        il.MemoryDestination.clear()

        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        events: list[Event] = []
        await AsyncRunner(on_event=events.append).run(il.DAG(solo(id="solo", destinations=[il.MemoryDestination()])))

        assert events
        run_ids = {e.metadata.get("run_id") for e in events}
        assert len(run_ids) == 1
        assert None not in run_ids


class TestPreflightValidation:
    """DAG-wide partition validation, raised before any asset executes."""

    async def test_partitioned_assets_require_a_partition(self):
        il.MemoryDestination.clear()
        calls: list[str] = []

        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily() -> list[dict[str, Any]]:
            calls.append("daily")
            return [{"date": "2026-01-01"}]

        @il.asset()
        def static() -> list[dict[str, Any]]:
            calls.append("static")
            return [{"x": 1}]

        dag = il.DAG(
            daily(id="daily", destinations=[il.MemoryDestination()]),
            static(id="static", destinations=[il.MemoryDestination()]),
        )
        with pytest.raises(PartitionError, match=r"requires a partition.*daily"):
            await AsyncRunner().run(dag)

        # The run fails as a whole: not even the non-partitioned asset ran.
        assert calls == []

    async def test_partition_satisfies_the_preflight(self):
        il.MemoryDestination.clear()

        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily() -> list[dict[str, Any]]:
            return [{"date": "2026-01-01"}]

        dag = il.DAG(daily(id="daily", destinations=[il.MemoryDestination()]))
        result = await AsyncRunner().run(dag, TimePartition(dt.date(2026, 1, 1)))

        assert result.status is ExecutionStatus.COMPLETED

    def test_non_materializable_partitioned_assets_are_exempt(self):
        # Upstream dependencies are hydrated read-only (materializable=False);
        # they must not force a partition onto an otherwise unpartitioned run.
        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def upstream() -> list[dict[str, Any]]:
            return [{"date": "2026-01-01"}]

        asset = upstream(id="upstream", destinations=[il.MemoryDestination()])
        asset.materializable = False

        AsyncRunner()._preflight_validation(il.DAG(asset), None)


class TestTelemetrySpans:
    """Inline spans emitted along the execution path."""

    async def test_span_tree_and_traceparent_on_events(self, span_exporter):
        il.MemoryDestination.clear()

        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        events: list[Event] = []
        await AsyncRunner(on_event=events.append).run(
            il.DAG(solo(id="solo", destinations=[il.MemoryDestination()])),
            metadata={"run_id": "run-1"},
        )

        spans = {s.name: s for s in span_exporter.get_finished_spans()}
        assert {
            "interloper.runner.run",
            "interloper.asset.materialize",
            "interloper.asset.data",
            "interloper.asset.normalize_and_conform",
            "interloper.destination.write",
        } <= set(spans)

        run_span = spans["interloper.runner.run"]
        asset_span = spans["interloper.asset.materialize"]
        assert asset_span.parent is not None and asset_span.parent.span_id == run_span.context.span_id
        children = (
            "interloper.asset.data",
            "interloper.asset.normalize_and_conform",
            "interloper.destination.write",
        )
        for child in children:
            assert spans[child].parent is not None
            assert spans[child].parent.span_id == asset_span.context.span_id

        assert run_span.attributes is not None
        assert run_span.attributes["interloper.run.id"] == "run-1"
        assert run_span.attributes["interloper.runner.type"] == "async_runner"
        assert asset_span.attributes is not None
        assert asset_span.attributes["interloper.asset.key"] == "solo"

        # The run span's context rides the metadata dict into every event —
        # the correlation hook for logs and cross-process children.
        trace_id = f"{run_span.context.trace_id:032x}"
        assert events
        for event in events:
            assert trace_id in event.metadata.get("traceparent", "")

    async def test_failed_run_marks_spans_as_error(self, span_exporter):
        from opentelemetry.trace import StatusCode

        @il.asset()
        def boom() -> list[dict[str, Any]]:
            raise ValueError("nope")

        result = await AsyncRunner().run(il.DAG(boom(id="boom", destinations=[il.MemoryDestination()])))
        assert result.status is ExecutionStatus.FAILED

        spans = {s.name: s for s in span_exporter.get_finished_spans()}
        assert spans["interloper.runner.run"].status.status_code is StatusCode.ERROR
        assert spans["interloper.asset.materialize"].status.status_code is StatusCode.ERROR
        assert spans["interloper.asset.data"].status.status_code is StatusCode.ERROR

    async def test_remote_parent_from_metadata(self, span_exporter):
        from interloper.telemetry.propagation import inject_metadata
        from interloper.telemetry.tracer import tracer

        il.MemoryDestination.clear()

        @il.asset()
        def solo() -> list[dict[str, Any]]:
            return [{"x": 1}]

        metadata: dict[str, Any] = {}
        with tracer().start_as_current_span("producer") as producer:
            inject_metadata(metadata)

        await AsyncRunner().run(il.DAG(solo(id="solo", destinations=[il.MemoryDestination()])), metadata=metadata)

        run_span = next(s for s in span_exporter.get_finished_spans() if s.name == "interloper.runner.run")
        assert run_span.context.trace_id == producer.get_span_context().trace_id
        assert run_span.parent is not None
        assert run_span.parent.span_id == producer.get_span_context().span_id
