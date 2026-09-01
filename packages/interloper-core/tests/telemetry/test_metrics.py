"""Tests for ``interloper.telemetry.metrics``."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from interloper.events import Event, EventBus, EventType
from interloper.telemetry.metrics import OtelMetricsHandler
from interloper.telemetry.setup import _register_metrics_handler

_T0 = dt.datetime(2026, 7, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _event(event_type: EventType, offset_s: float = 0.0, **metadata: Any) -> Event:
    return Event(type=event_type, timestamp=_T0 + dt.timedelta(seconds=offset_s), metadata=metadata)


@pytest.fixture
def points(metric_reader: Any) -> Any:
    """Data-point lookup over the shared in-memory metric reader.

    Returns:
        A callable filtering data points by metric name and attributes.
    """
    reader = metric_reader

    def _points(name: str, **attrs: str) -> list[Any]:
        found: list[Any] = []
        data = reader.get_metrics_data()
        for rm in data.resource_metrics if data else []:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name != name:
                        continue
                    for point in metric.data.data_points:
                        point_attrs = point.attributes or {}
                        if all(point_attrs.get(k) == v for k, v in attrs.items()):
                            found.append(point)
        return found

    return _points


class TestOtelMetricsHandler:
    def test_run_counter_and_duration(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.RUN_STARTED, 0, run_id="run-m1"))
        handler(_event(EventType.RUN_COMPLETED, 12.5, run_id="run-m1"))

        (counter,) = points("interloper.runs", status="completed")
        assert counter.value >= 1
        (duration,) = points("interloper.run.duration", status="completed")
        assert duration.sum >= 12.5

    def test_operation_counter_and_duration_by_key(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.OPERATION_STARTED, 0, run_id="run-m2", component_id="a1", component_key="orders_m2"))
        handler(_event(EventType.OPERATION_FAILED, 3.0, run_id="run-m2", component_id="a1", component_key="orders_m2"))

        (counter,) = points("interloper.operations", status="failed", component_key="orders_m2")
        assert counter.value == 1
        (duration,) = points("interloper.operation.duration", status="failed", component_key="orders_m2")
        assert duration.sum == 3.0

    def test_destination_io_counter(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.DEST_WRITE_COMPLETED, run_id="run-m3", destination_key="bq_m3"))
        handler(_event(EventType.DEST_READ_FAILED, run_id="run-m3", destination_key="bq_m3"))

        (write,) = points("interloper.destination.io", operation="write", status="completed", destination_key="bq_m3")
        assert write.value == 1
        (read,) = points("interloper.destination.io", operation="read", status="failed", destination_key="bq_m3")
        assert read.value == 1

    def test_duplicate_event_ids_count_once(self, points):
        # Docker/k8s hosts re-emit child events under the same deterministic id.
        handler = OtelMetricsHandler()
        event = _event(EventType.OPERATION_COMPLETED, run_id="run-m4", component_id="a1", component_key="orders_m4")
        handler(event)
        handler(event)

        (counter,) = points("interloper.operations", status="completed", component_key="orders_m4")
        assert counter.value == 1

    def test_terminal_without_start_still_counts(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.RUN_FAILED, run_id="run-m5"))

        (counter,) = points("interloper.runs", status="failed")
        assert counter.value >= 1
        assert not points("interloper.run.duration", status="failed")

    def test_tracking_stores_are_capped(self, points):
        handler = OtelMetricsHandler(max_tracked=10)
        for i in range(50):
            handler(_event(EventType.RUN_STARTED, run_id=f"run-m6-{i}"))

        assert len(handler._seen) <= 10
        assert len(handler._started) <= 10

    def test_handler_exceptions_are_contained(self, points):
        handler = OtelMetricsHandler()
        broken = _event(EventType.RUN_COMPLETED)
        broken.metadata = None  # ty: ignore[invalid-assignment]
        handler(broken)  # must not raise


class TestRegistration:
    def test_child_containers_skip_the_handler(self, metric_reader, monkeypatch):
        monkeypatch.setenv("INTERLOPER_EVENTS_TO_STDERR", "true")
        assert _register_metrics_handler() is None

    def test_host_subscribes_the_handler(self, metric_reader, monkeypatch):
        monkeypatch.delenv("INTERLOPER_EVENTS_TO_STDERR", raising=False)
        handler = _register_metrics_handler()
        try:
            assert handler is not None
            assert handler in EventBus()._handlers
            assert EventBus()._handlers[handler] == OtelMetricsHandler.EVENT_TYPES
        finally:
            if handler is not None:
                EventBus.unsubscribe(handler)


class TestDestinationKeyAttribute:
    def test_absent_destination_key_is_omitted(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.DEST_READ_COMPLETED, run_id="run-m7"))

        (point,) = points("interloper.destination.io", operation="read", status="completed")
        assert point.attributes is not None
        assert "destination_key" not in point.attributes


class TestPlatformIdentity:
    """Run-level instruments carry the scheduler-threaded identity."""

    def test_run_metrics_carry_org_and_target(self, points):
        handler = OtelMetricsHandler()
        identity = {"org_id": "org-p1", "target_kind": "job", "target_key": "nightly", "target_name": "Nightly"}
        handler(_event(EventType.RUN_STARTED, 0, run_id="run-p1", **identity))
        handler(_event(EventType.RUN_COMPLETED, 4.0, run_id="run-p1", **identity))

        (counter,) = points("interloper.runs", org_id="org-p1", target_kind="job", target_key="nightly")
        assert counter.value == 1
        # target_name is deliberately not a metric attribute (mutable, 1:1 with key).
        assert "target_name" not in (counter.attributes or {})
        (duration,) = points("interloper.run.duration", org_id="org-p1", target_key="nightly")
        assert duration.sum == 4.0

    def test_standalone_runs_omit_identity_attributes(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.RUN_FAILED, run_id="run-p2"))

        matches = [
            p for p in points("interloper.runs", status="failed")
            if "org_id" not in (p.attributes or {}) and "target_key" not in (p.attributes or {})
        ]
        assert matches

    def test_operation_metrics_stay_identity_free(self, points):
        handler = OtelMetricsHandler()
        handler(
            _event(
                EventType.OPERATION_COMPLETED,
                run_id="run-p3", component_id="a1", component_key="orders_p3",
                org_id="org-p1", target_key="nightly",
            )
        )
        (counter,) = points("interloper.operations", component_key="orders_p3")
        assert "org_id" not in (counter.attributes or {})
        assert "target_key" not in (counter.attributes or {})
