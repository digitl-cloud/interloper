"""Tests for ``interloper.telemetry.metrics``."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any

import pytest

from interloper.events import Event, EventBus, EventType
from interloper.telemetry.metrics import OtelMetricsHandler
from interloper.telemetry.setup import _register_metrics_handler
from interloper.telemetry.testing import install_metric_reader

_T0 = dt.datetime(2026, 7, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _event(event_type: EventType, offset_s: float = 0.0, **metadata: Any) -> Event:
    return Event(type=event_type, timestamp=_T0 + dt.timedelta(seconds=offset_s), metadata=metadata)


@pytest.fixture
def points() -> Any:
    """Data-point lookup over the shared in-memory metric reader.

    Returns:
        A callable filtering data points by metric name and attributes.
    """
    reader = install_metric_reader()

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

    def test_asset_counter_and_duration_by_key(self, points):
        handler = OtelMetricsHandler()
        handler(_event(EventType.ASSET_STARTED, 0, run_id="run-m2", asset_id="a1", asset_key="orders_m2"))
        handler(_event(EventType.ASSET_FAILED, 3.0, run_id="run-m2", asset_id="a1", asset_key="orders_m2"))

        (counter,) = points("interloper.assets", status="failed", asset_key="orders_m2")
        assert counter.value == 1
        (duration,) = points("interloper.asset.duration", status="failed", asset_key="orders_m2")
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
        event = _event(EventType.ASSET_COMPLETED, run_id="run-m4", asset_id="a1", asset_key="orders_m4")
        handler(event)
        handler(event)

        (counter,) = points("interloper.assets", status="completed", asset_key="orders_m4")
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
    @pytest.fixture(autouse=True)
    def _meter_provider(self) -> Iterator[None]:
        install_metric_reader()
        yield

    def test_child_containers_skip_the_handler(self, monkeypatch):
        monkeypatch.setenv("INTERLOPER_EVENTS_TO_STDERR", "true")
        assert _register_metrics_handler() is None

    def test_host_subscribes_the_handler(self, monkeypatch):
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
