"""Tests for ``interloper.telemetry.setup``."""

from __future__ import annotations

import os
import sys
from typing import Any

import pytest

from interloper.settings import TelemetrySettings
from interloper.telemetry import setup
from interloper.telemetry.setup import _exporter_kwargs, _parse_headers, init_telemetry, shutdown_telemetry


@pytest.fixture(autouse=True)
def _reset_state():
    yield
    shutdown_telemetry()


class TestInitTelemetry:
    def test_disabled_is_a_noop(self):
        assert init_telemetry(TelemetrySettings(enabled=False)) is False
        assert setup._initialized is False

    def test_missing_sdk_warns_and_stays_noop(self, monkeypatch, caplog):
        # A None sys.modules entry makes the SDK import raise ImportError,
        # simulating an install without the otel extra.
        monkeypatch.setitem(sys.modules, "opentelemetry.sdk.resources", None)
        with caplog.at_level("WARNING", logger="interloper.telemetry.setup"):
            assert init_telemetry(TelemetrySettings(enabled=True)) is False
        assert "interloper[otel]" in caplog.text
        assert setup._initialized is False

    def test_enabled_is_idempotent(self):
        # Both signals off: initialization completes without installing
        # global providers (which are set-once per process).
        settings = TelemetrySettings(enabled=True, traces=False, metrics=False)
        assert init_telemetry(settings) is True
        assert init_telemetry(settings) is True
        assert setup._initialized is True

    def test_shutdown_without_init_is_safe(self):
        shutdown_telemetry()
        assert setup._initialized is False

    def test_opts_into_stable_http_semconv(self, monkeypatch):
        monkeypatch.delenv("OTEL_SEMCONV_STABILITY_OPT_IN", raising=False)
        init_telemetry(TelemetrySettings(enabled=True, traces=False, metrics=False))
        assert os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] == "http"

    def test_operator_semconv_override_wins(self, monkeypatch):
        monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "http/dup")
        init_telemetry(TelemetrySettings(enabled=True, traces=False, metrics=False))
        assert os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] == "http/dup"

    def test_instrument_fastapi_is_a_noop_when_uninitialized(self):
        setup.instrument_fastapi(object())  # must not raise or touch the app


class TestExporterConfig:
    def test_parse_headers(self):
        assert _parse_headers("") is None
        assert _parse_headers("a=1,b=2") == {"a": "1", "b": "2"}
        assert _parse_headers("token=abc=def") == {"token": "abc=def"}

    def test_kwargs_only_carry_configured_fields(self):
        assert _exporter_kwargs(TelemetrySettings()) == {}
        kwargs = _exporter_kwargs(TelemetrySettings(endpoint="http://collector:4317", headers="x=y"))
        assert kwargs == {"endpoint": "http://collector:4317", "headers": {"x": "y"}}


class TestDurationBuckets:
    """The duration histograms need second-scaled buckets.

    The SDK's defaults start (0, 5, 10, 25, …) — tuned for milliseconds. Left
    alone, every run and asset finishing under 5s lands in a single bucket and
    histogram_quantile interpolates across it, reporting seconds-off values
    that look plausible on a dashboard.
    """

    def test_covers_sub_second_to_long_running(self):
        assert setup._DURATION_BUCKETS[0] < 0.1
        assert setup._DURATION_BUCKETS[-1] >= 3600
        assert list(setup._DURATION_BUCKETS) == sorted(setup._DURATION_BUCKETS)
        # Sub-second resolution is the point: several boundaries below 1s.
        assert len([b for b in setup._DURATION_BUCKETS if b < 1.0]) >= 4

    def test_views_cover_both_duration_instruments(self):
        views = setup._duration_views()
        assert {v._instrument_name for v in views} == {
            "interloper.run.duration",
            "interloper.operation.duration",
        }
        for view in views:
            assert view._aggregation._boundaries == setup._DURATION_BUCKETS

    def test_views_apply_to_the_handler_histograms(self, monkeypatch):
        # A view only takes effect when its name matches the instrument the
        # handler actually creates, so exercise the handler against a private
        # provider carrying the views and read the exported bucket bounds back.
        import datetime as dt

        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        from interloper.events import Event, EventType
        from interloper.telemetry import metrics as metrics_module
        from interloper.telemetry.metrics import OtelMetricsHandler

        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader], views=setup._duration_views())
        monkeypatch.setattr(metrics_module, "meter", lambda: provider.get_meter("interloper"))

        t0 = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
        handler = OtelMetricsHandler()
        handler(Event(type=EventType.RUN_STARTED, timestamp=t0, metadata={"run_id": "r"}))
        handler(Event(type=EventType.RUN_COMPLETED, timestamp=t0, metadata={"run_id": "r"}))
        operation = {"run_id": "r", "component_id": "c", "component_key": "k"}
        handler(Event(type=EventType.OPERATION_STARTED, timestamp=t0, metadata=operation))
        handler(Event(type=EventType.OPERATION_COMPLETED, timestamp=t0, metadata=operation))

        data: Any = reader.get_metrics_data()
        bounds = {
            metric.name: tuple(point.explicit_bounds)
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            if metric.name.endswith(".duration")
            for point in metric.data.data_points
        }
        assert bounds == {
            "interloper.run.duration": setup._DURATION_BUCKETS,
            "interloper.operation.duration": setup._DURATION_BUCKETS,
        }


class TestDeltaTemporality:
    """Sums and histograms are exported as deltas, not running totals.

    A run lives seconds and exports its counter once, with no earlier
    sample to be differenced against — so under cumulative temporality the
    work it did is invisible to rate()/increase(), and the next run
    restarts the series from zero. Deltas are self-contained; the
    collector accumulates them into one series that outlives any run.
    """

    def test_flows_are_delta_and_levels_stay_cumulative(self):
        from opentelemetry.sdk.metrics import (
            Counter,
            Histogram,
            ObservableCounter,
            ObservableUpDownCounter,
            UpDownCounter,
        )
        from opentelemetry.sdk.metrics.export import AggregationTemporality

        preference = setup._delta_temporality()

        for instrument in (Counter, Histogram, ObservableCounter):
            assert preference[instrument] is AggregationTemporality.DELTA
        # A level is meaningless as a delta of one.
        for instrument in (UpDownCounter, ObservableUpDownCounter):
            assert preference[instrument] is AggregationTemporality.CUMULATIVE
