"""Fixtures shared across every package's test suite.

The OpenTelemetry global providers are set-once per process, and the whole
workspace runs in one pytest process — so the span exporter and metric
reader here are process-wide singletons installed on first use, rather
than per-package instances that would silently lose their provider to
whichever suite ran first.
"""

from __future__ import annotations

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_span_exporter = InMemorySpanExporter()
_metric_reader = InMemoryMetricReader()
_tracing_installed = False
_metrics_installed = False


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    """The process-wide in-memory span exporter, cleared for this test.

    Returns:
        The shared exporter.
    """
    global _tracing_installed
    if not _tracing_installed:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(_span_exporter))
        trace.set_tracer_provider(provider)
        _tracing_installed = True
    _span_exporter.clear()
    return _span_exporter


@pytest.fixture
def metric_reader() -> InMemoryMetricReader:
    """The process-wide in-memory metric reader.

    Counters are cumulative for the life of the process, so assert on
    deltas or use test-unique attribute values rather than absolute counts.

    Returns:
        The shared reader.
    """
    global _metrics_installed
    if not _metrics_installed:
        metrics.set_meter_provider(MeterProvider(metric_readers=[_metric_reader]))
        _metrics_installed = True
    return _metric_reader
