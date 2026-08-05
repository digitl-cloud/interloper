"""OpenTelemetry SDK initialization and shutdown.

Everything outside this module only touches ``opentelemetry-api`` (whose
calls are no-ops without an SDK). This module is the single place that
imports the SDK — lazily, inside function bodies — so the framework runs
identically whether or not the ``interloper[otel]`` extra is installed.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider

    from interloper.settings import TelemetrySettings
    from interloper.telemetry.metrics import OtelMetricsHandler

logger = logging.getLogger(__name__)

_tracer_provider: TracerProvider | None = None
_meter_provider: MeterProvider | None = None
_metrics_handler: OtelMetricsHandler | None = None
_initialized = False


def _parse_headers(headers: str) -> dict[str, str] | None:
    if not headers:
        return None
    return dict(pair.split("=", 1) for pair in headers.split(",") if "=" in pair)


def _exporter_kwargs(settings: TelemetrySettings) -> dict[str, Any]:
    """Exporter constructor kwargs — only what is explicitly configured.

    Unset fields are omitted so the SDK's native ``OTEL_EXPORTER_OTLP_*``
    environment variables still apply.

    Returns:
        Keyword arguments for the OTLP exporter constructors.
    """
    kwargs: dict[str, Any] = {}
    if settings.endpoint:
        kwargs["endpoint"] = settings.endpoint
    headers = _parse_headers(settings.headers)
    if headers:
        kwargs["headers"] = headers
    return kwargs


def init_telemetry(settings: TelemetrySettings) -> bool:
    """Initialize the OpenTelemetry SDK from interloper settings.

    Idempotent; a fast no-op when telemetry is disabled. When enabled but
    the ``interloper[otel]`` extra is not installed, logs a warning and
    leaves the no-op API providers in place — telemetry must never take
    the data plane down.

    ``service.name`` defaults to ``interloper``; deployments that need
    distinct names (api, scheduler, run pods) set ``service_name`` where
    they are defined — e.g. ``INTERLOPER_OTEL_SERVICE_NAME`` per chart
    deployment, or the launchers for the pods they spawn.

    Args:
        settings: The resolved telemetry settings.

    Returns:
        True when the SDK was (or already is) active.
    """
    global _tracer_provider, _meter_provider, _metrics_handler, _initialized

    if _initialized:
        return True
    if not settings.enabled:
        return False

    try:
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.resources import Resource
    except ImportError:
        logger.warning(
            "Telemetry is enabled but the OpenTelemetry SDK is not installed; "
            "install the 'otel' extra (pip install 'interloper[otel]') to export telemetry."
        )
        return False

    from interloper.telemetry.tracer import _version

    grpc = settings.protocol == "grpc"
    exporter_kwargs = _exporter_kwargs(settings)
    resource = Resource.create(
        {
            "service.name": settings.service_name or "interloper",
            "service.version": _version() or "unknown",
        }
    )

    if settings.traces:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

        if grpc:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        _tracer_provider = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(settings.sample_ratio)),
        )
        _tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(**exporter_kwargs)))
        trace.set_tracer_provider(_tracer_provider)

    if settings.metrics:
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        if grpc:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        _meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(**exporter_kwargs, preferred_temporality=_delta_temporality()),
                    export_interval_millis=settings.metric_export_interval * 1000,
                )
            ],
            views=_duration_views(),
        )
        metrics.set_meter_provider(_meter_provider)

        _metrics_handler = _register_metrics_handler()

    _instrument_libraries()

    _initialized = True
    logger.info(
        "Telemetry initialized (service=%s, protocol=%s)", settings.service_name or "interloper", settings.protocol
    )
    return True


#: Bucket boundaries for the duration histograms, in seconds. The SDK's
#: defaults (0, 5, 10, 25 … 10000) are tuned for milliseconds — against
#: second-valued durations every run under 5s falls in one bucket and every
#: quantile is interpolated across it. These span a fast asset to a long load.
_DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0)


def _delta_temporality() -> dict[type, Any]:
    """Export sums and histograms as deltas rather than running totals.

    Runs are short-lived, and a cumulative point is only meaningful next to
    a previous one: a process that lives seconds exports its counter once,
    with no earlier sample to difference against, so the work it did is
    invisible to ``rate()``/``increase()`` and every new process restarts
    the series from zero. A delta point ("N happened since my last export")
    is self-contained, and the collector's ``deltatocumulative`` processor
    accumulates the deltas into one continuous series it owns for far
    longer than any single run.

    Up/down counters stay cumulative — they measure a level, not a flow,
    so a delta of one would be meaningless on its own.

    Returns:
        Preferred temporality per instrument kind, for the OTLP exporter.
    """
    from opentelemetry.sdk.metrics import (
        Counter,
        Histogram,
        ObservableCounter,
        ObservableGauge,
        ObservableUpDownCounter,
        UpDownCounter,
    )
    from opentelemetry.sdk.metrics.export import AggregationTemporality

    return {
        Counter: AggregationTemporality.DELTA,
        Histogram: AggregationTemporality.DELTA,
        ObservableCounter: AggregationTemporality.DELTA,
        UpDownCounter: AggregationTemporality.CUMULATIVE,
        ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
        ObservableGauge: AggregationTemporality.CUMULATIVE,
    }


def _duration_views() -> list[Any]:
    """Views giving the duration histograms second-scaled buckets.

    Returns:
        One view per duration instrument.
    """
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View

    return [
        View(
            instrument_name=name,
            aggregation=ExplicitBucketHistogramAggregation(_DURATION_BUCKETS),
        )
        for name in ("interloper.run.duration", "interloper.asset.duration")
    ]


def _register_metrics_handler() -> OtelMetricsHandler | None:
    """Subscribe the metrics handler on the event bus.

    Docker/k8s child containers stream their events to the host, which
    re-emits them — the host is authoritative for metrics, so children
    (marked by ``INTERLOPER_EVENTS_TO_STDERR``) skip the handler entirely.
    Traces are unaffected: spans exist where code runs.

    Returns:
        The subscribed handler, or ``None`` in a child container.
    """
    if os.environ.get("INTERLOPER_EVENTS_TO_STDERR") == "true":
        return None

    from interloper.events import EventBus
    from interloper.telemetry.metrics import OtelMetricsHandler

    handler = OtelMetricsHandler()
    EventBus.subscribe(handler, event_types=OtelMetricsHandler.EVENT_TYPES)
    return handler


def _instrument_libraries(*, enable: bool = True) -> None:
    """Toggle the contrib instrumentors that ship with the ``otel`` extra.

    Each is optional and global: SQLAlchemy patches engines created after
    this point (init runs before any Store is built), httpx covers the
    framework's REST clients and user code alike.

    Args:
        enable: Instrument when True, uninstrument when False.
    """
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        SQLAlchemyInstrumentor().instrument() if enable else SQLAlchemyInstrumentor().uninstrument()
    except ImportError:
        pass
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument() if enable else HTTPXClientInstrumentor().uninstrument()
    except ImportError:
        pass


def instrument_fastapi(app: Any) -> None:
    """Instrument a FastAPI app for request tracing.

    A no-op unless telemetry is active and the FastAPI instrumentor
    (part of the ``otel`` extra) is installed.

    Args:
        app: The FastAPI application instance.
    """
    if not _initialized:
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    except ImportError:
        return
    FastAPIInstrumentor.instrument_app(app, excluded_urls="/api/health")


def force_flush() -> None:
    """Flush pending telemetry without shutting the SDK down.

    For reused worker processes (e.g. process pools) that must not lose
    spans if the pool is torn down abruptly.
    """
    if _tracer_provider is not None:
        _tracer_provider.force_flush()
    if _meter_provider is not None:
        _meter_provider.force_flush()


def shutdown_telemetry() -> None:
    """Flush and shut down the SDK providers (idempotent)."""
    global _tracer_provider, _meter_provider, _metrics_handler, _initialized

    if _initialized:
        _instrument_libraries(enable=False)
    if _metrics_handler is not None:
        from interloper.events import EventBus

        # Deliver queued events to the handler before the provider goes away.
        EventBus.flush(timeout=5.0)
        EventBus.unsubscribe(_metrics_handler)
        _metrics_handler = None
    if _tracer_provider is not None:
        _tracer_provider.shutdown()
        _tracer_provider = None
    if _meter_provider is not None:
        _meter_provider.shutdown()
        _meter_provider = None
    _initialized = False
