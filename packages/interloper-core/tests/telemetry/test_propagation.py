"""Tests for ``interloper.telemetry.propagation``."""

from __future__ import annotations

from opentelemetry import trace

from interloper.telemetry.propagation import (
    context_from_env,
    extract_metadata,
    inject_metadata,
    traceparent_env,
)
from interloper.telemetry.tracer import tracer


class TestMetadataCarrier:
    def test_round_trip(self, span_exporter):
        metadata = {"run_id": "r1"}
        with tracer().start_as_current_span("outer") as span:
            inject_metadata(metadata)

        assert "traceparent" in metadata
        ctx = extract_metadata(metadata)
        assert ctx is not None
        extracted = trace.get_current_span(ctx).get_span_context()
        assert extracted.trace_id == span.get_span_context().trace_id

    def test_extract_without_traceparent_returns_none(self):
        assert extract_metadata({"run_id": "r1"}) is None

    def test_extract_ignores_non_string_values(self, span_exporter):
        metadata = {"run_id": "r1", "count": 3}
        with tracer().start_as_current_span("outer"):
            inject_metadata(metadata)
        assert extract_metadata(metadata) is not None


class TestEnvCarrier:
    def test_traceparent_env_round_trip(self, span_exporter, monkeypatch):
        with tracer().start_as_current_span("outer") as span:
            env = traceparent_env()

        assert "TRACEPARENT" in env
        monkeypatch.setenv("TRACEPARENT", env["TRACEPARENT"])
        ctx = context_from_env()
        assert ctx is not None
        extracted = trace.get_current_span(ctx).get_span_context()
        assert extracted.trace_id == span.get_span_context().trace_id

    def test_no_active_span_yields_empty_env(self, span_exporter):
        assert traceparent_env() == {}

    def test_unset_env_returns_none(self, monkeypatch):
        monkeypatch.delenv("TRACEPARENT", raising=False)
        assert context_from_env() is None
