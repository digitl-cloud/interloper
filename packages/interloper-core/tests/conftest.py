"""Shared test fixtures."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# The OTel global tracer provider is set-once per process, so every test that
# asserts on spans shares this exporter (cleared per test) instead of
# installing its own provider.
_SPAN_EXPORTER = InMemorySpanExporter()
_PROVIDER_INSTALLED = False


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    """In-memory span exporter behind the process-wide tracer provider.

    Returns:
        The shared exporter, cleared for this test.
    """
    global _PROVIDER_INSTALLED
    if not _PROVIDER_INSTALLED:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
        trace.set_tracer_provider(provider)
        _PROVIDER_INSTALLED = True
    _SPAN_EXPORTER.clear()
    return _SPAN_EXPORTER
