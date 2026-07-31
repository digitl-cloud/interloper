"""Test support for span assertions.

The OTel global tracer provider is set-once per process, so every test
suite that asserts on spans must share one provider and one in-memory
exporter — this module owns both. Importing it requires the SDK (the
``otel`` extra or the dev dependency group).
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_exporter = InMemorySpanExporter()
_installed = False


def install_span_exporter() -> InMemorySpanExporter:
    """Install (once) the shared in-memory exporter and clear it.

    Returns:
        The process-wide exporter, cleared of previously captured spans.
    """
    global _installed
    if not _installed:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(_exporter))
        trace.set_tracer_provider(provider)
        _installed = True
    _exporter.clear()
    return _exporter
