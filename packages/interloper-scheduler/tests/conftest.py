"""Shared test fixtures."""

from __future__ import annotations

import pytest
from interloper.telemetry.testing import install_span_exporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    """The process-wide in-memory span exporter, cleared for this test.

    Returns:
        The shared exporter.
    """
    return install_span_exporter()
