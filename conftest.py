"""Fixtures shared across every package's test suite.

The OpenTelemetry global providers are set-once per process, and the whole
workspace runs in one pytest process — so the span exporter and metric
reader here are process-wide singletons installed on first use, rather
than per-package instances that would silently lose their provider to
whichever suite ran first.

SQLite stands in for Postgres in every suite. It stores no offset, so a
TIMESTAMPTZ column would read back naive where Postgres hands back an aware
value; every stored instant is UTC, so the SQLite dialect is taught here,
once per process, to read datetimes back aware.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import types
from sqlalchemy.dialects.sqlite.base import DATETIME, SQLiteDialect
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import Dialect


class _AwareDATETIME(DATETIME):
    """SQLite's DATETIME reading back aware UTC, as Postgres' TIMESTAMPTZ does."""

    def result_processor(self, dialect: Dialect, coltype: object) -> Callable[[Any], datetime | None]:
        """Parse the stored text, then stamp UTC onto the naive result.

        Args:
            dialect: The SQLite dialect reading the value.
            coltype: The column's DB-API type code.

        Returns:
            The processor turning a stored value into an aware datetime.
        """
        parse = super().result_processor(dialect, coltype)

        def to_aware(value: Any) -> datetime | None:
            parsed = parse(value) if parse is not None else value
            if parsed is not None and parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed

        return to_aware


for dialect_class in (SQLiteDialect, SQLiteDialect_pysqlite):
    dialect_class.colspecs = {**dialect_class.colspecs, types.DateTime: _AwareDATETIME}

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
