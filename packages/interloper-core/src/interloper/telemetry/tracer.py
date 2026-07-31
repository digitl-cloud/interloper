"""Cached tracer/meter acquisition.

Only depends on ``opentelemetry-api``: before (or without) SDK
initialization the returned instruments are no-op proxies that pick up
the real providers once :func:`interloper.telemetry.init_telemetry`
installs them.
"""

from __future__ import annotations

import importlib.metadata
from functools import cache

from opentelemetry import metrics, trace


def _version() -> str | None:
    try:
        return importlib.metadata.version("interloper-core")
    except importlib.metadata.PackageNotFoundError:
        return None


@cache
def tracer() -> trace.Tracer:
    """Return the framework tracer.

    Returns:
        The ``interloper`` tracer (a proxy delegating to the active provider).
    """
    return trace.get_tracer("interloper", _version())


@cache
def meter() -> metrics.Meter:
    """Return the framework meter.

    Returns:
        The ``interloper`` meter (a proxy delegating to the active provider).
    """
    return metrics.get_meter("interloper", _version() or "")
