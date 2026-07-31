"""W3C trace-context propagation over the framework's two carriers.

Trace context crosses interloper's boundaries on two channels:

- the run ``metadata`` dict (``traceparent`` key), which flows unchanged
  from :meth:`Runner.run` through ``RunState`` into every event and into
  ``MultiProcessRunner`` workers;
- the ``TRACEPARENT`` / ``TRACESTATE`` environment variables, the only
  channel into spawned processes and containers (launchers, docker/k8s
  runner children).
"""

from __future__ import annotations

import os
from typing import Any

from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_propagator = TraceContextTextMapPropagator()

TRACEPARENT_ENV = "TRACEPARENT"
TRACESTATE_ENV = "TRACESTATE"


def inject_metadata(metadata: dict[str, Any]) -> None:
    """Write the current span context into a run metadata dict (in place)."""
    _propagator.inject(metadata)


def extract_metadata(metadata: dict[str, Any]) -> Context | None:
    """Read a remote parent context from a run metadata dict.

    Args:
        metadata: Run metadata possibly carrying a ``traceparent`` key.

    Returns:
        The extracted context, or ``None`` when no ``traceparent`` is present.
    """
    if not metadata.get("traceparent"):
        return None
    return _propagator.extract({k: v for k, v in metadata.items() if isinstance(v, str)})


def traceparent_env() -> dict[str, str]:
    """Render the current span context as environment variables.

    Returns:
        ``{"TRACEPARENT": ..., "TRACESTATE": ...}`` (empty when there is no
        active span) — merge into a child process/container environment.
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return {key.upper(): value for key, value in carrier.items()}


def child_process_env() -> dict[str, str]:
    """Telemetry environment for a spawned process or container.

    Forwards this process's ``INTERLOPER_OTEL_*`` configuration (so the
    child initializes its own exporter) together with the current trace
    context — merge into the child's environment.

    Returns:
        Environment variable mapping (possibly empty).
    """
    env = {key: value for key, value in os.environ.items() if key.startswith("INTERLOPER_OTEL_")}
    env.update(traceparent_env())
    return env


def context_from_env() -> Context | None:
    """Read a remote parent context from this process's environment.

    Returns:
        The extracted context, or ``None`` when ``TRACEPARENT`` is unset.
    """
    traceparent = os.environ.get(TRACEPARENT_ENV)
    if not traceparent:
        return None
    carrier = {"traceparent": traceparent}
    tracestate = os.environ.get(TRACESTATE_ENV)
    if tracestate:
        carrier["tracestate"] = tracestate
    return _propagator.extract(carrier)
