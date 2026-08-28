"""OpenTelemetry setup, context propagation, and tracer/meter access."""

from interloper.telemetry import attributes
from interloper.telemetry.propagation import (
    child_process_env,
    context_from_env,
    extract_metadata,
    inject_metadata,
    traceparent_env,
)
from interloper.telemetry.setup import force_flush, init_telemetry, instrument_fastapi, shutdown_telemetry
from interloper.telemetry.tracer import meter, tracer
