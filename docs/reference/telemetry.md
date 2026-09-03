# Spans & metrics

## Spans

| Span | Wraps | Notes |
|------|-------|-------|
| `interloper.runner.run` | `Runner.run()` | Root of a run. Attributes: run metadata, `interloper.runner.type`, `interloper.partition`. Status error when the result is failed. |
| `interloper.operation.execute` | `Operation.execute()` | One per operation. |
| `interloper.asset.resolve_resource` | resource lookup | One per resource slot; adds `interloper.resource.name`. |
| `interloper.destination.read` | `Destination.read()` | One per upstream dependency; adds `interloper.destination.key`, `interloper.upstream.key`. |
| `interloper.asset.data` | `data()` | |
| `interloper.normalizer.normalize` | `Normalizer.normalize()` | Only when a normalizer is configured. |
| `interloper.asset.conform` | the conform step | |
| `interloper.asset.infer_schema` | schema inference | Only under `AUTO` without a declared schema. |
| `interloper.conformer.reconcile` | `Conformer.reconcile()` | Only with a declared schema under `AUTO` or `RECONCILE`. |
| `interloper.destination.write` | `Destination.write()` | One per destination; adds `interloper.destination.key`. |
| `interloper.dag.materialize` | `DAG.materialize_async()` | Root when a DAG is driven directly. Attribute `interloper.dag.operation_count`. |
| `interloper.dag_spec.reconstruct` | `DAGSpec.reconstruct()` | Attribute `interloper.dag.spec_items`. |

## Attributes

| Attribute | Source metadata key |
|-----------|---------------------|
| `interloper.run.id` | `run_id` |
| `interloper.backfill.id` | `backfill_id` |
| `interloper.component.id` | `component_id` |
| `interloper.component.kind` | `component_kind` |
| `interloper.component.key` | `component_key` |
| `interloper.component.qualified_key` | `qualified_key` |
| `interloper.source.id` | `source_id` |
| `interloper.partition` | `partition_or_window` |
| `interloper.destination.key` | `destination_key` |
| `interloper.upstream.key` | set directly on read spans |
| `interloper.resource.name` | set directly on resolve spans |
| `interloper.runner.type` | set directly on the run span |
| `interloper.dag.operation_count`, `interloper.dag.spec_items` | set directly on DAG spans |
| `interloper.org.id`, `interloper.target.id`, `interloper.target.kind`, `interloper.target.key`, `interloper.target.name` | `org_id`, `target_*` when the platform supplies them |
| `interloper.launcher.type` | set by platform launchers |

`interloper.telemetry.attributes.from_metadata(metadata)` performs the mapping, dropping `None`
values and stringifying the rest.

## Metrics

All instruments are recorded by `OtelMetricsHandler`, an event-bus subscriber, and deduped on
event id.

| Instrument | Kind | Unit | Attributes |
|------------|------|------|------------|
| `interloper.runs` | counter | `{run}` | `status` (`completed`, `failed`); `org_id`, `target_kind`, `target_key` when present |
| `interloper.run.duration` | histogram | `s` | same |
| `interloper.operations` | counter | `{execution}` | `status` (`completed`, `failed`, `canceled`), `component_key` |
| `interloper.operation.duration` | histogram | `s` | same |
| `interloper.destination.io` | counter | `{operation}` | `operation` (`read`, `write`), `status` (`completed`, `failed`), `destination_key` |

Durations are measured between the start and terminal events' timestamps. Counters and
histograms export with delta temporality; up-down counters and gauges stay cumulative.

## Propagation helpers

| Function | Purpose |
|----------|---------|
| `inject_metadata(metadata)` | Write the current span context into a metadata dict (`traceparent`). |
| `extract_metadata(metadata)` | Read a parent context back from it. |
| `traceparent_env()` | `{"TRACEPARENT": ..., "TRACESTATE": ...}` for the current span. |
| `child_process_env()` | `INTERLOPER_OTEL_*` plus the trace context, with `INTERLOPER_OTEL_SERVICE_NAME=interloper-run`. |
| `context_from_env()` | A parent context from `TRACEPARENT` / `TRACESTATE`. |

## Setup functions

| Function | Purpose |
|----------|---------|
| `init_telemetry(settings)` | Install providers and exporters. Idempotent; returns whether the SDK is active. |
| `shutdown_telemetry()` | Flush and shut the providers down. |
| `force_flush()` | Flush without shutting down. |
| `instrument_fastapi(app)` | Add request spans to a FastAPI app when telemetry is active. |
| `tracer()`, `meter()` | The framework's cached tracer and meter. |

`init_telemetry` sets `OTEL_SEMCONV_STABILITY_OPT_IN=http` unless already set, so HTTP spans use
the stable semantic conventions. Contrib instrumentors for httpx and SQLAlchemy are activated
when installed.
