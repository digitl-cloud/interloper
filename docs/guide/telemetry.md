# Telemetry

Interloper exports OpenTelemetry **traces** and **metrics** over OTLP: one trace per run, from
the runner through every operation, `data()` call and destination read or write, across
process boundaries; plus counters and duration histograms derived from the event bus.

Telemetry is off by default and costs nothing when disabled: the core depends only on the no-op
`opentelemetry-api`. The SDK and exporters ship in the `otel` extra.

## Enabling

```sh
pip install 'interloper-core[otel]'

export INTERLOPER_OTEL_ENABLED=true
export INTERLOPER_OTEL_ENDPOINT=http://localhost:4317
interloper run my_package.sources.Shop
```

The CLI initializes telemetry for every command. Library code (a script, a notebook) does it
explicitly:

```py
import interloper as il
from interloper.settings import TelemetrySettings
from interloper.telemetry import init_telemetry, shutdown_telemetry

init_telemetry(TelemetrySettings(enabled=True, endpoint="http://localhost:4317"))
il.run(il.AsyncRunner().run(dag))
shutdown_telemetry()          # flush before exit
```

`init_telemetry` is idempotent and a no-op when disabled. When enabled without the extra
installed it logs a warning and leaves the no-op providers in place; telemetry never takes the
data plane down. `force_flush()` flushes without shutting down, for reused worker processes.

## Settings

The `otel` block of `interloper.yaml`, or `INTERLOPER_OTEL_*` variables:

| Setting | Default | Meaning |
|---------|---------|---------|
| `enabled` | `false` | Master switch. The standard `OTEL_*` variables never activate the SDK on their own. |
| `endpoint` | empty | OTLP endpoint. Empty falls through to `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `protocol` | `grpc` | `grpc` or `http/protobuf`. |
| `headers` | empty | Exporter headers as `key=value,key2=value2`. Treat as a secret. |
| `service_name` | `interloper` | The `service.name` resource attribute. |
| `traces`, `metrics` | `true` | Signal toggles. |
| `sample_ratio` | `1.0` | Parent-based head sampling ratio. |
| `metric_export_interval` | `60` | Seconds between metric exports. |

Interloper settings win over the SDK's `OTEL_*` variables; anything left empty can still be
supplied through them.

## Traces

Spans are named `interloper.<class>.<method>` after the call they wrap:

```
interloper.runner.run                          Runner.run
└── interloper.operation.execute               Operation.execute
    ├── interloper.asset.resolve_resource      per resource relation
    ├── interloper.destination.read            per bound upstream
    ├── interloper.asset.data                  data()
    ├── interloper.normalizer.normalize        only when a normalizer is configured
    ├── interloper.asset.conform
    │   ├── interloper.asset.infer_schema      AUTO without a declared schema
    │   └── interloper.representation.reconcile  with a declared schema
    └── interloper.destination.write           per destination
```

`interloper.dag.materialize` wraps `dag.materialize()`, and `interloper.dag_spec.reconstruct`
measures the deserialization paid by process and container workers. Resource resolution is
lookup and instantiation only; a client built lazily on a connection costs under
`interloper.asset.data`.

Spans carry `interloper.*` attributes: run and backfill ids, component id, kind, key and
qualified key, source id, partition, destination key, upstream key, resource name, runner type.
A failed operation sets its span status to error, and so does a run that swallowed failures
into a failed result.

Trace context propagates automatically: the run span's context rides `metadata["traceparent"]`
into every event and into `MultiProcessRunner` workers, and `TRACEPARENT` / `TRACESTATE`
environment variables carry it into spawned processes. `child_process_env()` builds the
environment a child needs (trace context plus the `INTERLOPER_OTEL_*` configuration, with the
service name reset to `interloper-run`). httpx2 client spans are enabled when the httpx2
instrumentation is installed, so REST-based sources get egress spans for free.

## Metrics

| Instrument | Type | Attributes |
|------------|------|------------|
| `interloper.runs` | counter | `status`, plus `org_id`, `target_kind`, `target_key` when the platform supplies them |
| `interloper.run.duration` | histogram, seconds | same |
| `interloper.operations` | counter | `status`, `component_key` |
| `interloper.operation.duration` | histogram, seconds | `status`, `component_key` |
| `interloper.destination.io` | counter | `operation` (`read`, `write`), `status`, `destination_key` |

Metrics are computed by a subscriber on the [event bus](events.md), so they cost nothing on the
execution path, and they dedupe on event id so re-emitted child events count once. Attributes
stay low-cardinality by design: ids and partitions never become metric attributes. In a child
container (`INTERLOPER_EVENTS_TO_STDERR=true`) the metrics handler is not installed; the host
that re-emits the events is authoritative.

### Delta temporality

Counters and histograms are exported as **deltas**, not running totals. Runs are short-lived
processes: a cumulative point from a process that exports once has no earlier point to be
differenced against, and the next process restarts at zero. A delta point is self-contained. The
collector must therefore accumulate: an OpenTelemetry Collector with the `deltatocumulative`
processor in front of a Prometheus exporter that is scraped, with OpenMetrics enabled so
counter start timestamps survive, and `metric_expiration` raised well above the default so idle
counters keep publishing. The accumulated totals live in the collector; restarting it resets
them. A worked collector and Grafana setup is in the repository under `examples/telemetry`.

## Custom instrumentation

`tracer()` and `meter()` from `interloper.telemetry` return the framework's tracer and meter,
no-op until the SDK is initialized:

```py
from interloper.telemetry import tracer

with tracer().start_as_current_span("shop.fetch_report", attributes={"shop.account": account_id}):
    ...
```

`interloper.telemetry.attributes.from_metadata(metadata)` maps event-style metadata onto the
`interloper.*` attribute names. The spans, attributes and instruments are listed below.

## Spans

| Span | Wraps | Notes |
|------|-------|-------|
| `interloper.runner.run` | `Runner.run()` | Root of a run. Attributes: run metadata, `interloper.runner.type`, `interloper.partition`. Status error when the result is failed. |
| `interloper.operation.execute` | `Operation.execute()` | One per operation. |
| `interloper.asset.resolve_resource` | resource lookup | One per resource relation; adds `interloper.resource.name`. |
| `interloper.destination.read` | `Destination.read()` | One per upstream dependency; adds `interloper.destination.key`, `interloper.upstream.key`. |
| `interloper.asset.data` | `data()` | |
| `interloper.normalizer.normalize` | `Normalizer.normalize()` | Only when a normalizer is configured. |
| `interloper.asset.conform` | the conform step | |
| `interloper.asset.infer_schema` | schema inference | Only under `AUTO` without a declared schema. |
| `interloper.representation.reconcile` | `Representation.reconcile()` | Only with a declared schema under `AUTO` or `RECONCILE`. |
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
the stable semantic conventions. Contrib instrumentors for httpx2 and SQLAlchemy are activated
when installed.
