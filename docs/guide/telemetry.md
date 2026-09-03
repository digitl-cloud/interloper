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
    ├── interloper.asset.resolve_resource      per resource slot
    ├── interloper.destination.read            per upstream dependency
    ├── interloper.asset.data                  data()
    ├── interloper.normalizer.normalize        only when a normalizer is configured
    ├── interloper.asset.conform
    │   ├── interloper.asset.infer_schema      AUTO without a declared schema
    │   └── interloper.conformer.reconcile     with a declared schema
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
service name reset to `interloper-run`). httpx client spans are enabled when the httpx
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
`interloper.*` attribute names. The full list of spans, attributes and instruments is in
[Spans and metrics](../reference/telemetry.md).
