# Telemetry

Interloper exports OpenTelemetry **traces** and **metrics** over OTLP: one trace per run —
from the scheduler's dispatch through every asset, `data()` call, and destination read/write,
across process and container boundaries — plus counters and duration histograms for runs,
assets, and destination I/O.

Telemetry is **off by default** and adds no overhead when disabled: the framework only
depends on the no-op `opentelemetry-api`; the SDK and exporters ship in the optional
`otel` extra.

## Quickstart

Install the extra and point the exporter at an OTLP endpoint (any collector, Grafana
Tempo/Mimir, Jaeger, etc.):

```bash
pip install 'interloper[otel]'

export INTERLOPER_OTEL_ENABLED=true
export INTERLOPER_OTEL_ENDPOINT=http://localhost:4317

interloper run my_pipeline.py
```

For a local look, run a collector that prints what it receives:

```bash
docker run --rm -p 4317:4317 otel/opentelemetry-collector
```

## Configuration

Settings live under the `otel` block of `interloper.yaml` or the matching
`INTERLOPER_OTEL_*` environment variables:

| Setting | Env var | Default | Description |
|---------|---------|---------|-------------|
| `enabled` | `INTERLOPER_OTEL_ENABLED` | `false` | Master switch. Telemetry is never activated from `OTEL_*` env vars alone. |
| `endpoint` | `INTERLOPER_OTEL_ENDPOINT` | — | OTLP endpoint. Empty falls through to the SDK's own `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `protocol` | `INTERLOPER_OTEL_PROTOCOL` | `grpc` | `grpc` or `http/protobuf`. |
| `headers` | `INTERLOPER_OTEL_HEADERS` | — | Exporter auth headers, `key=value,key2=value2`. Treat as a secret. |
| `service_name` | `INTERLOPER_OTEL_SERVICE_NAME` | `interloper` | Set per deployment where distinct names matter; see below. |
| `traces` | `INTERLOPER_OTEL_TRACES` | `true` | Toggle the traces signal. |
| `metrics` | `INTERLOPER_OTEL_METRICS` | `true` | Toggle the metrics signal. |
| `sample_ratio` | `INTERLOPER_OTEL_SAMPLE_RATIO` | `1.0` | Head sampling ratio (parent-based). |
| `metric_export_interval` | `INTERLOPER_OTEL_METRIC_EXPORT_INTERVAL` | `60` | Seconds between metric exports. A freshness knob: runs flush on exit regardless, and an idle interval exports nothing. |

### Service names

`service.name` is deployment configuration, not something the framework
infers: every process defaults to `interloper`, and the deployment that wants
a distinct name sets `service_name` where that deployment is defined. The
Helm chart does this for its own workloads — the api pod reports as
`interloper-api` and the scheduler pod as `interloper-scheduler` — and the
launchers name the run pods and per-asset containers they spawn
`interloper-run` (identity is per-process, so it deliberately isn't inherited
from the launching process's `INTERLOPER_OTEL_SERVICE_NAME`; the rest of the
`INTERLOPER_OTEL_*` config is forwarded).

Two consequences worth knowing: runs executed by the in-process launcher
report under the host process's name, since `service.name` is fixed per
process; and standalone scripts or notebooks report as plain `interloper`
unless they pass ``service_name`` themselves.

### Delta temporality — required collector setup

Metrics are exported with **delta temporality**: each point reports what
happened since the previous export ("6 reads in the last interval") rather
than a running total ("my counter is at 6"). This is not the OpenTelemetry
default, and it is deliberate.

A cumulative point only means something next to an earlier one — the
information lives in the difference between points. A run that lives a few
seconds exports its counter once, with nothing before it to be differenced
against, so the work it did is invisible to `rate()`/`increase()`; and the
next run starts a fresh process whose counter begins at zero again, so the
series never rises. A delta point is self-contained, so neither problem
arises.

**This requires a collector**, configured as an aggregation gateway that
Prometheus *scrapes* — not one that pushes. Three pieces have to line up, and
each is load-bearing:

```yaml
# collector
processors:
  deltatocumulative:
    max_stale: 25h          # (1) accumulate deltas; hold state ≥ expiration

exporters:
  prometheus:
    endpoint: 0.0.0.0:9464
    enable_open_metrics: true   # (2) publish counter created timestamps
    metric_expiration: 24h      # (4) keep publishing idle counters
    resource_to_telemetry_conversion: { enabled: true }

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [deltatocumulative, batch]
      exporters: [prometheus]
```

```bash
# prometheus
--enable-feature=created-timestamp-zero-ingestion   # (3)
```

1. **`deltatocumulative`** accumulates the deltas into one continuous series
   per stream, owned by the collector rather than by any run.
2. **Scraping, with OpenMetrics.** The collector is long-lived and always
   holds the current totals, so scraping it has none of the ephemeral-producer
   problems. OpenMetrics carries each counter's *created timestamp*; the
   remote-write v1 path drops it.
3. **`created-timestamp-zero-ingestion`** makes Prometheus act on that
   timestamp, writing a zero sample at the counter's start. Without it a
   counter whose first observed value is 1 has no rise, and the first run of
   every stream stays invisible to `increase()` — verified: the flag is the
   difference between the series minimum reading `0` and reading `1`.
4. **`metric_expiration`** raised from its 5-minute default. The exporter
   stops publishing a counter that has not been updated recently, so with the
   default, totals vanish from any pipeline idle for five minutes: the series
   goes stale and every instant query reads zero even though the runs
   happened. `deltatocumulative`'s `max_stale` must be at least as long, or
   the running total is discarded while the series is still published and the
   counter silently restarts from zero.

The accumulation state lives in the collector, so restarting it resets the
running totals.

### Counting vs rating

Both question shapes are legitimate, and they need different queries:

- **"How many runs happened in this window?"** — an exact counter delta:
  `(sum(interloper_runs_total) or vector(0)) - (sum(interloper_runs_total offset $__range) or vector(0))`.
  Integers, no extrapolation.
- **"What did activity look like over time?"** — the same delta per bucket,
  with the panel's minimum step pinned to the bucket width so the buckets are
  disjoint.

`increase()` suits neither: it extrapolates to the window edges, so ten runs
render as 10.5, and `$__rate_interval` deliberately makes the window wider
than the plotting step — which draws one burst of work as several adjacent
bars that must not be summed. That combination is the usual cause of counts
that look too high.

Because the interval is always passed explicitly, the SDK's own
`OTEL_METRIC_EXPORT_INTERVAL` is inert.

Precedence: interloper settings win over the SDK's standard `OTEL_*` environment
variables; anything you leave unset here (endpoint, headers, resource attributes, …)
can still be supplied through them. Enabling always requires
`INTERLOPER_OTEL_ENABLED=true` — if the `otel` extra is missing, a warning is logged
and the framework runs unchanged.

## Traces

Every span is named `interloper.<class>.<method>` after the call it wraps.
The tree for a scheduled run:

```
interloper.launcher.launch                    Launcher.launch
└── interloper.runner.run                     Runner.run
    └── interloper.asset.materialize          Asset.materialize_async
        ├── interloper.asset.resolve_resource Asset._resolve_resource (per resource)
        ├── interloper.destination.read       Destination.read (per upstream)
        ├── interloper.asset.data             Asset.data
        ├── interloper.normalizer.normalize   Normalizer.normalize (only when configured)
        └── interloper.asset.conform          Asset._conform
            ├── interloper.asset.infer_schema Asset._infer_schema (AUTO, no declared schema)
            └── interloper.conformer.reconcile  Conformer.reconcile (declared schema)
        └── interloper.destination.write      Destination.write (per destination)
```

Two more spans sit outside the scheduled path: `interloper.dag.materialize`
(`DAG.materialize_async`, the entrypoint when you drive a DAG directly) and
`interloper.dag_spec.reconstruct` (`DAGSpec.reconstruct`, the deserialization
cost paid by multiprocess and per-asset container workers).

Several spans are conditional, and their absence is itself information:
`interloper.normalizer.normalize` only when a normalizer is configured,
`interloper.asset.infer_schema` only when a schema is inferred rather than
declared, and `interloper.conformer.reconcile` only the other way around.
Note that resource *resolution* is lookup and instantiation only — the
credentialed client a resource wraps is built lazily, so its cost lands under
`interloper.asset.data`.

Spans carry `interloper.*` attributes: `run.id`, `backfill.id`, `asset.key`,
`asset.qualified_key`, `source.id`, `partition`, `destination.key`, `runner.type`.
Failures set span status to error; runs that swallow asset failures into a failed
`RunResult` are marked too.

Trace context crosses every execution boundary automatically:

- **scheduler → run**: the in-process launcher hands its context to the run thread; the
  Docker/Kubernetes launchers inject `TRACEPARENT` (and forward `INTERLOPER_OTEL_*`)
  into the launched container.
- **run → per-asset container**: the Docker/Kubernetes runners do the same for each
  asset container, whose spans parent under the host's run span.
- **run metadata**: the run span's context also rides `metadata["traceparent"]` into
  every event and into `MultiProcessRunner` workers.

When the API is served (`interloper app`), FastAPI request spans plus SQLAlchemy and
httpx client spans are enabled through the standard contrib instrumentors — REST-based
sources get egress spans for free.

## Metrics

| Instrument | Type | Attributes |
|------------|------|------------|
| `interloper.runs` | counter | `status` + identity† |
| `interloper.run.duration` | histogram (s) | `status` + identity† |
| `interloper.assets` | counter | `status`, `asset_key` |
| `interloper.asset.duration` | histogram (s) | `status`, `asset_key` |
| `interloper.destination.io` | counter | `operation`, `status`, `destination_key` |
| `interloper.runs.launched` | counter | `outcome` |

† **Platform identity** — `org_id`, `target_kind`, `target_key` — appears on
run-level instruments when the run goes through the scheduler, which threads
the run's organisation and target (job, source, or asset) into run metadata;
standalone runs simply omit the attributes. The same identity lands on every
span as `interloper.org.id` / `interloper.target.*`. It is deliberately kept
off asset-level instruments: `org × asset_key` is the cardinality product that
grows fastest with adoption, and per-org asset questions belong to traces
(which carry full identity) or the run history.

The duration histograms use second-scaled bucket boundaries (50ms → 1h) rather
than the SDK's defaults, which start at 0, 5, 10, 25 — tuned for milliseconds.
Against second-valued durations those defaults put every run under five seconds
in a single bucket, so quantiles get interpolated across it and report values
that look plausible but track nothing.

Metrics are derived from the [event bus](events.md), so they cost nothing on the
execution hot path. Attributes are deliberately low-cardinality: ids and partitions
never become metric attributes. In Docker/Kubernetes runs the host process is
authoritative — child containers export traces but not metrics, and re-emitted events
are deduplicated by id.

## Library and notebook use

Embedding interloper (scripts, notebooks) never auto-initializes telemetry — that only
happens through the CLI. Initialize it yourself:

```py
import interloper as il
from interloper.settings import TelemetrySettings
from interloper.telemetry import init_telemetry, shutdown_telemetry

init_telemetry(TelemetrySettings(enabled=True, endpoint="http://localhost:4317"))

il.run(il.AsyncRunner().run(dag))

shutdown_telemetry()  # flush before the process exits
```

## Kubernetes deployment

The Helm chart wires everything from one block. The published images ship
with the `otel` extras of the packages they carry (`COMMON_EXTRAS=otel` at
build time — SDK and exporters everywhere, SQLAlchemy spans where
`interloper-db` is present, request spans in the api image); pass
`COMMON_EXTRAS=""` when building images to leave them out.

```yaml
otel:
  enabled: true
  endpoint: http://otel-collector.observability:4317
```

The API and scheduler pods get the settings directly; run pods and per-asset pods
receive them (plus the live trace context) from the launcher — no extra chart config.
Exporter auth headers are read from the `INTERLOPER_OTEL_HEADERS` key of the release
secret (`secrets.existingSecret`) or per-deployment `extraEnv`; the same `extraEnv`
route serves any finer tuning (`INTERLOPER_OTEL_SAMPLE_RATIO`, service names, signal
toggles).
