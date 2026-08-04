# Telemetry example

A self-contained local stack — OpenTelemetry Collector, Tempo, Prometheus and a
pre-provisioned Grafana — plus a demo pipeline whose traces and metrics land in
it. Nothing to configure: bring the stack up, run the pipeline, open Grafana.

## Run it

```bash
docker compose -f examples/telemetry/docker-compose.yml up -d
```

```bash
uv run python examples/telemetry/pipeline.py
```

The pipeline loops until you press Ctrl-C, so the dashboard fills in while you
watch. Then open **<http://localhost:8080>** (no login) → *Dashboards* →
*Interloper — Runs & Assets*.

Tear down with:

```bash
docker compose -f examples/telemetry/docker-compose.yml down -v
```

## What you're looking at

The pipeline is one source, `shop`, with five assets shaped so that a single run
exercises the whole span catalogue rather than the same two spans five times:

| Asset | What it demonstrates |
|---|---|
| `customers` | Declared schema → `interloper.conformer.reconcile` |
| `orders` | No schema under AUTO → `interloper.asset.infer_schema` |
| `refunds` | Normalizer configured → `interloper.normalizer.normalize` |
| `fx_rates` | Declares a resource → `interloper.asset.resolve_resource`; fails ~25% of runs → error spans and `status="failed"` metrics |
| `report` | Reads three upstreams → three `interloper.destination.read` spans |

One successful run produces 34 spans covering every span the framework emits
except `interloper.dag_spec.reconstruct`, which only appears with the
multiprocess or container runners.

The dashboard has three rows: **Runs** (throughput, success rate, duration
percentiles), **Assets** (per-asset outcomes and p95 duration, plus a table of
which assets fail), and **Traces** (a live list of `interloper.runner.run`
spans — click one to open its span tree).

Because `fx_rates` fails intermittently and the runner is fail-fast by default,
you will see runs that fail with downstream assets `canceled`. That is the
interesting case: open one of those traces and the failed span is marked in red
with the exception recorded on it.

## How the pieces fit

```
pipeline.py ──OTLP/gRPC :4317──▶ otel-collector ──▶ Tempo       (traces)
                                                └──▶ Prometheus (metrics, remote write)
                                                          ▲
                                                       Grafana :8080
```

Ports deliberately avoid 3000/3001 so this can run alongside an interloper dev
instance. Prometheus is on :9090 if you want to poke at raw series; Tempo is
reachable only from inside the compose network.

Telemetry is configured the way an embedding application would do it — via
`TelemetrySettings` and `init_telemetry()` — not by assembling the
OpenTelemetry SDK by hand:

```py
init_telemetry(
    TelemetrySettings(enabled=True, endpoint="http://localhost:4317", service_name="interloper-example"),
    role="example",
)
```

Equivalently, without touching code:
`INTERLOPER_OTEL_ENABLED=true INTERLOPER_OTEL_ENDPOINT=http://localhost:4317`.

## Notes

- **The metric export interval is forced to 5s** (`OTEL_METRIC_EXPORT_INTERVAL`).
  The SDK default is 60s, which would give a short demo a single data point —
  and the `rate()`-based timeseries panels need at least two to plot anything.
- **The total/rate panels use `max_over_time`, not `increase`.** A counter from a
  short-lived process is already non-zero in its very first exported sample:
  OpenTelemetry only exports an instrument once it has recorded something, so
  Prometheus never sees the 0 → 1 rise. `increase()` measures last − first within
  the window, so it under-reports by the first increment — and reads exactly **0**
  when every run completed before the first export (which is what happens at the
  SDK's 60s default). `max_over_time` reads the counter's peak instead, which is
  both correct and an integer. The trade-off: restarting the pipeline resets the
  counter, so these panels show the largest single session rather than a sum
  across sessions.
- **The per-interval timeseries panels still use `increase()`**, which is the right
  shape for "activity over time" — they rely on the 5s export interval above, and
  will look sparse if you point a 60s-export process at this dashboard.
- **Panels go blank ~5 minutes after you stop the pipeline.** That is Prometheus'
  lookback window, not a broken dashboard: a short-lived process stops producing
  samples the moment it exits. The `max_over_time` panels keep working as long as
  the dashboard time range still covers the run.
- The metric names are the OTLP names with dots replaced by underscores, e.g.
  `interloper.asset.duration` → `interloper_asset_duration_seconds`. `service.name`
  arrives as the `job` label.
- **Image versions are pinned.** Tempo 3.x restructured its config file and its
  search behaviour, so `:latest` silently broke the trace list; the whole stack
  is pinned to versions this example was verified against.
- The demo writes assets to `/tmp/interloper-telemetry` (`--data-dir` to change).

See [docs/features/telemetry.md](../../docs/features/telemetry.md) for the full
span and metric catalogue.
