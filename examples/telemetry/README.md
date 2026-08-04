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
                    traces ────────────────▶ Tempo
pipeline.py ──OTLP :4317──▶ otel-collector                     Grafana :8080
                    metrics ── accumulated ──┐                      │
                                             ▼                      │
                          Prometheus scrapes :9464 ◀────────────────┘
```

Metrics are *pulled* from the collector rather than pushed onward: the
collector holds the accumulated totals and outlives every run, so scraping it
avoids the short-lived-producer problem entirely. Traces are pushed straight
through to Tempo.

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

- **Metrics are deltas, accumulated by the collector, and Prometheus scrapes it.**
  Runs report "N happened since my last export" rather than a running total;
  `deltatocumulative` accumulates those into one continuous series the collector
  owns; and Prometheus scrapes that with `created-timestamp-zero-ingestion`, so
  a counter first observed at 1 is recorded as having risen from 0. All three
  pieces are load-bearing — drop any one and the first run of every series
  disappears from the counts. See
  [docs/features/telemetry.md](../../docs/features/telemetry.md).
- **Counts are exact; rate panels are approximate.** The stat and table panels
  use a counter delta over the range, so they read exactly 1 after one run. The
  bar charts use `increase()`, which extrapolates by design — treat their
  heights as activity, not as counts.
- **The demo exports every 5s** (`metric_export_interval=5`, versus the
  framework's 60s default). Purely for responsiveness: the deltas are correct
  either way, they just land sooner.
- **Panels go blank ~5 minutes after you stop the pipeline.** That is Prometheus'
  lookback window, not a broken dashboard. Range-based panels keep working as
  long as the dashboard time range still covers the runs.
- **Restarting the collector resets the accumulated totals**, since that is where
  the running state lives.
- The metric names are the OTLP names with dots replaced by underscores, e.g.
  `interloper.asset.duration` → `interloper_asset_duration_seconds`. `service.name`
  arrives as the `job` label.
- **Image versions are pinned.** Tempo 3.x restructured its config file and its
  search behaviour, so `:latest` silently broke the trace list; the whole stack
  is pinned to versions this example was verified against.
- The demo writes assets to `/tmp/interloper-telemetry` (`--data-dir` to change).

See [docs/features/telemetry.md](../../docs/features/telemetry.md) for the full
span and metric catalogue.
