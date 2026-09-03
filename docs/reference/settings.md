# Settings

`AppSettings` is the runtime configuration the CLI and the platform read. It loads from three
sources, highest priority first: environment variables, an `interloper.yaml` in the working
directory, field defaults. Every section has its own environment prefix.

```py
from interloper.settings import AppSettings

settings = AppSettings.get()          # the active settings, or freshly loaded ones
settings.runner.type
```

`AppSettings.activate(settings)` pins an instance for the current process (the CLI does this);
`clear_active()` releases it.

## Sections the framework uses

### `runner`

Prefix `INTERLOPER_RUNNER_`.

| Field | Default | Meaning |
|-------|---------|---------|
| `type` | `"async"` | Registry key of the runner: `async`, `serial`, `multi_process`, or one registered by another package. |
| `config` | `{}` | Keyword arguments for the runner class. |

### `otel`

Prefix `INTERLOPER_OTEL_`. See [Telemetry](../guide/telemetry.md#settings).

| Field | Default |
|-------|---------|
| `enabled` | `false` |
| `endpoint` | `""` |
| `protocol` | `"grpc"` |
| `headers` | `""` |
| `service_name` | `""` (reported as `interloper`) |
| `traces` | `true` |
| `metrics` | `true` |
| `sample_ratio` | `1.0` |
| `metric_export_interval` | `60` |

### `catalog`

`INTERLOPER_CATALOG`, a list of import paths. Empty enables every installed component. See
[Catalog](../guide/catalog.md#building-a-catalog).

### `secrets`

Prefix `INTERLOPER_`.

| Field | Default | Meaning |
|-------|---------|---------|
| `encryption_key` | `""` | Key for encrypting stored resource configuration. Required by the platform to persist resources; unused by the core alone. |

## Sections used by platform packages

These sections live in the core because `AppSettings` does, but the core does not read them.
They configure `interloper-db`, `interloper-api`, `interloper-scheduler`, `interloper-agent`
and `interloper-mcp`.

| Section | Prefix | Fields |
|---------|--------|--------|
| `postgres` | `INTERLOPER_POSTGRES_` | `host`, `port`, `user`, `password`, `database`; `dsn` property |
| `auth` | `INTERLOPER_AUTH_` | `google_client_id`, `google_client_secret`, `google_redirect_uri`, `cookie_secure`, `session_expiry_days`, `super_admin_emails`, `allowed_domains` |
| `server` | `INTERLOPER_SERVER_` | `enabled`, `host`, `port` |
| `cron` | `INTERLOPER_CRON_` | `enabled`, `reconcile_interval`, `max_execution_delay`, `batch_size` |
| `renewal` | `INTERLOPER_RENEWAL_` | `enabled`, `reconcile_interval`, `batch_size` |
| `worker` | `INTERLOPER_WORKER_` | `enabled`, `poll_interval` |
| `reaper` | `INTERLOPER_REAPER_` | `enabled`, `timeout`, `poll_interval` |
| `launcher` | `INTERLOPER_LAUNCHER_` | `type`, `config` |
| `smtp` | `INTERLOPER_SMTP_` | `host`, `port`, `user`, `password`, `from_addr` |
| `agent` | `INTERLOPER_AGENT_` | `enabled`, `model` |
| `mcp` | `INTERLOPER_MCP_` | `host`, `port`, `external_url`, `token`, `org_id` |
| `quota` | `INTERLOPER_QUOTA_` | `max_sources`, `max_assets_per_source`, `max_successful_runs_per_month`, `max_backfill_partitions` |

## YAML example

```yaml
# interloper.yaml
runner:
  type: async
  config:
    max_workers: 8

otel:
  enabled: true
  endpoint: http://localhost:4317

catalog:
  - my_package.sources.Shop
  - my_package.sources.Finance
```

Note that a YAML block wins over environment variables for that whole submodel only when the
block is absent: pydantic-settings merges by section, so setting `runner:` in YAML and
`INTERLOPER_RUNNER_TYPE` in the environment resolves in favour of the environment.

## Other environment variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `INTERLOPER_EVENTS_TO_STDERR` | `interloper run`, telemetry setup | `true` marks a child container: events are forwarded as `@EVENT:` lines and the metrics handler is not installed. |
| `INTERLOPER_<PROVIDER>_CLIENT_ID`, `_CLIENT_SECRET`, `_REDIRECT_URI` | OAuth connections and providers | In-house OAuth app credentials per provider. |
| `TRACEPARENT`, `TRACESTATE` | telemetry propagation | Parent trace context for a spawned process. |
| `OTEL_EXPORTER_OTLP_*` | OpenTelemetry SDK | Fallbacks for exporter settings left empty in `otel`. |
