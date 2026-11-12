# Configuration

`AppSettings` is the runtime configuration the CLI and the platform read. It loads from three
sources: an `interloper.yaml` in the working directory, environment variables, field defaults. Every section has its own environment prefix.

```py
from interloper.settings import AppSettings

settings = AppSettings.get()          # the active settings, or freshly loaded ones
settings.runner.type
```

`AppSettings.activate(settings)` pins an instance for the current process (the CLI does this);
`clear_active()` releases it.

Every top-level key is a section. In `interloper.yaml` a section is a mapping under its key; in the environment every field is `INTERLOPER_<SECTION>_<FIELD>` in upper case (`INTERLOPER_RUNNER_TYPE`), and a list takes comma-separated values (`INTERLOPER_CATALOG_INCLUDE=my_package.sources.Shop,my_package.sources.Finance`). The framework reads `runner`, `otel`, `log`, `catalog`, `run` and `server`; the other sections configure the platform packages. Every section and field is listed in [Settings](../reference/settings.md).

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
  include:
    - my_package.sources.Shop
    - my_package.sources.Finance
```

Within a section, a field present in the YAML wins over its environment variable, and the
environment fills the fields the YAML leaves out: `runner: {type: serial}` with
`INTERLOPER_RUNNER_TYPE=async` resolves to `serial`, while `INTERLOPER_POSTGRES_PASSWORD` completes a
`postgres:` block that omits the password. The YAML is not interpolated, so secrets belong in the
environment, not in `${VAR}` placeholders.

## Other environment variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `INTERLOPER_RUN_TOKEN` | `interloper launch` | `run.token`, set by a launcher on a run's pod and forwarded by a container runner to its children: the bearer the run is loaded from the API and its events are reported with. |
| `INTERLOPER_RUN_HEARTBEAT_INTERVAL`, `_TIMEOUT` | `interloper launch` | `run.heartbeat_interval` and `run.heartbeat_timeout`, set and forwarded the same way from the scheduler's `reaper` settings: how often the run heartbeats, and the silence after which the reaper fails it. |
| `INTERLOPER_<PROVIDER>_CLIENT_ID`, `_CLIENT_SECRET` | OAuth connections and providers | In-house OAuth app credentials per provider. |
| `INTERLOPER_<PROVIDER>_REDIRECT_URI` | the API's connect flow | Overrides a provider's redirect URI, `<server.external_url>/auth/<provider>` by default. |
| `TRACEPARENT`, `TRACESTATE` | telemetry propagation | Parent trace context for a spawned process. |
| `OTEL_EXPORTER_OTLP_*` | OpenTelemetry SDK | Fallbacks for exporter settings left empty in `otel`. |
