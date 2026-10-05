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

In `interloper.yaml` a section is a mapping under its key; in the environment every field is the section's prefix followed by the field name in upper case (`INTERLOPER_RUNNER_TYPE`). The framework reads `runner`, `otel`, `catalog` and `secrets`; the other sections configure the platform packages. Every section and field is listed in [Settings](../reference/settings.md).

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

Within a section, a field present in the YAML wins over its environment variable, and the
environment fills the fields the YAML leaves out: `runner: {type: serial}` with
`INTERLOPER_RUNNER_TYPE=async` resolves to `serial`, while `INTERLOPER_POSTGRES_PASSWORD` completes a
`postgres:` block that omits the password. The YAML is not interpolated, so secrets belong in the
environment, not in `${VAR}` placeholders.

## Other environment variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `INTERLOPER_EVENTS_TO_STDERR` | `interloper run`, telemetry setup | `true` marks a child container: events are forwarded as `@EVENT:` lines and the metrics handler is not installed. |
| `INTERLOPER_<PROVIDER>_CLIENT_ID`, `_CLIENT_SECRET`, `_REDIRECT_URI` | OAuth connections and providers | In-house OAuth app credentials per provider. |
| `TRACEPARENT`, `TRACESTATE` | telemetry propagation | Parent trace context for a spawned process. |
| `OTEL_EXPORTER_OTLP_*` | OpenTelemetry SDK | Fallbacks for exporter settings left empty in `otel`. |
