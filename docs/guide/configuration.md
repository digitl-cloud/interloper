# Configuration

The CLI and the platform read their configuration as **settings sections**, each declared by the
package whose behaviour it configures: core declares `runner`, `otel`, `log`, `catalog`, `run`
and `server`, the database package `postgres`, `encryption` and `quota`, the API `auth` and `smtp`,
the scheduler `cron`, `renewal`, `worker`, `reaper` and `launcher`, the agent `agent` and the MCP
server `mcp`. Every section and field is listed in [Settings](../reference/settings.md).

A section loads from three sources, highest first: environment variables, its block of
`interloper.yaml` in the working directory, field defaults. A section class reads itself:

```py
from interloper.settings import RunnerSettings

RunnerSettings.get().type   # the section this process runs with, loaded on the first read
```

A section is loaded once per process and kept. `RunnerSettings.activate(settings)` makes another
instance the one every later `get()` returns (a command does this to apply its flags), and
`Settings.clear_active()` forgets every section, so the next read loads it again.

In `interloper.yaml` a section is a mapping under its name; in the environment every field is
`INTERLOPER_<SECTION>_<FIELD>` in upper case (`INTERLOPER_RUNNER_TYPE`), and a list takes
comma-separated values (`INTERLOPER_CATALOG_INCLUDE=my_package.sources.Shop,my_package.sources.Finance`).

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

The environment beats the file for every field, and the file fills the fields the environment
leaves out: `runner: {type: serial}` with `INTERLOPER_RUNNER_TYPE=async` resolves to `async`, while
`INTERLOPER_POSTGRES_PASSWORD` completes a `postgres:` block that omits the password. The YAML is
not interpolated, so secrets belong in the environment, not in `${VAR}` placeholders.

One file serves every role, so a block no installed package declares is ignored: the API's
process reads no `launcher` block when the scheduler is not installed beside it. An unknown field
inside a declared block is still an error wherever the package declaring it is installed.

## Other environment variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `INTERLOPER_RUN_TOKEN` | `interloper launch` | `run.token`, set by a launcher on a run's pod and forwarded by a container runner to its children: the bearer the run is loaded from the API and its events are reported with. |
| `INTERLOPER_RUN_HEARTBEAT_INTERVAL`, `_TIMEOUT` | `interloper launch` | `run.heartbeat_interval` and `run.heartbeat_timeout`, set and forwarded the same way from the scheduler's `reaper` settings: how often the run heartbeats, and the silence after which the reaper fails it. |
| `INTERLOPER_<PROVIDER>_CLIENT_ID`, `_CLIENT_SECRET` | OAuth connections and providers | In-house OAuth app credentials per provider. |
| `INTERLOPER_<PROVIDER>_REDIRECT_URI` | the API's connect flow | Overrides a provider's redirect URI, `<server.external_url>/auth/<provider>` by default. |
| `TRACEPARENT`, `TRACESTATE` | telemetry propagation | Parent trace context for a spawned process. |
| `OTEL_EXPORTER_OTLP_*` | OpenTelemetry SDK | Fallbacks for exporter settings left empty in `otel`. |
