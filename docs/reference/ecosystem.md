# Ecosystem

`interloper-core` is the framework. The packages below build on it; each is documented in its
own README. All are versioned and released together.

## Extras of the core

| Extra | Adds |
|-------|------|
| `interloper-core[otel]` | The OpenTelemetry SDK, OTLP exporters (gRPC and HTTP) and httpx instrumentation. |
| `interloper-core[google-cloud]` | Pulls in `interloper-google-cloud`. |
| `interloper-core[slack]` | Pulls in `interloper-slack`. |

## Framework extensions

| Package | Provides | Registers |
|---------|----------|-----------|
| `interloper-pandas` | `DataFrameRepresentation`, `DataFrameConformer`, `DataFrameNormalizer`: assets may return pandas DataFrames, normalized and conformed natively. | `interloper.representations`: `dataframe` |
| `interloper-google-cloud` | `GoogleCloudConnection`, `BigQueryDestination` (a `DatabaseDestination` with typed Parquet loads and time partitioning), `GCSDestination` (hive-partitioned Parquet, JSONL or CSV). | `interloper.components` |
| `interloper-slack` | `SlackConnection`, `SlackHook`: a notification hook posting run outcomes to a channel. | `interloper.components` |
| `interloper-assets` | Ready-made sources and connections for advertising, analytics and commerce platforms (Facebook Ads, Google Ads, Bing Ads, Amazon Ads, LinkedIn, TikTok, Pinterest, Snapchat, Criteo, Search Console, and more), plus the `demo` source. | `interloper.components` |
| `interloper-docker` | `DockerRunner`: each operation runs in a container, events stream back to the host. | `interloper.runners`: `docker`; `interloper.launchers`: `docker` |
| `interloper-k8s` | `KubernetesRunner`: each operation runs as a Kubernetes Job. | `interloper.runners`: `kubernetes`; `interloper.launchers`: `kubernetes` |

## The platform

| Package | Role |
|---------|------|
| `interloper-db` | Persistence: components, relations, runs, events, organisations. Migrations and the store the CLI's `db` commands operate. |
| `interloper-scheduler` | Cron controller, hook evaluator, credential renewal, queue worker, reaper and launchers. Acts on the jobs, hooks and connections the core declares. |
| `interloper-api` | FastAPI backend reading catalog metadata and persisted state. |
| `interloper-app` | The web UI (Nuxt SPA), served by the API image or standalone. |
| `interloper-mcp` | An MCP server exposing the catalog, lineage and run history to agents. |
| `interloper-agent` | An AI agent (Google ADK) over the same data. |
| `interloper-toolkit` | Shared tooling for the agent and MCP server. |

Deployment artifacts (Docker images, a Helm chart) are described in the repository README.
