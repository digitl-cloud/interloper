<p align="center">
  <img src="docs/assets/logo.svg" alt="Interloper" width="96">
</p>

<h1 align="center">Interloper</h1>
<h3 align="center">The ultra-portable data asset framework</h3>

<p align="center">
Define assets as functions, group them in sources, wire dependencies from parameter names, and materialize them into pluggable destinations. The same code runs in a notebook, a container, or a scheduled platform.
</p>

<p align="center">
  <a href="https://github.com/digitl-cloud/interloper/actions/workflows/checks.yaml"><img src="https://github.com/digitl-cloud/interloper/actions/workflows/checks.yaml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://codecov.io/gh/digitl-cloud/interloper"><img src="https://codecov.io/gh/digitl-cloud/interloper/graph/badge.svg" alt="Coverage"></a>
  <a href="https://pypi.org/project/interloper-core/"><img src="https://img.shields.io/pypi/v/interloper-core?logo=pypi&logoColor=white&label=PyPI" alt="PyPI"></a>
  <img src="https://img.shields.io/badge/python-3.10+-3776ab?logo=python&logoColor=white" alt="Python 3.10+">
  <a href="https://github.com/digitl-cloud/interloper/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
</p>

<p align="center">
  <a href="https://docs.interloper.dev">Documentation</a> ·
  <a href="https://docs.interloper.dev/tutorial/">Tutorial</a> ·
  <a href="https://pypi.org/project/interloper-core/">PyPI</a> ·
  <a href="CHANGELOG.md">Changelog</a>
</p>

## Install

```bash
uv add interloper-core
# or: pip install interloper-core
```

## Quick start

```python
import datetime as dt

import interloper as il


class OrderCount(il.Schema):
    date: dt.date
    count: int


@il.source
class Shop(il.Source):
    currency: str = il.InputField(default="EUR", description="Reporting currency")

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "id": 1, "total": 99.9, "currency": self.currency}]

    @il.asset(schema=OrderCount, partitioning=il.TimePartitionConfig(column="date"))
    def order_count(self, context: il.ExecutionContext, orders: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "count": len(orders)}]


shop = Shop(destinations=il.CSVDestination(base_path="./data"))
result = il.DAG(shop).materialize(il.TimePartition(dt.date(2026, 1, 15)))
print(result)
# RunResult(status=completed, partition=2026-01-15, completed=2, failed=0, canceled=0, time=0.01s)
```

Nothing in this example was wired by hand: `currency` is a configurable field loadable from the
environment, `order_count` depends on `orders` because its parameter is named after it, the
schema is reconciled on every run, and the partition drives what each asset fetches. Swap
`CSVDestination` for a warehouse destination and the assets do not change.

## What the framework gives you

- **Assets and sources**: functions or methods returning rows, DataFrames or any object; sources
  group them with shared configuration, credentials and destinations.
- **Dependencies**: inferred from parameter names inside a source, declared with `requires` across
  sources, validated when the DAG is built.
- **Resources**: configs and connections injected by type annotation, loaded from the environment,
  with health checks, credential renewal and OAuth sign-in for connections.
- **Destinations**: CSV, pickle and in-memory built in; a `read()` and a `write()` for your own,
  or a handful of row operations for database-style stores.
- **Schemas**: declared or inferred, enforced with `AUTO`, `STRICT` or `RECONCILE` strategies.
- **Partitioning**: hourly, daily, monthly and yearly time partitions, windows, trailing lookbacks
  and backfills.
- **Execution**: an async-native engine with serial, concurrent and multi-process runners, sync
  entry points for scripts and notebooks, and a lifecycle event bus.
- **Specs and catalog**: every component is self-describing and serializable, so DAGs travel
  across processes, run from YAML files, and are catalogued for UIs.
- **Telemetry**: OpenTelemetry traces and metrics across process boundaries, off by default.

The [documentation](https://docs.interloper.dev) covers each of these with a guide page, an
extension guide for people building on the framework, and a reference section.

## Packages

The repository is a uv workspace. `interloper-core` is the framework; the other packages extend
it or build the platform on top of it.

| Package | Provides |
|---------|----------|
| [`interloper-core`](packages/interloper-core) | The framework: assets, sources, destinations, DAG, runners, partitioning, specs, catalog, CLI |
| [`interloper-pandas`](packages/interloper-pandas) | pandas DataFrame representation, conformer and normalizer |
| [`interloper-google-cloud`](packages/interloper-google-cloud) | Google Cloud connection, BigQuery and Cloud Storage destinations |
| [`interloper-slack`](packages/interloper-slack) | Slack connection and notification hook |
| [`interloper-assets`](packages/interloper-assets) | Ready-made sources for advertising, analytics and commerce platforms |
| [`interloper-docker`](packages/interloper-docker) | Docker runner and launcher |
| [`interloper-k8s`](packages/interloper-k8s) | Kubernetes runner and launcher |
| [`interloper-db`](packages/interloper-db) | Persistence layer: components, relations, runs, events, migrations |
| [`interloper-scheduler`](packages/interloper-scheduler) | Cron controller, hook evaluator, credential renewal, queue worker, reaper |
| [`interloper-api`](packages/interloper-api) | FastAPI backend |
| [`interloper-app`](packages/interloper-app) | Web UI (Nuxt SPA, bundled as static assets) |
| [`interloper-mcp`](packages/interloper-mcp) | MCP server exposing the catalog, lineage and run history to agents |
| [`interloper-agent`](packages/interloper-agent) | AI agent (Google ADK) |
| [`interloper-toolkit`](packages/interloper-toolkit) | Read-only tool functions shared by the agent and the MCP server |

All packages share one version and are released together. Every package is on PyPI under the
same name.

## Deploying the platform

`interloper app` runs the API, cron controller, queue worker and reaper from one command against a
Postgres database, with `interloper.yaml` and `INTERLOPER_*` environment variables as
configuration. Releases publish the pieces needed to run it in containers:

- **Docker images** on the [GitHub Container Registry](https://github.com/orgs/digitl-cloud/packages?repo_name=interloper),
  one per role and tagged with the version and `latest`: `interloper-api` (flavour `-agent`
  bundles the agent), `interloper-frontend`, `interloper-worker`, `interloper-scheduler` (flavours
  `-k8s` and `-docker` carry the matching launcher), `interloper-mcp` and `interloper-docs`.
- **A Helm chart** at [`https://digitl-cloud.github.io/interloper`](https://digitl-cloud.github.io/interloper).

```bash
docker pull ghcr.io/digitl-cloud/interloper-scheduler:latest-k8s

helm repo add interloper https://digitl-cloud.github.io/interloper
helm install interloper interloper/interloper
```

See [RELEASING.md](RELEASING.md) for how releases are cut and published.

## Development

```bash
make setup          # pre-commit hooks + uv sync --all-packages --all-extras
make check          # ruff, ty, pytest, and the frontend lint and typecheck
make dev            # reset, seed and run a local instance with the web UI on :3000
uv run zensical serve   # preview the documentation site
```

[AGENTS.md](AGENTS.md) documents the repository layout, the local dev instance, the image
catalog and the conventions (Conventional Commits, linear history, rebase-only branches).

## License

Apache 2.0. See [LICENSE](LICENSE).
