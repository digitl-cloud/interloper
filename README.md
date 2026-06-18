<h1 align="center">Interloper</h1>
<h3 align="center">A lightweight Python framework for building data assets</h3>

<p align="center">
Define assets as functions, group them in sources, wire dependencies automatically, and materialize them with pluggable IO backends and runners.
</p>

<p align="center">
  <a href="https://github.com/digitl-cloud/interloper/actions/workflows/checks.yaml"><img src="https://github.com/digitl-cloud/interloper/actions/workflows/checks.yaml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://codecov.io/gh/digitl-cloud/interloper"><img src="https://codecov.io/gh/digitl-cloud/interloper/graph/badge.svg" alt="Coverage"></a>
  <a href="https://pypi.org/project/interloper-core/"><img src="https://img.shields.io/pypi/v/interloper-core?logo=pypi&logoColor=white&label=PyPI" alt="PyPI"></a>
  <img src="https://img.shields.io/badge/python-3.10+-3776ab?logo=python&logoColor=white" alt="Python 3.10+">
  <a href="https://github.com/digitl-cloud/interloper/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
</p>

## Install

```bash
uv add interloper-core
```

## Quick Start

```python
import interloper as il

@il.source
class MySource:
    @il.asset
    def users(self) -> list:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(self, users: list) -> int:
        return len(users)

source = MySource(io=il.FileIO("data"))
dag = il.DAG(source)
dag.materialize()
```

## Packages

| Package                                                                       | Description                                              |
| ----------------------------------------------------------------------------- | -------------------------------------------------------- |
| [`interloper-core`](https://pypi.org/project/interloper-core/)                 | Core: assets, sources, DAG, runners, IO, partitioning    |
| [`interloper-assets`](https://pypi.org/project/interloper-assets/)             | Pre-built source definitions (bing, facebook, google, …) |
| [`interloper-pandas`](https://pypi.org/project/interloper-pandas/)             | pandas DataFrame normalizer and adapter                  |
| [`interloper-db`](https://pypi.org/project/interloper-db/)                     | Database persistence layer                               |
| [`interloper-google-cloud`](https://pypi.org/project/interloper-google-cloud/) | Google Cloud integration: BigQuery destination           |
| [`interloper-docker`](https://pypi.org/project/interloper-docker/)             | Docker runner and backfiller                             |
| [`interloper-k8s`](https://pypi.org/project/interloper-k8s/)                   | Kubernetes runner and backfiller                         |
| [`interloper-scheduler`](https://pypi.org/project/interloper-scheduler/)       | Cron scheduler, queue worker, and reaper                 |
| [`interloper-api`](https://pypi.org/project/interloper-api/)                   | FastAPI HTTP backend (reads catalog metadata only)       |
| [`interloper-agent`](https://pypi.org/project/interloper-agent/)               | AI agent (Google ADK)                                    |
| [`interloper-app`](https://pypi.org/project/interloper-app/)                   | Web UI (Nuxt SPA, bundled as static assets)              |

## Releases

Every release publishes across three channels, all versioned together by [`python-semantic-release`](https://python-semantic-release.readthedocs.io/) from the commit history.

### PyPI

All workspace packages are published to [PyPI](https://pypi.org/project/interloper-core/) (see the [Packages](#packages) table above for the full list and links).

```bash
pip install interloper-core
# or: uv add interloper-core
```

### Docker images (GHCR)

Container images are published to the [GitHub Container Registry](https://github.com/orgs/digitl-cloud/packages?repo_name=interloper), tagged with the release version and `latest`. There's one image per role; flavored variants ride the tag as a `-<flavor>` suffix (e.g. `:<version>-k8s`).

| Image                                                                                                            | Role                              | Flavor tags (`:<version>-<flavor>`)            |
| ---------------------------------------------------------------------------------------------------------------- | --------------------------------- | ---------------------------------------------- |
| [`ghcr.io/digitl-cloud/interloper-api`](https://github.com/digitl-cloud/interloper/pkgs/container/interloper-api)                             | FastAPI HTTP backend              | `-agent` (bundles the ADK agent / `/agent` routes) |
| [`ghcr.io/digitl-cloud/interloper-frontend`](https://github.com/digitl-cloud/interloper/pkgs/container/interloper-frontend)                   | Web UI (nginx-served SPA)         | —                                              |
| [`ghcr.io/digitl-cloud/interloper-worker`](https://github.com/digitl-cloud/interloper/pkgs/container/interloper-worker)                       | Queue worker                      | —                                              |
| [`ghcr.io/digitl-cloud/interloper-scheduler`](https://github.com/digitl-cloud/interloper/pkgs/container/interloper-scheduler)                 | Scheduler (cron + worker + reaper) | `-k8s` (Kubernetes launcher), `-docker` (Docker launcher) |

```bash
docker pull ghcr.io/digitl-cloud/interloper-api:latest
docker pull ghcr.io/digitl-cloud/interloper-api:latest-agent
docker pull ghcr.io/digitl-cloud/interloper-scheduler:latest-k8s
```

### Helm chart

The Helm chart is published to a GitHub Pages Helm repository at [`https://digitl-cloud.github.io/interloper`](https://digitl-cloud.github.io/interloper).

```bash
helm repo add interloper https://digitl-cloud.github.io/interloper
helm repo update
helm install interloper interloper/interloper
```

## Development

```bash
uv sync --all-packages --all-extras
uv run pytest
uv run ruff check .
uv run ty check
```
