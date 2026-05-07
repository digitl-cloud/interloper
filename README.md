<h1 align="center">Interloper</h1>
<h3 align="center">A lightweight Python framework for building data assets</h3>

<p align="center">
Define assets as functions, group them in sources, wire dependencies automatically, and materialize them with pluggable IO backends and runners.
</p>

<p align="center">
  <a href="https://github.com/digitl-cloud/interloper-core/actions/workflows/checks.yaml"><img src="https://github.com/digitl-cloud/interloper-core/actions/workflows/checks.yaml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://codecov.io/gh/digitl-cloud/interloper-core"><img src="https://codecov.io/gh/digitl-cloud/interloper-core/graph/badge.svg" alt="Coverage"></a>
  <img src="https://img.shields.io/badge/python-3.10+-3776ab?logo=python&logoColor=white" alt="Python 3.10+">
  <a href="https://github.com/digitl-cloud/interloper-core/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
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

| Package             | Description                                           |
| ------------------- | ----------------------------------------------------- |
| `interloper-core`   | Core: assets, sources, DAG, runners, IO, partitioning |
| `interloper-sql`    | SQL IO backends (Postgres, MySQL, SQLite)             |
| `interloper-assets` | Pre-built source definitions and registry             |
| `interloper-dbos`   | DBOS durable execution                                |
| `interloper-docker` | Docker runner and backfiller                          |
| `interloper-k8s`    | Kubernetes runner and backfiller                      |

## Development

```bash
uv sync --all-packages --all-extras
uv run pytest
uv run ruff check .
uv run pyright
```
