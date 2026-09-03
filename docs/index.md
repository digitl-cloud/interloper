---
hide:
    - navigation
    - toc
---

<div class="title center" markdown>
# Interloper
## The ultra-portable data asset framework
</div>

Interloper is a Python framework for defining **data assets**, grouping them into **sources**,
wiring their **dependencies**, and **materializing** them into pluggable **destinations**. It is
a library first: a single asset is a function, and the whole engine runs in a script, a notebook,
a container, or a scheduler with the same code.

```py
import interloper as il

@il.source
class Shop(il.Source):
    @il.asset
    def users(self) -> list[dict]:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(self, users: list[dict]) -> list[dict]:
        return [{"count": len(users)}]

shop = Shop(destinations=il.CSVDestination(base_path="./data"))
il.DAG(shop).materialize()
```

---

## Core concepts

- **Assets** produce data. An asset is a function or method returning rows, a DataFrame, or any
  object a destination can store. Assets carry a key, an optional schema, and optional
  partitioning.
- **Sources** group assets that share configuration, credentials and destinations. Dependencies
  between sibling assets are inferred from parameter names.
- **Resources** are injected into assets by type annotation: configs for settings, connections
  for credentials and clients, or anything else you define.
- **Destinations** decide where and how data lands. The same asset writes to a local CSV folder
  or a warehouse without changing its code.
- **Partitions** slice assets by time at hourly, daily, monthly or yearly granularity. A run is
  always scoped to a partition or a window of partitions.
- **DAGs and runners** order assets by dependency and execute them serially, concurrently on the
  event loop, or across processes. The engine is async-native; sync entry points are provided.
- **Components and specs** make every asset, source, destination, connection and job
  self-describing and serializable, so they can be persisted, reconstructed and catalogued.

## Where to start

- [Getting started](getting-started.md): install, write a first asset, materialize it, build a DAG.
- [Tutorial](tutorial.md): an end-to-end walk through a real source with configuration,
  credentials, dependencies, schema, partitioning and the CLI.
- [Guide](guide/assets.md): one page per concept, from assets to telemetry.
- [Extending](extending/components.md): the component model, representations, runners and
  entry points for people building on the framework.
- [Reference](reference/decorators.md): every decorator option, setting, CLI flag, event type,
  span and error in table form.

## Beyond the core

This site documents `interloper-core`. Companion packages add a pandas representation, a
BigQuery destination, Docker and Kubernetes runners, a Slack hook, a library of ready-made
sources, and a full scheduling platform with an API and a web UI. See
[Ecosystem](reference/ecosystem.md) for the list and what each one registers.
