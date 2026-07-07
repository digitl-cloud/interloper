---
hide:
    - navigation
    - toc
---

<div class="title center" markdown>
# Interloper
## The ultra-portable data asset framework
</div>

Interloper is a Python data-asset framework that makes defining, configuring and materializing data assets effortless.
It combines the flexibility of a very lightweight library with powerful execution features inspired by modern orchestrators — and an **async-native** execution engine.

---

## Core concepts

- **Everything is an asset** -- In Interloper, an asset is a first-class entity. It **produces data**, which is then **materialized independently**. The framework provides a simple, structured way to define assets without unnecessary complexity.
- **Destination-driven materialization** -- Asset materialization is **driven by destination configuration**, completely separate from how the data is produced. This allows for clean, flexible execution.
- **Framework-agnostic data outputs** -- Interloper **does not enforce metadata dependencies** on destinations. Your data is written and mutated in a **deterministic, transparent way**, ensuring full control over your pipelines.

## Features

- **Asset & source definition** -- Define structured, reusable data assets using decorators.
- **Multi-destination materialization** -- Write the same asset to multiple destinations at once.
- **Schema & normalizer** -- Normalize, validate and reconcile data structures automatically.
- **Upstream asset dependencies** -- Build logical relationships between assets with automatic dependency resolution.
- **Data validation** -- Ensure data integrity before and during materialization.
- **Partitioning & backfilling** -- Efficiently process and reprocess historical data.
- **Async-native runners** -- Execute assets serially, concurrently with asyncio, across processes, in Docker, or on Kubernetes.
- **Connections & resources** -- Inject credentials and reusable services into assets, with a built-in OAuth connect flow.
- **REST & pagination** -- Build connectors against paginated REST APIs with composable clients, auth and paginators.
- **Event system** -- Subscribe to lifecycle events for monitoring and observability.
- **Catalog & declarative workloads** -- Introspect a catalog of components and materialize Jobs declaratively from YAML specs.
- **CLI** -- Run and backfill DAGs from the command line.

---

<div class="center" markdown>
# Destinations
Interloper ships with built-in destinations and additional packages for external systems.
</div>

<div class="grid cards center" markdown>
- :material-file-outline: **FileDestination**
    Pickle-based local filesystem storage. Ships with the core library.
- :material-file-delimited-outline: **CSVDestination**
    CSV files on the local filesystem. Ships with the core library.
- :material-memory: **MemoryDestination**
    In-memory storage for testing and development. Ships with the core library.
- :material-cloud: **BigQueryDestination**
    Google BigQuery materialization via `interloper-google-cloud`.
</div>

Need a custom backend? Subclass `il.Destination` (or `il.DatabaseDestination` for SQL-style stores) and implement `read`/`write`. See [Destinations](features/destinations.md).

---
<div class="center" markdown>
# Asset Library

Alongside Interloper, we maintain a **pre-built collection of sources** that pull data from well-known platforms -- ranging from **social media to digital marketing and beyond**. These ready-to-use sources help you **bootstrap your data stack instantly** without reinventing the wheel.

Install via `pip install interloper-assets`.
</div>

<div class="grid cards center" markdown>
- :material-advertisements: **Adservice**
- :material-advertisements: **Adup**
- :fontawesome-brands-amazon: **Amazon Ads**
- :fontawesome-brands-amazon: **Amazon Selling Partner**
- :material-link-variant: **Awin**
- :fontawesome-brands-microsoft: **Bing Ads**
- :material-chart-line: **Brandwatch**
- :fontawesome-brands-google: **Campaign Manager 360**
- :material-target: **Criteo**
- :fontawesome-brands-google: **Display Video 360**
- :fontawesome-brands-google: **DoubleClick Bid Manager**
- :fontawesome-brands-facebook: **Facebook Ads**
- :fontawesome-brands-facebook: **Facebook Insights**
- :fontawesome-brands-google: **Google Ads**
- :material-link-variant: **Impact**
- :fontawesome-brands-instagram: **Instagram Insights**
- :fontawesome-brands-linkedin: **LinkedIn Ads**
- :fontawesome-brands-linkedin: **LinkedIn Organic**
- :fontawesome-brands-pinterest: **Pinterest Ads**
- :fontawesome-brands-google: **Search Ads 360**
- :fontawesome-brands-google: **Search Console**
- :fontawesome-brands-snapchat: **Snapchat Ads**
- :material-advertisements: **Teads**
- :material-advertisements: **The Trade Desk**
- :fontawesome-brands-tiktok: **TikTok Ads**
- :material-cookie: **Usercentrics**
</div>
