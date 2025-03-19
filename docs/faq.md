# FAQ

### Why another data framework?

Because the existing ones are either too opinionated, too bloated, or just don’t get out of your way.

* ETL tools force rigid workflows.
* Orchestration frameworks overcomplicate simple jobs.
* DIY pipelines break the moment your schema changes.

Interloper sits in between:

* **Simple when you want it**: write a function, materialize an asset.
* **Powerful when you need it**: define dependencies, automatically reconcile schemas, partitioning and backfill strategies, etc.

Interloper essentially positions itself as an alternative to DLT, while being simpler and yet more powerful on several aspects. In terms of concepts and design, Interloper draws a lot of inspiration from Dagster.