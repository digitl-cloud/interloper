---
name: interloper-run
description: Use when running, materializing or backfilling Interloper assets, sources or jobs from Python, a notebook, CI or the `interloper` CLI, including writing the YAML spec file a CLI run needs.
---

# Running Interloper assets

## Overview

There are two entry points: Python (`run()`, `materialize()`, `DAG`) and the `interloper run`
command. The command has no `materialize` or `backfill` subcommands; everything is `run` plus
flags, and anything beyond class defaults comes from a spec file.
Reference: https://docs.interloper.dev/guide/execution/ and https://docs.interloper.dev/guide/cli/

## Python

```py
shop.orders.run(il.TimePartition(dt.date(2026, 1, 15)))           # execute, return data, write nothing
shop.orders.materialize(il.TimePartition(dt.date(2026, 1, 15)))   # also write to destinations
dag = il.DAG(shop)                                                  # required once assets depend on each other
dag.materialize(il.TimePartition(dt.date(2026, 1, 15)))
for partition in il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 7)):   # newest first
    dag.materialize(partition)
result = il.run(il.AsyncRunner(max_workers=8, fail_fast=False).run(dag, partition))  # notebook-safe sync bridge
```

Partitioned assets always need a partition. `materialize()` on an asset with dependencies needs
`dag=dag` so the upstream can be read.

## CLI recipe

1. **Make the module importable.** A console script does not put the working directory on
   `sys.path`: run with `PYTHONPATH=.` or install the package.

2. **Write a spec file.** `interloper run <import.path>` instantiates the class with its
   defaults, which means no destinations, so any asset with a dependency fails with
   `No destination found for upstream asset`. The spec carries destinations and configuration:

   ```yaml
   # shop.yaml
   path: shop.Shop            # or key: shop when the package is installed and registered
   init:
     account: acme
     destinations:
       - path: interloper.destination.csv.CSVDestination
         init:
           base_path: ./data
   ```

   Connections resolve from the environment when their fields are env-loadable
   (`SHOP_API_KEY` for a connection with `env_prefix="shop_"`); a `resources:` block is only
   needed to pin explicit values. Write `${VAR}` placeholders in block style, never inside
   `{ ... }` flow mappings, where the braces break the YAML.

3. **Dry-run, then run:**

   ```sh
   export SHOP_API_KEY=...
   PYTHONPATH=. interloper run -f shop.yaml --date 2026-01-15 --dry-run
   PYTHONPATH=. interloper run -f shop.yaml --date 2026-01-15
   ```

   The date flag takes a partition key whose shape is the granularity: `2026-01-15`, `2026-01`,
   `2026`, `2026-01-15T13`. `--dry-run` validates the graph and the scope, not credentials: a
   missing key only fails on the real run, as `api_key: Field required`.

4. **Backfill from the CLI is a loop.** `--start-date`/`--end-date` is one run covering the
   window and is rejected unless every partitioned asset declares `allow_window=True`. For one
   run per day:

   ```sh
   for d in 2026-01-0{1..7}; do PYTHONPATH=. interloper run -f shop.yaml --date "$d" || break; done
   ```

5. **Check the output.** CSV lands at `./data/<dataset>/<table>/<column>=<key>/data.csv`
   (`./data/<dataset>/<table>/data.csv` unpartitioned); re-running a partition overwrites it.
   CSV files hold strings; there is no CLI read command, so read back typed from Python:

   ```py
   dest = il.CSVDestination(base_path="./data")
   dest.read(il.IOContext(asset=Shop().order_stats, partition_or_window=il.TimePartition(dt.date(2026, 1, 15)), schema=OrderStats))
   ```

   The command exits non-zero when the run did not complete; `-v` shows destination reads and
   writes, `--events json` streams the events.

## Quick reference

| Situation | Do |
|-----------|----|
| Runner or concurrency | `interloper.yaml` `runner: {type: async, config: {max_workers: 8}}` or `INTERLOPER_RUNNER_TYPE` |
| Only some assets | `select: [orders]` in the spec's `init` |
| Several sources in one run | a `path: interloper.job.base.Job` spec with `targets:` and shared `destinations:` |
| Reuse a registered class by name | `key: <component key>`; the catalog is `interloper.yaml`'s `catalog:` list or everything installed |
| Docs pages | `/guide/execution/`, `/guide/cli/`, `/guide/specs/`, `/guide/backfilling/`, `/reference/cli/` under https://docs.interloper.dev |

## Common mistakes

- `interloper materialize`, `interloper backfill`: neither exists.
- Running an import path for a source whose assets depend on each other; use a spec file.
- Expecting `--start-date`/`--end-date` to loop; it is a single windowed run.
- Forgetting `PYTHONPATH=.` and reading `No module named ...` as a packaging problem.
- Reading a YAML parse error at `${VAR}` as a spec problem; the placeholder sits inside a
  `{ ... }` flow mapping and needs block style.
