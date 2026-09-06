---
name: interloper-manifest
description: Use when writing or debugging an Interloper YAML spec that the CLI or a scheduler runs: a job over several sources, a shared destination, connections and secrets, restricting a source to some assets, or wiring a dependency between two sources.
---

# Writing an Interloper job spec

## Overview

A spec file is the serialized form of one component: `path` (or `key`) names the class, `init`
carries its constructor keywords, nested components are specs themselves. A job spec wraps
several sources in one run. The shape below is exact; the traps are in the `assets:` map and
in cross-source dependencies. Reference: https://docs.interloper.dev/guide/specs/ and
https://docs.interloper.dev/guide/jobs/

## Recipe

1. **Write the spec** around `interloper.job.base.Job`. Workload-level `destinations` cascade to
   every target that declares none:

   ```yaml
   # daily.yaml
   path: interloper.job.base.Job
   init:
     destinations:
       - path: interloper.destination.csv.CSVDestination
         init:
           base_path: ./data
     # a qualified `depends_on` key wires itself when exactly one match is in the job
     targets:
       - path: shop.Shop
         init:
           account: acme
           resources:
             connection:
               path: shop.ShopConnection
               init:
                 api_key: ${SHOP_API_KEY}   # block style, never inside { ... }
           assets:                           # a whitelist: only listed assets exist, so order_stats is out
             orders:
               id: shop-orders              # any unique string; the cross-source edge below points at it
       - path: finance.Finance
         init:
           currency: USD
           assets:
             revenue:
               upstreams: {orders: [shop-orders]}   # explicit wiring; only needed when several candidates exist
   ```

2. **Run it.** The CLI does not put the working directory on `sys.path`, and `${VAR}` is a hard
   error when the variable is unset:

   ```sh
   export SHOP_API_KEY=... PYTHONPATH=.
   interloper run -f daily.yaml --date 2026-01-15 --dry-run   # prints the plan: operations and generations
   interloper run -f daily.yaml --date 2026-01-15
   ```

   Read the dry-run plan: each numbered line is one generation, parallel operations share a
   line. A downstream asset on the same line as its upstream, or a `materializable / total`
   count that does not add up, means the edge is not wired. A failed run still leaves the
   completed upstream files under `./data`.

3. **Check the output** at `./data/<dataset>/<table>/<column>=<key>/data.csv`. Exit code is
   non-zero when the run did not complete.

## Rules that are not obvious

- **`assets:` is a whitelist.** Reconstruction builds only the assets listed in the map; an
  asset left out does not exist in the run, so `assets: {order_stats: {materializable: false}}`
  removes `orders`, not `order_stats`. Use `select:` to restrict what runs. Use `assets:` only
  for per-asset overrides (`id`, `materializable`, `destinations`, `upstreams`) and list
  every asset the run needs.
- **`depends_on` wires itself when unambiguous.** A qualified key (`shop.orders`) binds the
  single matching asset in the job; two matches (two `shop` instances) fail at load time with
  `DAGError`, and an unbound non-optional key fails at load time with
  `DependencyNotFoundError`. Wire by id (an `id` on the upstream, `upstreams:` on the
  downstream) only in the ambiguous case. A many-valued slot
  (`il.Dependency(key="*.campaigns", many=True)`) takes a list:
  `upstreams: {campaigns: [id-a, id-b]}`.
- **`${VAR}` is a spec-file feature.** `Spec.from_file` interpolates it; `interloper.yaml`
  (settings) does not, see the interloper-deploy skill.
- **Connections resolve from the environment** when their fields are env-loadable
  (`SHOP_API_KEY` for `env_prefix="shop_"`), so the `resources:` block is only needed to pin a
  value or to name a differently-named variable (`api_key: ${SHOP_PROD_KEY}`).
- **`key:` instead of `path:`** needs the package installed with an `interloper.components`
  entry point; a bare module is always `path: module.Class`.
- **Dump a spec to learn the shape**: `yaml.safe_dump(job.to_spec().model_dump(mode="json"))`
  from a Python-built `Job(...)`. The dump is exhaustive (generated ids, defaults, destinations
  repeated on every asset); keep only what differs from defaults.
- **CLI flags**: `--dry-run`, `--date KEY` (`2026-01-15`, `2026-01`, `2026`, `2026-01-15T13`),
  `--start-date/--end-date` (one windowed run), `--events json`, `-v`, `-q`, `--run-id`.

## Quick reference

| Need | Spec |
|------|------|
| Cron schedule the platform runs | `path: interloper.job.cron.CronJob` with `cron`, `timezone`, `lookback`, `offset`; the CLI runs it as a plain job |
| Destination shared by all targets | `init.destinations` on the job |
| Destination for one target | `init.destinations` on that target (overrides the job's) |
| Runner or concurrency | not in the spec: `interloper.yaml` `runner:` block or `INTERLOPER_RUNNER_TYPE` |
| A single source, no job | `path: shop.Shop` at the top level with the same `init` keys |

## Common mistakes

- `materializable: false` to drop an asset: the map is a whitelist and the other assets vanish.
- Reading `DependencyNotFoundError: ... nothing is wired in the DAG` as a bug: it means the job
  lacks the upstream source. Reading `DAGError: ... matching assets` as a bug: it means two
  instances match and one must be wired by id.
- `il.DAG([shop, fin])`: the constructor is varargs, `il.DAG(shop, fin)`.
- `${VAR}` inside a `{ ... }` flow mapping breaks the YAML.
- Reading `No module named 'shop'` as a spec error: set `PYTHONPATH=.`.
