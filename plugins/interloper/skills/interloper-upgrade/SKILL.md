---
name: interloper-upgrade
description: Use when moving a project, spec files or a deployment to a newer Interloper release, or when code written against an older version fails with missing attributes, unknown keywords, renamed events or renamed settings.
---

# Upgrading an Interloper project

## Overview

Releases are frequent and breaking changes are listed per version under "Breaking Changes" in
the changelog. The procedure is: find both versions, read the sections in between, apply the
renames, run one partition, migrate the database.
Changelog (raw, complete): https://raw.githubusercontent.com/digitl-cloud/interloper/main/CHANGELOG.md

## Recipe

1. **Versions.** There is no `il.__version__`:

   ```sh
   python -c "from importlib.metadata import version; print(version('interloper-core'))"
   ```

   The old version is in the lock file or the deployment's image tag. Run the project once
   before changing anything: the first `AttributeError` or `TypeError` names the first rename.

2. **Probe the code** for names that moved before reading anything else:

   ```sh
   python -c "import interloper as il; print([n for n in ('FileIO','OAuthConnectionBase','ASSET_COMPLETED') if hasattr(il, n) or hasattr(il.EventType, n)])"
   python -c "import interloper as il, inspect; print(inspect.signature(il.Source.__call__)); print([e.name for e in il.EventType])"
   ```

3. **Apply the renames.** Known moves, newest first. The rows marked `earlier` are not under a
   "Breaking Changes" heading; search the whole changelog for the old name, and treat this table
   as the record when the search finds nothing:

   | Version | Old | New |
   |---------|-----|-----|
   | 0.76.0 | `FileDestination` ignored the partition scope: one `{dataset}/{table}/data.pkl` per asset, and `{table}/{table}/` when no dataset was set | a `{column}={partition_id}` segment per partition, and no duplicated segment, the same layout as `CSVDestination`. Re-materialize, or move existing pickles into the partition directories |
   | 0.74.0 | a fail-fast break interrupted running operations | they finish on their own and are recorded |
   | 0.70.0 | `OAuthProvider.token_method`, `token_params`, `token_basic_auth`; `renew()` overrides | request-builder overrides on the provider; `renewable` is derived, drop `renew()` |
   | 0.68.0 | `EventType.ASSET_STARTED/COMPLETED/FAILED`, `RunResult.asset_executions` | `OPERATION_*` events filtered on `metadata["component_kind"] == "asset"`; `ASSET_DATA_*` mark only the `data()` step |
   | 0.64.0 | `Hook.events` accepted any list of strings | unknown event names and empty lists are rejected |
   | 0.63.0 | `CronJob(partitioned=...)` | removed; partitioning comes from the targets |
   | 0.62.0 | `Run.partition_date`, `Backfill.start_date/end_date`, `HookContext.partition_date` | `partition_key`, `start_key/end_key` |
   | 0.60.0 / 0.61.0 | `CronJob(backfill_days=...)`; `INTERLOPER_QUOTA_MAX_BACKFILL_DAYS` | `lookback` + `offset`; `INTERLOPER_QUOTA_MAX_BACKFILL_PARTITIONS` |
   | 0.54.0 | event `asset_id` / `asset_key` | `component_id`, `component_kind`, `component_key` |
   | 0.53.0 | `auth.signup_allowed_domains`, `INTERLOPER_AUTH_SIGNUP_ALLOWED_DOMAINS` | `auth.allowed_domains`, `INTERLOPER_AUTH_ALLOWED_DOMAINS` |
   | 0.49.0 | AUTO with a declared schema validated | AUTO reconciles: coerces, drops extra columns; `STRICT` keeps the rejecting behaviour |
   | earlier | `il.FileIO("data")` passed as `io=` | `il.FileDestination(base_path="data")` passed as `destinations=` |
   | earlier | `il.OAuthConnectionBase` with a hand-declared `refresh_token` and `OAuthConfig(fields={...})` | `il.RefreshTokenOAuthConnection`; the trio and the field mapping are built in |
   | earlier | `LINKEDIN_CLIENT_ID` / `_SECRET` | `INTERLOPER_LINKEDIN_CLIENT_ID` / `_CLIENT_SECRET`, exported in the environment (a `.env` file is not read), filled into the required `client_id` / `client_secret` fields before validation; `_REDIRECT_URI` only for the platform sign-in tab |
   | earlier | `asyncio.run(runner.run(dag))` | `il.run(runner.run(dag))`, or `dag.materialize(partition)` |
   | earlier | `SECRETS_ENCRYPTION_KEY` | `INTERLOPER_ENCRYPTION_KEY` |

   Functional `@il.source def ...` sources and `asyncio.run(...)` still work; the class-based
   form (`@il.source class X(il.Source)` with `@il.asset` methods, see the interloper-source
   skill) is the documented one. `FileDestination` writes
   `{base_path}/{dataset}/{table}/data.pkl`, plus a `{column}={partition_id}` segment when the
   asset is partitioned. `AppSettings()` from the project directory
   validates an `interloper.yaml`. `runner: type: k8s` in `interloper.yaml` is still a
   valid registry key; confirm registered keys with
   `python -c "from importlib.metadata import entry_points; print([e.name for e in entry_points(group='interloper.runners')])"`.

   **0.7x to the relation model.** `resource_types`, `depends_on`, `upstreams` and their
   companions collapse into one `il.Relation` primitive, declared under the name the parameter
   or the attribute already has. Rewrite a user's own sources with this table:

   | Old | New |
   |-----|-----|
   | `@il.source(resources={"connection": Conn})` | `connection: Conn` on the class body, or `relations={"connection": il.Relation(Conn)}` |
   | `@il.asset(resources={"config": Cfg})` | annotate the `data()` parameter `config: Cfg`, or `relations={"config": il.Relation(Cfg)}` |
   | `@il.asset(depends_on={"orders": "shop.orders"})` | `relations={"orders": il.Relation("asset", "shop.orders")}` |
   | `@il.asset(destinations=[Dest])` / `@il.source(destinations=[Dest])` | `relations={"destinations": [Dest]}` |
   | `il.Dependency(key="k", optional=True)` | `il.Relation("asset", "k", optional=True)` |
   | `il.Dependency(key="*.campaigns", many=True)` | `il.Relation("asset", "*.campaigns", many=True)` |
   | a `data()` upstream annotated `list[dict]` | annotate it `il.Upstream` and read `param.data` |
   | `Source(resources={"connection": conn})` | `Source(connection=conn)` |
   | `asset.upstreams["orders"] = [other.id]` | `asset.bind("orders", other)`, or `asset.orders = other` |
   | `component.resources["connection"]` | `component.connection`, or `component.bound("connection")` |
   | `component.trickle_resources(child)` | `component.trickle(child)` |
   | `il.ResourceRef(Conn, required=True)` | `conn: Conn = il.Relation(Conn)` (non-optional by default) |
   | spec `resources: {connection: {...}}` | the relation's own name: `connection: {...}` |
   | spec `upstreams: {orders: [id]}` | `orders: {ref: id}`, a list for a `many` relation |
   | `DependencyNotFoundError` | `ConfigError` from `validate_relations` |
   | `class X(il.PartitionedDestination)` with `_write_scope` / `_read_scope` | `class X(il.Destination)` with `write_partition` / `read_partition`; the partition dispatch is the base class's |

   A relation left unbound is resolved when it is read: an explicit `default=`, else the target
   class built from the environment. So a connection that used to resolve through the cascade
   still resolves, but the validation error of a missing credential now surfaces from the asset
   that needed it rather than at build time.

4. **Run it** with the events visible and confirm the destination output: `python pipeline.py`
   for a script, or for a spec file:

   ```sh
   PYTHONPATH=. interloper run -f shop.yaml --date 2026-01-15 -v
   ```

   Event callbacks written against old metadata keys fail silently (`metadata.get("asset_key")`
   returns `None`); grep the callback for `asset_key`, `asset_id`, `ASSET_`.

5. **Platform**: `interloper db upgrade` after pulling the new images; migrations rewrite
   persisted configs (job `backfill_days`, `partitioned`) and historical events.

## Common mistakes

- Rewriting `runner: type: k8s` into a `launcher:` block: the runner key is still valid, and the
  launcher key is `kubernetes`, not `k8s`.
- Using `ASSET_DATA_COMPLETED` as "asset done": it fires before the destination write.
- Setting `client_id=` by hand to silence `Field required`: export the provider variables.
- Reading the docs as a migration guide: they describe the current API only; the changelog
  carries the old names.
