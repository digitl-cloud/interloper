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
   form (`@il.source(resources={...}) class X(il.Source)` with `@il.asset` methods, see the
   interloper-source skill) is the documented one. `FileDestination` writes
   `{base_path}/{dataset}/{table}/data.pkl`. `AppSettings()` from the project directory
   validates an `interloper.yaml`. `runner: type: k8s` in `interloper.yaml` is still a
   valid registry key; confirm registered keys with
   `python -c "from importlib.metadata import entry_points; print([e.name for e in entry_points(group='interloper.runners')])"`.

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
