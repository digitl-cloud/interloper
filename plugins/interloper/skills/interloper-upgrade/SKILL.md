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
   | unreleased (core phase 5) | `Partition.id`; `Upstream.records`, `Representation.to_records`/`from_records`, `interloper_pandas.dataframe_to_records`; `interloper.representation.View`, `Representation.of(data)`; `InProcessRun`, `ContainerRun`, `JobRun`; `from interloper.destination import PartitionFilter`; `SourceError`, `utils.to_slug_case`, `utils.require_import`; `Catalog.get(key, default)`, `period_range(..., reversed=)`, `import_from_path(path, target_type)`, `Schema.infer(..., name=)`; `InputField(title=...)` (any field factory); `source(assets=...)` | `Partition.key`; `Upstream.rows`, `to_rows`/`from_rows`, `dataframe_to_rows`; `TableData`, `TableData.from_data(data)`; `AsyncRun`, `DockerRun`, `KubernetesRun`; `from interloper.partitioning import PartitionFilter`; removed (nothing replaces them); `Catalog.get(key)` then `or default`, reverse the range yourself, check the type yourself, the inferred schema is `InferredSchema`; `label=`; build a new source with `assets={key: {...}}` |
   | unreleased (core phase 4) | a destination taking its source's connection; a job writing its destinations into its targets (`job.targets[0].destinations`); `Component.trickle`, `Component.on_rebind`; `interloper.component.decorator.decorate`/`declare`, `build_asset_class`, `Serializable.build_class(decorated, classvars=, fields=)`, `Component._classvars`; giving a source another source's asset | bind `connection` on the destination; the job's destinations apply when it runs (`job.operations()`, `il.DAG(job)`); nothing replaces the hook, a component is fields plus relations; `Decorator(Kind)` (the public decorators are its instances, same call forms), `Serializable.build_class(ClassBody, **settings)`, `Serializable._classvars`; build the source's own assets (a copy keeps its owner) |
   | unreleased (core phase 3) | `MultiProcessRunner` and `runner: {type: multi_process}`; `SyncRunner`; `runner.state`; `Runner._run`, `_on_start`/`_on_end`, `_preflight`, `_submit_operation`, `_handle_completed`; `RunState(dag, metadata=...)`, `RunState.mark_*(..., emit=...)`, `start_run`/`end_run`, `attempts`, `*_operations`, `is_run_complete()`; `ExecutionInfo.effects`/`.exception`/`mark_*` | `AsyncRunner`, or `DockerRunner`/`KubernetesRunner` for isolation; a custom runner names its run (`Run = MyRun`) and declares `capacity`, its `Run` implements `execute(operation) -> OperationResult` (and `close()`); the run's state on the `RunResult`; `RunState(dag, window, metadata, reports_progress=...)` with `start`/`retry`/`finish`/`cancel_pending`, `ready`/`completed`/`failed`/`done`; `ExecutionInfo.result` (`.error` and `.traceback` read from it), `ExecutionInfo.attempt`; `OperationResult.from_execution` for the attempt loop |
   | unreleased (core phase 2) | `Source(select=[...])`; `Source.asset_def(key)`; `AssetRef`; the `asset(normalizer=...)` sentinel and `None` meaning "unchanged" in `asset(...)`/`source(...)`; assets holding copies of their source's dataset, default destination key, normalizer, retry and strategy, and of its relation bindings; `View._read_upstreams`/`_definition`, `IbisView._read_upstreams`/`_definition`, `Asset._read_destination` | `Source(enabled=False)` for a read-only source, the `assets` override map to narrow one; `Shop.<key>.definition()`; `Shop.<key>` (the class) and `source.<key>` (the live asset); `component(**changes)`: naming changes, `None` clears a relation or makes an inherited field inherit again; an asset reads what it leaves unset through its source on every read and runs only while it and its source are enabled; specs carry overrides only; `_upstream`/`_output`/`_land`; `asset.default_destination` |
   | unreleased (core phase 1) | event key `partition_or_window`; `IOContext(partition_or_window=...)`; `ExecutionContext(asset_key=, partitioning=, partition_or_window=)`; `RunResult.partition_or_window`; `str(window)` as `start:end`, `window.id`, `window.partition_count()`, `window.iter_partitions()`; `TimeGranularity.WEEK`/`QUARTER`; a run scoped to a bare partition | event key `window`; `IOContext(window=...)` taking a `PartitionWindow` (`partition.window` for one); `ExecutionContext(asset, window, metadata)`; `RunResult.window`; `str(window)` as `start..end` (the partition id for a window of one), `len(window)`, iteration oldest first; the four BigQuery granularities only; every entry point still accepts a bare partition as the window of one |
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
   `{base_path}/{dataset}/{table}/data.pkl`, plus a `{column}={partition_key}` segment when the
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
   | `component.trickle_resources(child)` | `component.trickle(child)`, itself removed in core phase 4: nothing replaces the hook, a component is fields plus relations |
   | `il.ResourceRef(Conn, required=True)` | `conn: Conn = il.Relation(Conn)` (non-optional by default) |
   | spec `resources: {connection: {...}}` | the relation's own name: `connection: {...}` |
   | spec `upstreams: {orders: [id]}` | `orders: {ref: id}`, a list for a `many` relation |
   | `DependencyNotFoundError` | `ConfigError` from `validate_relations` |
   | `class X(il.PartitionedDestination)` with `_write_scope` / `_read_scope` | `class X(il.Destination)` with `write_partition` / `read_partition`; the partition dispatch is the base class's |
   | `DatabaseDestination` hooks `_insert`, `_delete_all`, `_delete_partition`, `_delete_partition_range`, `_select_all`, `_select_partition`, `_select_partition_range`, `_count_by_partition`, `_transaction` | public `insert(table, dataset, data, context)`, `delete(table, dataset, where)`, `select(table, dataset, where)`, `count(table, dataset, column)`, `transaction()`; `where: PartitionFilter | None` (`.value` or half-open `.bounds`); the second positional is the dataset, `schema` now only ever means an `il.Schema` |
   | `write_disposition = WriteDisposition.APPEND` | gone: a partition is always deleted before its data is inserted |
   | `read_representation = "dataframe"` / `@destination(read_representation=...)` | gone: `_select` returns the backend's native type; consumers read `leg.records` |
   | `Representation.of(leg.data).to_records(leg.data)` in an asset | `leg.records` |
   | `Representation.of(x)` falling back to rows for a dict / generator / model / opaque object | raises `RepresentationError`; the asset unwraps dict / models / generators / `None` to rows right after `data()`, opaque objects still pass through conform when no schema is declared |
   | `interloper.conformer` (`Conformer`, `RowsConformer`), `interloper_pandas.conformer` (`DataFrameConformer`), `Representation.conformer`, `View.conformer` | gone: `validate(data, schema, strict=)`, `reconcile(data, schema)` and `infer(data)` are methods on `Representation` (a table type is one class), and `Representation.of(data).validate(schema)` / `.reconcile(schema)` / `.infer()` on the bound view; the reconcile span is `interloper.representation.reconcile` |
   | a `Destination` subclass leaving `write_partition`, `read_partition` or `partition_row_counts` unimplemented (`NotImplementedError` at first use) | the three hooks are abstract: `TypeError` at instantiation. Test fixtures that only need a destination-shaped component subclass `il.MemoryDestination` |
   | `il.DestinationDefinition` / `interloper.destination.DestinationDefinition` | gone: `Destination.definition()` returns a plain `ComponentDefinition`; a richer kind builds its model from `super().definition()` |
   | `MaterializationStrategy.AUTO` | gone: `RECONCILE` is the default and infers when no schema is declared; stored `auto` reads as `reconcile`. `STRICT` now returns canonical values too (validated, then coerced) |
   | `materialization_strategy` on a `DatabaseDestination` / `@destination(materialization_strategy=...)` | gone: conform runs once on the asset and destinations receive canonical data; a stored key is stripped from destination configs |
   | `Representation.validate(data, schema, strict=)` / `view.validate(schema)` | `reconcile(data, schema, strict=True)` / `view.reconcile(schema, strict=True)` |
   | `Conformer.prepare`, `coerce_to_records`, `Normalizer._coerce` | gone: normalizers take `list[dict]` (`DataFrameNormalizer` also a DataFrame) and raise `NormalizerError` for anything else |
   | `Representation.of(data).to_records(data)` / `.columns(data)` / `.filter_eq(data, column, value)` | `Representation.of(data)` is a bound `View`: `.records`, `.columns`, `.filter_eq(column, value)`, and `.to("dataframe")` to convert |

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
