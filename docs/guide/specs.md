# Specs & serialization

Every component, and everything that configures one (normalizers, runners, schemas), is
**serializable**: an instance is "a class plus its configuration", and a `Spec` is its wire
form. Specs are how a DAG travels to another process, how a run is described in a YAML file,
and how the platform stores and rebuilds components.

## The spec envelope

```py
spec = source.to_spec()
spec.path     # "my_package.sources.Shop"    the class, as an import path
spec.key      # "" (or a catalog key instead of a path)
spec.id       # the instance id
spec.init     # the constructor payload, nested specs included
```

A component is named by exactly one of `path` (a fully qualified import path, what `to_spec()`
emits) or `key` (a [catalog](catalog.md) key, what hand-written specs may use). `init` holds
every field's value, defaults included; only a field left at `None` is omitted. It also carries
one entry per relation that holds a binding, under the relation's own name: a list for a `many`
relation, a single value otherwise. `id` is required only when something references it.

Round-trip:

```py
rebuilt = il.Source.from_spec(spec)          # must reconstruct to a Source
rebuilt = il.Component.from_spec(spec.model_dump())
```

Calling `from_spec` on a subclass enforces the kind: a spec reconstructing to something else
raises `TypeError`. Nested `key` references resolve through the catalog passed in, or the
settings-configured catalog built lazily.

## The reference rule

One rule decides whether a bound target is written out or pointed at, and no kind knows about
it:

- a target that **has a parent** is emitted under that parent and is a `{ref: id}` everywhere
  else. An asset lives under its source, so an upstream is always a reference;
- a target with **no parent** is written out in full the first time the traversal reaches it,
  and is a `{ref: id}` at every later one. A destination shared by two sources is inline once
  and referenced once.

A `{ref: id}` resolves inside the document first, then through `resolve=`:

```py
il.Component.from_spec(spec, resolve=store.load)
il.DAG.from_spec_file("daily.yaml", resolve=store.load)
```

In a hand-written manifest the referenced component has to be in the tree, since there is
nothing else to ask. In the platform, the store resolves it. A reference nobody answers raises
`SpecError`.

## Sources and their assets

A source is the unit of reconstruction. Its spec carries the assets as an **override map** keyed
by asset key rather than as individual specs, which keeps the document compact and lets
per-asset state (its own destinations, `materializable`, its bound upstreams) survive:

```py
Shop(account_id="act_1").to_spec().init
# {"account_id": "act_1", "assets": {"orders": {"id": "...", "materializable": True}, ...}}
```

Reconstruction builds each asset class with its overrides. The map is also the list of assets the
source ends up with: an asset absent from a non-empty map does not exist after reconstruction.
To restrict what runs while keeping every asset wired, use `select` instead.

## Spec files

`Spec.from_file(path)` loads a YAML document, interpolating `${VAR}` placeholders from the
environment in every string value. Unresolved variables are a hard error, so credentials never
need to live in the file. A manifest is one component, nested, and is exactly `to_spec()` output:

```yaml
path: interloper.job.cron.CronJob
init:
  cron: "0 7 * * *"
  destinations:
    - key: bigquery_destination
      id: bq
      init: {project: dwh, connection: {key: google_cloud_connection, init: {service_account_key: ${GCP_KEY}}}}
  targets:
    - key: facebook_ads
      init:
        account_id: act_1
        connection: {key: facebook_ads_connection, init: {access_token: ${FB_TOKEN}, app_id: "1"}}
        assets: {campaigns: {id: fb-campaigns, materializable: false}}
    - key: campaign_matcher
      init:
        destinations: [{ref: bq}]                # second occurrence of a parentless component: a reference
        assets:
          campaign_matches:
            campaigns: [{ref: fb-campaigns}]     # the asset has a parent: always a reference
```

Without the explicit `campaigns:` line the DAG binds every `*.campaigns` node it holds.

```py
job = il.Job.from_spec_file("daily.yaml")
dag = il.DAG.from_spec_file("daily.yaml")          # any runnable component
```

Invalid YAML, a missing file, undefined variables, malformed documents and unresolved references
raise `SpecError`; a component kind that is not a workload raises `DAGError` from
`DAG.from_spec_file`. The CLI's `interloper run -f` is this call.

## DAG specs

A `DAGSpec` is a list of component specs, one per root of the graph: sources with their asset
override maps, plus standalone assets. Every item shares one traversal, so the reference rule
holds across the whole document and a destination bound to several roots is written once. As
YAML it is one document per root, separated by `---`:

```py
from interloper.dag import DAGSpec

dag_spec = dag.to_spec()
payload = dag_spec.model_dump(mode="json")
dag = il.DAG.from_spec(DAGSpec(**payload))
```

It is what `MultiProcessRunner` ships to its workers and what `interloper run --format inline`
accepts. The override map is built from the DAG's **actual** asset instances, so a parent the run
only reads travels as its own document carrying that one asset with `materializable: false`,
and stays read-only after the round-trip.

## What makes something serializable

`Serializable` is a pydantic model with a few additions:

- a class-level `key`, snake_cased from the class name unless declared;
- strict construction: unknown keyword arguments raise `TypeError` instead of being dropped;
- `to_spec()`, `from_spec()`, `from_spec_file()`, `classpath()`, `resolve_path()`;
- `config_schema()`, the JSON Schema of the user-facing fields.

`Component` extends it with identity (`kind`, `id`, `parent`) and relations. Runners, normalizers
and schemas are serializable without being components. The full picture is in the
[component model](../extending/components.md).
