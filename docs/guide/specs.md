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

A component is referenced by exactly one of `path` (a fully qualified import path, what
`to_spec()` emits) or `key` (a [catalog](catalog.md) key, what hand-written specs may use).
`init` holds the field values; fields set to `None` are omitted; nested serializables
(destinations, resources, a normalizer) are nested specs.

Round-trip:

```py
rebuilt = il.Source.from_spec(spec)          # must reconstruct to a Source
rebuilt = il.Component.from_spec(spec.model_dump())
```

Calling `from_spec` on a subclass enforces the kind: a spec reconstructing to something else
raises `TypeError`. Nested `key` references resolve through the catalog passed in, or the
settings-configured catalog built lazily.

## Sources and their assets

A source is the unit of reconstruction. Its spec carries the assets as an **override map**
keyed by asset key rather than as individual specs, which keeps the document compact and lets
per-asset state (destinations, `materializable`, dependency wiring) survive:

```py
Shop(account_id="act_1").to_spec().init
# {"account_id": "act_1", "assets": {"orders": {"id": "...", "materializable": True, ...}, ...}}
```

Reconstruction builds each asset class with its overrides. The map is also the list of assets the
source ends up with: an asset absent from a non-empty map does not exist after reconstruction.
To restrict what runs while keeping every asset wired, use `select` instead.

## Spec files

`Spec.from_file(path)` loads a YAML document, interpolating `${VAR}` placeholders from the
environment in every string value. Unresolved variables are a hard error, so credentials never
need to live in the file:

```yaml
# shop.yaml
key: shop
init:
  account_id: act_1
  destinations:
    - key: bigquery_destination
  resources:
    connection:
      key: shop_connection
      init:
        api_key: ${SHOP_API_KEY}
```

```py
source = il.Source.from_spec_file("shop.yaml")
dag = il.DAG.from_spec_file("shop.yaml")           # any runnable component
```

Invalid YAML, a missing file, undefined variables and malformed documents raise `SpecError`; a
component kind that is not a workload raises `DAGError` from `DAG.from_spec_file`. The CLI's
`interloper run -f` is this call.

## DAG specs

A `DAGSpec` is a flat list of component specs: sources with their asset override maps, plus
standalone assets. It is what `MultiProcessRunner` ships to its workers and what
`interloper run --format inline` accepts:

```py
from interloper.dag import DAGSpec

dag_spec = dag.to_spec()
payload = dag_spec.model_dump(mode="json")
dag = il.DAG.from_spec(DAGSpec(**payload))
```

The override map is built from the DAG's **actual** asset instances, so a mini-DAG's read-only
parents stay read-only after the round-trip.

## What makes something serializable

`Serializable` is a pydantic model with a few additions:

- a class-level `key`, snake_cased from the class name unless declared;
- strict construction: unknown keyword arguments raise `TypeError` instead of being dropped;
- `to_spec()`, `from_spec()`, `from_spec_file()`, `classpath()`, `resolve_path()`;
- `config_schema()`, the JSON Schema of the user-facing fields.

`Component` extends it with identity (`kind`, `id`), resource slots and relations. Runners,
normalizers and schemas are serializable without being components. The full picture is in the
[component model](../extending/components.md).
