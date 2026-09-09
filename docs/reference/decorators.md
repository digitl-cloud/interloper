# Decorator options

A decorator is the function form of a class body, and a class body says three kinds of thing, so
every component decorator carries exactly three channels and nothing else. Each can be used bare
(`@il.asset`) or with arguments (`@il.asset(...)`).

## 1. Definition metadata and behaviour

Plain keyword arguments. They are routed by introspecting the kind's anchor class (`il.Asset`,
`il.Source`, `il.Destination`, `il.Connection`, `il.Config`), or the decorated class itself when it
already extends that anchor:

| A name that is | Becomes |
|----------------|---------|
| a public `ClassVar` of the anchor (`key`, `name`, `icon`, `tags`, `schema`, `partitioning`, `read_representation`, `oauth`, ...) | a class attribute of the built class |
| a field of the anchor (`dataset`, `default_destination_key`, `normalizer`, `materialization_strategy`, `auto_renew`, ...) | the default of that field |

Nothing is hand-maintained per kind: the accepted names are whatever the anchor declares, so an
unknown one is a `TypeError` at decoration listing them. Names carrying the framework's own
machinery (`kind`, `relations`, `internal_fields`, `asset_types`, `model_config`, anything private)
and the per-instance `id` are never routable.

```py
@il.asset(
    key="ads_stats",                                          # a ClassVar of il.Asset
    tags=["Report"],
    schema=AdsStats,
    materialization_strategy=il.MaterializationStrategy.RECONCILE,   # a field of il.Asset
)
def ads_stats(self, context: il.ExecutionContext) -> list[dict]: ...
```

## 2. `relations=`

The only relation channel: a map of relation name to one of three value forms.

| Value | Means |
|-------|-------|
| `il.Relation(...)` | the relation as written, which is where anything an annotation cannot express goes (a cross-source or many-valued upstream, `optional`, `on_delete`) |
| a component class | the same shorthand the annotation form is: `il.Relation(cls)` |
| a list of component classes | the relation the anchor already declares under this name, its key list narrowed to those classes' keys |

A list under a name the anchor declares no relation for, or holding a class of a kind that relation
does not accept, is a `TypeError` at decoration.

```py
@il.asset(
    relations={
        "destinations": [il.CSVDestination],                  # narrowed: only this class is accepted
        "config": AdsConfig,                                  # shorthand
        "budget": il.Relation("asset", "finance.budget", optional=True),
    },
)
def ads_stats(self, config: AdsConfig, budget: il.Upstream | None = None) -> list[dict]: ...
```

For an asset, the map is keyed by `data()` parameter name and wins over the relation inferred from
that parameter's annotation. For every other kind it is keyed by attribute name and wins over the
class annotations.

## 3. The build step

What the decorator does with what it decorates. This is the only part that differs per kind.

### `@il.asset`

Turns a function into the asset's `data()`. Sync and `async` functions are both accepted; a first
parameter named `self` makes it a **method asset**, receiving the source instance. The key defaults
to the function name, the docstring becomes the class docstring, and every other parameter declares
a relation read off its annotation.

```py
@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def ads_stats(self, context: il.ExecutionContext, campaigns: il.Upstream) -> list[dict]: ...
```

### `@il.source`

Takes a class or a function. A class declares its configuration fields, its assets (methods
carrying `@il.asset`, collected into `asset_types`) and its helpers. A function returns the asset
classes and turns its own annotated parameters into configuration fields.

```py
@il.source(tags=["Advertising"], dataset="raw_facebook")
class FacebookAds(il.Source): ...


@il.source
def open_meteo(latitude: float = 52.52) -> list[type[il.Asset]]:
    return [forecast]
```

### `@il.destination`, `@il.connection`, `@il.config`

Take a class. One already extending the anchor is stamped in place (a field default override
subclasses it, keeping every other field's metadata); a plain class is re-parented onto the anchor,
carrying its annotations and attributes over. `il.Connection` and `il.Config` extend
`BaseSettings`, so their fields still load from the environment. `oauth=` requires an
`OAuthConnection` subclass; a plain `Connection` is a `TypeError`.

```py
@il.connection(
    name="Amazon Ads",
    oauth=il.OAuthConfig("amazon", scope="advertising::campaign_management"),
)
class AmazonAdsConnection(il.RefreshTokenOAuthConnection):
    location: str = il.SelectField(...)
```

A source's `assets` and `select`, and the components bound to any relation, are instance settings:
they mean something only as constructor arguments.

## `@il.schema`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Schema key. |
| `name` | `str` | class | Display name. |

## Instance reconfiguration

Calling an instance returns a copy; omitted keywords mean "unchanged".

| `asset(...)` | `source(...)` |
|--------------|---------------|
| `id` | |
| `dataset` | `dataset` (re-points assets that inherited the old value) |
| `default_destination_key` | `default_destination_key` |
| `materializable` | `materializable` (applied to every asset) |
| `materialization_strategy` | `materialization_strategy` |
| `normalizer` (`None` clears) | `normalizer` |
| any relation name (replaced; `None` clears) | any relation name (replaced; `None` clears, and repoints what was trickled) |

## `OAuthConfig`

| Argument | Default | Meaning |
|----------|---------|---------|
| `provider` | required | Provider key in the registry. |
| `scope` | `""` | Scope to request. |
| `fields` | `{"client_id": "client_id", "client_secret": "client_secret", "refresh_token": "refresh_token"}` | OAuth role to connection field. |
| `auth_url` | from the provider | Required for an unregistered provider. |
| `label`, `icon` | from the provider | Display overrides. |

## Partition configs

| `TimePartitionConfig` | Default | Meaning |
|-----------------------|---------|---------|
| `column` | required | Partition column. |
| `granularity` | `TimeGranularity.DAY` | `HOUR`, `DAY`, `MONTH` or `YEAR`. |
| `allow_window` | `False` | Whether a run may cover several partitions. |
| `start` | `None` | First partition that exists. |

## Normalizer options

See [Normalization](../guide/normalization.md#options).
