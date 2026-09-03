# Decorator options

Every decorator can be used bare (`@il.asset`) or with keyword arguments (`@il.asset(...)`).
Options marked *class* set class-level attributes; options marked *field* set the default of an
instance field.

## `@il.asset`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Asset key. Defaults to the snake_cased function name. |
| `name` | `str` | class | Display name. |
| `icon` | `str` | class | Icon identifier. |
| `tags` | `list[str]` | class | Catalog tags. |
| `schema` | `type[Schema]` | class | Output schema. |
| `partitioning` | `PartitionConfig` | class | Partition configuration. |
| `destinations` | `list[type[Destination]]` | class | Allowed destination classes. |
| `resources` | `dict[str, type[Resource]]` | class | Resource slots; wins over annotations. |
| `requires` | `dict[str, str]` | class | Mandatory upstream assets, parameter to key. |
| `optional_requires` | `dict[str, str]` | class | Optional upstream assets. |
| `materialization_strategy` | `MaterializationStrategy` | field | Schema enforcement. |
| `normalizer` | `Normalizer` | field | Normalizer applied before conform. |

## `@il.source`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Source key. Defaults to the snake_cased class name. |
| `name` | `str` | class | Display name. |
| `icon` | `str` | class | Icon identifier. |
| `tags` | `list[str]` | class | Catalog tags. |
| `resources` | `dict[str, type[Resource]]` | class | Resource slots. |
| `destinations` | `list[type[Destination]]` | class | Allowed destination classes. |
| `dataset` | `str` | field | Default dataset. Defaults to the source key when empty. |
| `default_destination_key` | `str` | field | Preferred destination for downstream readers. |
| `normalizer` | `Normalizer` | field | Default normalizer for the assets. |
| `materialization_strategy` | `MaterializationStrategy` | field | Default strategy for assets still on `AUTO`. |

Instance-only settings (`assets`, `select`, `destinations` instances, `resources` instances)
are constructor arguments, not decorator options.

## `@il.destination`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Destination key. |
| `name` | `str` | class | Display name. |
| `icon` | `str` | class | Icon identifier. |
| `tags` | `list[str]` | class | Catalog tags. |
| `resources` | `dict[str, type[Resource]]` | class | Resource slots. |
| `read_representation` | `str` | class | Representation reads materialize into (`"rows"`, `"dataframe"`). `DatabaseDestination` only. |
| `materialization_strategy` | `MaterializationStrategy` | field | Write-time schema strategy. `DatabaseDestination` only. |

## `@il.connection`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Connection key. |
| `name` | `str` | class | Display name. |
| `icon` | `str` | class | Icon identifier. |
| `tags` | `list[str]` | class | Catalog tags. |
| `oauth` | `OAuthConfig` | class | OAuth configuration. Requires an `OAuthConnection` subclass; `TypeError` otherwise. |

## `@il.config`

| Option | Type | Kind | Meaning |
|--------|------|------|---------|
| `key` | `str` | class | Config key. |
| `name` | `str` | class | Display name. |
| `icon` | `str` | class | Icon identifier. |
| `tags` | `list[str]` | class | Catalog tags. |

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
| `resources` (merged) | `resources` (merged) |
| `destinations` (replaced) | `destinations` (replaced) |
| `dataset` | `dataset` (re-points assets that inherited the old value) |
| `default_destination_key` | `default_destination_key` |
| `materializable` | `materializable` (applied to every asset) |
| `materialization_strategy` | `materialization_strategy` |
| `normalizer` (`None` clears) | `normalizer` |
| `dependencies` | |

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
