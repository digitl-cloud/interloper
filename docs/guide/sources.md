# Sources

A source groups assets that share configuration, credentials, destinations and helpers. It is
the unit you configure, serialize and run.

## Defining a source

```py
import interloper as il

@il.source(name="Facebook Ads", icon="logos:facebook", tags=["Advertising"])
class FacebookAds(il.Source):
    """Campaigns, ads and their daily statistics from the Marketing API."""

    connection: FacebookAdsConnection

    account_id: str = il.InputField(description="Ad account id", discriminator=True)

    @il.asset(schema=Campaigns, tags=["Entity"])
    def campaigns(self) -> list[dict]:
        return self.connection.api.campaigns(self.account_id)

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats(self, context: il.ExecutionContext) -> list[dict]:
        return self.connection.api.insights(self.account_id, context.partition_date)
```

Four kinds of things live in the class body:

- **Relations**: an annotation naming a component class (`connection: FacebookAdsConnection`)
  declares a link to the component that fills it, not a pydantic field. The source holds one
  instance, and its assets read it as `self.connection`, since a method asset receives the
  source as `self`. An asset declares a relation of its own only for what its source does not
  hold (an upstream, a config of its own).
- **Configuration fields**: pydantic fields, optionally declared with the
  [field helpers](fields.md) so a UI knows how to render them. They load from the environment
  like any pydantic-settings model and can be set at construction.
- **Assets**: methods decorated with `@il.asset`. They are collected into `asset_types` and
  receive the source instance as `self`.
- **Helpers**: ordinary methods and attributes the assets call.

The class need not extend `il.Source`; the decorator builds a subclass either way. Extending it
keeps type checkers informed.

### Decorator options

```py
@il.source(
    key="facebook_ads",                       # override the derived key
    name="Facebook Ads",
    icon="logos:facebook",
    tags=["Advertising"],
    relations={
        "connection": FacebookAdsConnection,            # wins over the class annotations
        "destinations": [BigQueryDestination],          # allowed destination classes
    },
    dataset="raw_facebook",                             # default dataset for the assets
    default_destination_key="warehouse",
    normalizer=il.Normalizer(),                         # default normalizer for the assets
    materialization_strategy=il.MaterializationStrategy.RECONCILE,
)
class FacebookAds(il.Source): ...
```

All options are in [Decorator options](decorators.md).

### Functional form

A source can also be a function returning asset definitions. Its annotated parameters become
configuration fields:

```py
@il.source
def open_meteo(latitude: float = 52.52, longitude: float = 13.41):
    @il.asset
    def forecast(context: il.ExecutionContext) -> list[dict]:
        ...

    return [forecast]
```

Assets in this form cannot reach the source's configuration through `self`, which is why the
class form is preferred for anything configurable.

## Instances

```py
source = FacebookAds(account_id="act_123")
source = FacebookAds(account_id="act_123", destinations=BigQueryDestination(...))
source = FacebookAds(account_id="act_123", connection=FacebookAdsConnection(...))
```

Every declared relation is a constructor keyword under its own name; a list binds several
targets, `None` binds nothing. Attribute access reads what is bound (`source.connection`), and
`bind`, `unbind` and assignment change it afterwards.

Assets are attributes, under their key:

```py
FacebookAds.campaigns          # the asset definition (class)
source.campaigns               # the live asset instance owned by this source
source.campaigns.materialize()
```

An asset class is its own descriptor: read on the class it is the class, read on an instance
the live asset. Assets written in the class body are the typed form (`source.campaigns` types
as `Asset`); an asset given only in `asset_types` is reached the same way at runtime, but a type
checker cannot see it. An asset class belongs to one source: listing it in a second is a
`TypeError` at definition.

### Reconfiguring

Calling an instance returns a copy with the named changes applied; the original is untouched:

```py
staging = source(destinations=il.CSVDestination(base_path="./staging"))
read_only = source(enabled=False)
```

The rule is the one every component follows: naming a field or a relation changes it, leaving it
out keeps it. `None` clears a relation, and makes a setting the assets inherit inherit again. The
copy's assets are copied with it, still wired to each other, while the connections and
destinations they read are shared.

### Narrowing and disabling

A source built with `enabled=False` makes every one of its assets read-only: they stay in a run
as dependencies, so wiring still validates and their stored output is still readable. An asset
can also be disabled alone (`source.campaigns(enabled=False)`). To build a source with only some
of its assets, pass the `assets` override map, which is what the platform persists:

```py
source = FacebookAds(account_id="act_123", assets={"campaigns": {}, "ads_stats": {}})
```

## What assets read through their source

An asset never holds a copy of its source's settings or bindings: it reads what it leaves unset
through the source, on every read, so a change to the source reaches its assets at once.

| Asset reads | When it leaves unset |
|-------------|----------------------|
| `dataset` (the source's, else the source key; `{field}` templates filled from the source) | its own `dataset` |
| `default_destination_key`, `normalizer`, `materialization_strategy`, `retry` | its own |
| `enabled` | never unset: an asset runs only while it and its source are enabled |
| any relation, such as `connection` or `destinations` | its own binding |

A relation is read by name, filtered by what the asset's relation accepts. A binding the asset
holds itself always wins, and a spec carries only what an asset set itself. The source still
trickles its bindings into its destinations, so a `GoogleCloudConnection` set on the source
reaches a `BigQueryDestination` that declares a `connection` relation.

An asset relation that names a sibling (a bare key) is bound at construction too, from the
source's own asset instances. Anything reaching outside the source is left to the
[DAG](dependencies.md#how-wiring-works).

## Dataset and table naming

Every asset materializes into `dataset.table`. The dataset is the asset's own, else the
source's `dataset`, else the source key; either may name a source field in braces
(`"analytics_{property_id}"`).
The table is computed by `Source.table()`:

- Without a discriminator: the asset key (`campaigns`).
- With a discriminator: `{asset_key}__{discriminator}` (`campaigns__act_123`), so several
  instances of one source land side by side in one dataset.
- A reference's `table` as written, with the source's fields filled in.

The default is coerced to a valid identifier (lowercase letters, digits, underscores). Override
`table()` for another convention; its result is validated when the source resolves its assets.

### The discriminator

One configuration field may be marked `discriminator=True`. Its value is what distinguishes
instances of the same source: an ad account, a site URL, a property id. It drives the derived
table suffix above and the default display name (`source.instance_name()`). Marking two fields
raises `TypeError`.

## Definitions

`FacebookAds.definition()` returns a `SourceDefinition`: key, name, description, tags, the JSON
Schema of its configuration fields, its declared relations, and one `AssetDefinition` per
asset. `FacebookAds.campaigns.definition()` returns a single asset's. This is what the
[catalog](catalog.md) is built from.

## Sources are workloads

A source is a `Workload`: `il.DAG(source)` flattens it into its assets. Runners never execute a
source, only its assets.
