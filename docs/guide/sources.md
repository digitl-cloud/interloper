# Sources

A source groups assets that share configuration, credentials, destinations and helpers. It is
the unit you configure, serialize and run.

## Defining a source

```py
import interloper as il

@il.source(name="Facebook Ads", icon="logos:facebook", tags=["Advertising"])
class FacebookAds(il.Source):
    """Campaigns, ads and their daily statistics from the Marketing API."""

    account_id: str = il.InputField(description="Ad account id", discriminator=True)

    @il.asset(schema=Campaigns, tags=["Entity"])
    def campaigns(self, connection: FacebookAdsConnection) -> list[dict]:
        return connection.api.campaigns(self.account_id)

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats(self, context: il.ExecutionContext, connection: FacebookAdsConnection) -> list[dict]:
        return connection.api.insights(self.account_id, context.partition_date)
```

Three kinds of things live in the class body:

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
    resources={"connection": FacebookAdsConnection},   # explicit resource slots
    destinations=[BigQueryDestination],                 # allowed destination classes
    dataset="raw_facebook",                             # default dataset for the assets
    default_destination_key="warehouse",
    normalizer=il.Normalizer(),                         # default normalizer for the assets
    materialization_strategy=il.MaterializationStrategy.RECONCILE,
)
class FacebookAds(il.Source): ...
```

All options are in [Decorator options](../reference/decorators.md).

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
source = FacebookAds(account_id="act_123", resources={"connection": FacebookAdsConnection(...)})
```

Resource slots can also be passed directly as keyword arguments named after the slot:
`FacebookAds(account_id="act_123", connection=FacebookAdsConnection(...))`.

Assets are attributes:

```py
FacebookAds.campaigns          # the asset definition (class)
source.campaigns               # the live asset instance owned by this source
source.campaigns.materialize()
```

### Reconfiguring

Calling an instance returns a deep copy with overrides applied; the original is untouched:

```py
staging = source(destinations=il.CSVDestination(base_path="./staging"))
read_only = source(materializable=False)
```

Accepted keywords: `resources` (merged), `destinations` (replaced), `dataset`,
`default_destination_key`, `materializable` (applied to every asset), `normalizer`,
`materialization_strategy`.

### Selecting assets

`select` limits which assets a run materializes. Unselected assets stay in the source as
read-only dependencies, so wiring still validates and their stored output is still readable:

```py
source = FacebookAds(account_id="act_123", select=["campaigns", "ads_stats"])
```

An unknown key raises `SourceError`.

## What cascades to assets

At construction the source fills in whatever its assets did not set themselves:

| Source setting | Asset receives it when |
|----------------|------------------------|
| `dataset` (defaults to the source key) | the asset's `dataset` is empty |
| `destinations` | the asset has none |
| `default_destination_key` | the asset's is empty |
| `normalizer` | the asset has none |
| `materialization_strategy` | the asset is still on `AUTO` |
| `resources` | an asset slot is empty; matched by slot name, then by type |

Resources also trickle into the destinations' empty slots, so a `GoogleCloudConnection` set on
the source reaches a `BigQueryDestination` that declares a `connection` slot.

## Dataset and table naming

Every asset materializes into `dataset.table`. The dataset is the source's `dataset` (or key).
The table is computed by `Source.asset_table()`:

- Without a discriminator: the asset key (`campaigns`).
- With a discriminator: `{asset_key}__{discriminator}` (`campaigns__act_123`), so several
  instances of one source land side by side in one dataset.

Override `asset_table()` for another convention. The result is coerced to a valid identifier
(lowercase letters, digits, underscores).

### The discriminator

One configuration field may be marked `discriminator=True`. Its value is what distinguishes
instances of the same source: an ad account, a site URL, a property id. It drives the derived
table suffix above and the default display name (`source.instance_name()`). Marking two fields
raises `TypeError`.

## Definitions

`FacebookAds.definition()` returns a `SourceDefinition`: key, name, description, tags, the JSON
Schema of its configuration fields, its relation vocabulary, and one `AssetDefinition` per
asset. `FacebookAds.asset_def("campaigns")` returns a single asset's definition with its
qualified key. This is what the [catalog](catalog.md) is built from.

## Sources are workloads

A source is a `Workload`: `il.DAG(source)` flattens it into its assets. Runners never execute a
source, only its assets.

## Imperative registration

`FacebookAds.register_asset_type(SomeAsset)` attaches an asset class defined elsewhere to a
source after the fact. Normally the class body does this for you.
