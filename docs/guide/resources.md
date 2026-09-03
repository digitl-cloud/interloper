# Resources & configs

A resource is an injectable dependency: a settings object, a credential holder, a client, a
cache. Assets, sources and destinations declare the resources they need, and the framework
resolves and injects them at run time.

## Defining a resource

Every resource extends `il.Resource`, itself a pydantic-settings model. Fields load from
constructor arguments, a `.env` file, or environment variables, in that order:

```py
import interloper as il

class Cache(il.Resource):
    host: str = "localhost"
    port: int = 6379

    def get(self, key: str): ...
```

Two resource kinds ship with the core:

- **`Config`** for plain settings. Define with `@il.config` or by subclassing `il.Config`.
- **`Connection`** for credentials and clients. It adds health checks, credential renewal and
  OAuth. See [Connections](connections.md).

```py
@il.config(name="Reporting")
class ReportingConfig:
    currency: str = il.InputField(default="EUR")
    lookback_days: int = 7
```

Set `model_config = SettingsConfigDict(env_prefix="reporting_")` to namespace the environment
variables, as any pydantic-settings model does.

## Injecting resources into assets

Annotate a parameter with the resource type:

```py
@il.asset
def revenue(self, config: ReportingConfig, connection: ShopConnection) -> list[dict]:
    return connection.client.get("/revenue", params={"currency": config.currency}).json()
```

The parameter name is the **slot name**. An explicit declaration on the decorator does the same
and takes precedence over the annotation:

```py
@il.asset(resources={"config": ReportingConfig})
def revenue(self, config): ...
```

## The resolution cascade

When an asset runs, each slot is resolved in order:

1. The asset's own `resources[slot]`.
2. The source's `resources[slot]`, matched by name.
3. Any resource on the source that is an instance of the slot's type.
4. A fresh instance of the declared type, built from the environment.
5. `None`.

A resolved value that does not match the declared type raises `AssetError`. In practice this
means a resource "just works" from environment variables in development, and production injects
configured instances at the source or asset level:

```py
source = Shop()                                                   # everything from env
source = Shop(resources={"connection": ShopConnection(api_key="...")})
asset = source.revenue(resources={"config": ReportingConfig(currency="USD")})
```

## Resources on sources and destinations

Components other than assets declare resource **slots** too. The cleanest way is a typed class
attribute:

```py
@il.destination
class WarehouseDestination(il.Destination):
    connection: WarehouseConnection        # a slot named "connection"

    def write(self, context, data):
        self.connection.load(context.asset.table, data)
```

The annotation becomes a `ResourceRef` descriptor: it registers the slot in `resource_types`,
is removed from the pydantic fields, and gives typed attribute access that reads from
`self.resources`. Declare the descriptor directly to mark a slot as required:

```py
class WarehouseDestination(il.Destination):
    connection = il.ResourceRef(WarehouseConnection, required=True)
```

Accessing a required slot that was never filled raises `ValueError`; an optional one returns
`None`.

Slots are filled at construction, either through the `resources` dict or as keyword arguments
named after the slot:

```py
dest = WarehouseDestination(connection=WarehouseConnection(...))
dest = WarehouseDestination(resources={"connection": WarehouseConnection(...)})
```

Passing a value of the wrong type, or the same slot both ways, is an error.

## Trickling

A source fills the empty slots of its assets and destinations from its own resources, by slot
name first and by type second. A [job](jobs.md) does the same for its targets. Pre-filled slots
are never overwritten. `component.trickle_resources(child)` is the method behind it.

## Describing a resource

`ReportingConfig.definition()` returns a `ResourceDefinition` with the JSON Schema of the
user-facing fields (`config_schema`). Framework fields (`id`, `resources`) and anything listed in
the class's `internal_fields` are stripped from that schema. Resources are marked `sensitive`
by default, which tells the platform to encrypt their stored configuration.

The [field helpers](fields.md) decide how each field is rendered in a form.
