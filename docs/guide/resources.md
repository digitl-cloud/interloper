# Resources & configs

A resource is an injectable dependency: a settings object, a credential holder, a client, a
cache. Assets, sources and destinations declare the resources they need as **relations**, and the
framework resolves and injects them at run time.

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

Annotate a parameter with the resource class. The parameter name is the relation name:

```py
@il.source
class Shop(il.Source):
    connection: ShopConnection

    @il.asset
    def revenue(self, context: il.ExecutionContext, config: ReportingConfig) -> list[dict]:
        return self.connection.client.get("/revenue", params={"currency": config.currency}).json()
```

`connection: ShopConnection` on the source body is the same declaration, one level up: an
annotation naming a component class declares a relation rather than a pydantic field. The source
holds one instance, and a method asset reads it as `self.connection`, since it receives the source
as `self`. An asset declares a resource of its own, like `config` above, only for what its source
does not hold; declaring `connection: ShopConnection` on the asset as well is legal, and the
asset [reads the source's instance](#reading-through-the-source), but it says twice what the
source says once.

## Explicit relations

`il.Relation` is what says anything beyond "a resource of this class fills this name". Use it on
the decorator, keyed by parameter name, or as a typed class attribute:

```py
@il.asset(relations={"config": il.Relation(ReportingConfig, optional=True)})
def revenue(context: il.ExecutionContext, config: ReportingConfig | None) -> list[dict]:
    ...


@il.destination
class WarehouseDestination(il.Destination):
    connection: WarehouseConnection
    config: WarehouseConfig = il.Relation(WarehouseConfig, default=lambda: WarehouseConfig(retries=5))

    def write(self, context, data):
        self.connection.load(context.asset.table, data)
```

`il.Relation(ReportingConfig)` is shorthand for `il.Relation(kind="config", key="reporting_config")`
with the class kept as the relation's `target`, which is what makes a fallback possible.
`optional=True` allows the relation to stay unbound; `default=` is a zero-argument factory.
Neither says anything about deletion: `on_delete` alone decides that, and a resource relation
keeps its `block` default whether it is optional or not, so a bound connection, config or
destination cannot be deleted while a component consumes it.

## Fallbacks

Nothing is resolved at construction. When `data()` (or any reader) asks for a single-valued
relation that holds no binding, `resolve(name)` falls back, in order:

1. `default()`, when the relation declares one.
2. A fresh instance of the target class, when it is a `Resource`: its required fields come from
   the environment, so a credential nobody bound is still constructible where it is read.
3. A fresh instance of the target class, when every one of its fields is defaulted.
4. `None`.

A `many` relation never falls back: `resolve(name)` on it is exactly `bound(name)`, an empty list
when nothing is bound.

A fallback is never bound, so an explicit binding, or one read through the owning source,
always wins over it, and `to_spec()` never carries an auto-instantiated component. Attribute
access reads bindings only, the asset's own or its source's: `asset.connection` is `None` while
neither holds one, where the same relation injected into `data()` goes through `resolve()` and
receives the fallback. Because a resource is built at the
moment it is read, a credential that is neither bound nor in the environment surfaces as a
pydantic validation error from the read that needed it, not at build time.

## Relations on sources and destinations

Components other than assets declare the same relations, and are constructed with them by name:

```py
dest = WarehouseDestination(connection=WarehouseConnection(...))
source = Shop(connection=ShopConnection(api_key="..."), destinations=[dest])
```

A relation name accepted as a constructor keyword binds the relation instead of reaching
pydantic; a list binds every element, `None` binds nothing. Passing a component the relation does
not accept, or two of them to a single-valued relation, raises `ConfigError`. Attribute access
reads what is bound (`source.connection`), and assignment rebinds atomically.

Once every explicit target is in place, the component checks itself: a non-optional relation that
is unbound and cannot fill itself is a `ConfigError`.

## Reading through the source

An asset never holds a copy of its source's bindings: a relation it leaves unbound reads the
source's targets under the same name, keeping only those its own relation accepts. A binding the
asset holds itself always wins.

```py
shop = Shop(connection=ShopConnection(api_key="..."), destinations=[warehouse])
shop.revenue.connection              # the source's connection instance
shop.revenue.bound("connection")     # None: the asset holds none of its own
```

Because nothing is copied, rebinding the source, or reconfiguring it, reaches its assets at once:

```py
staging = shop(connection=ShopConnection(api_key="..."))    # a copy; its assets read the new one
```

Components a source does not own still receive its bindings by trickling: a source fills the
unbound relations of its destinations, and a [job](jobs.md) those of its targets, by relation
name, on every `bind()`.

## Describing a resource

`ReportingConfig.definition()` returns a `ResourceDefinition` with the JSON Schema of the
user-facing fields (`config_schema`) and, in `relations`, one entry per declared relation with
its `kind`, `key`, `many`, `optional` and `on_delete`. Framework fields (`id`) and anything listed
in the class's `internal_fields` are stripped from the schema. Resources are marked `sensitive`
by default, which tells the platform to encrypt their stored configuration.

The [field helpers](fields.md) decide how each field is rendered in a form.
