# Resources

A **resource** is an injectable dependency that Interloper resolves and passes into asset
functions at run time. [Config](config.md) and [Connection](connections.md) are the two built-in
resource kinds, and you can define your own for any reusable service (a cache, an SDK client, a
warehouse handle, …).

## Defining a resource

Subclass `il.Resource` and add whatever fields and methods your assets need:

```py
import interloper as il

class MyCache(il.Resource):
    host: str = "localhost"
    port: int = 6379

    def get(self, key: str): ...
    def set(self, key: str, value): ...
```

## Injecting resources

Resources are bound to assets either by **type annotation** or by an explicit `resources`
mapping on the decorator:

```py
# By type annotation — inferred from the signature
@il.asset
def my_asset(cache: MyCache):
    return cache.get("data")

# By explicit mapping — name -> resource type
@il.asset(resources={"cache": MyCache})
def my_asset(cache: MyCache):
    return cache.get("data")
```

## Resolution cascade

When an asset runs, each resource is resolved in order:

1. An instance set on the **asset** (`my_asset(resources={"cache": MyCache(...)})`)
2. An instance set on the **source** the asset belongs to
3. **Auto-instantiated** from the declared resource type (loading any settings from the
   environment)

This means a resource "just works" from environment variables in development, while production
can inject fully configured instances at the source or asset level.

## Declaring resources on components

Destinations and sources can declare resource dependencies too. The cleanest way is a typed
class attribute, which the component framework turns into a resource slot automatically:

```py
class BigQueryDestination(il.Destination):
    connection: GoogleCloudConnection   # becomes a resource slot named "connection"
    ...
```

For an explicit, typed accessor you can use `il.ResourceRef`:

```py
class BigQueryDestination(il.Destination):
    connection = il.ResourceRef(GoogleCloudConnection)
    config = il.ResourceRef(BigQueryConfig, required=True)
```

## Field types

Resource fields use helpers that both set Pydantic defaults and describe how the field is
rendered in the connect/configure form (Interloper's web UI). They emit JSON-Schema extensions
the frontend reads.

| Helper | Renders as | Use for |
|--------|-----------|---------|
| `il.InputField()` | Text input | Plain string settings (IDs, hosts) |
| `il.SecretField()` | Masked password input | API keys, tokens, passwords |
| `il.TextField()` | Multi-line textarea | Long strings (queries, keys) |
| `il.JsonField()` | JSON editor | Object/dict settings |
| `il.SelectField(options=[...])` | Dropdown | A fixed set of choices |
| `il.FetchField(provider="...")` | Dynamic dropdown | Choices fetched live from the service |

```py
@il.connection(name="Amazon Ads")
class AmazonAdsConnection(il.OAuthConnection):
    region: str = il.SelectField(
        options=[{"label": "North America", "value": "na"}, {"label": "Europe", "value": "eu"}],
    )
```

### Fetched options

`SelectField(options_from="...")` resolves its choices from other configured components (e.g.
the configured destinations), while `FetchField` calls a method on a resource to fetch live
options at form-render time. The provider string is `"<slot>.<method>"`, and the target method
must be allow-listed with `@il.fetch_field_provider`:

```py
@il.connection(name="Facebook Ads", oauth=il.OAuthConfig("facebook", scope="ads_read"))
class FacebookAdsConnection(il.OAuthConnectionBase):
    access_token: str = il.SecretField()

    @il.fetch_field_provider
    async def accounts(self) -> list[dict]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://graph.facebook.com/v19.0/me/adaccounts",
                params={"access_token": self.access_token},
            )
        return resp.json()["data"]

@il.source(resources={"connection": FacebookAdsConnection})
def facebook_ads():
    @il.asset
    def campaigns(
        connection: FacebookAdsConnection,
        account_id: str = il.FetchField(provider="connection.accounts", label_key="name", value_key="id"),
    ):
        ...

    return [campaigns]
```

Only methods decorated with `@il.fetch_field_provider` may be invoked this way — it is the
allow-list that stops the browser from calling arbitrary attributes. Use
`il.is_fetch_field_provider(obj)` to check whether a callable is a provider.
