# Connections

A **connection** holds the credentials and client setup for an external service. It is a
[resource](resources.md): you declare it once and Interloper injects it into the asset functions
that need it. Connections extend both Pydantic Settings (so values load from the environment) and
the resource system (so they can be injected and described to the UI).

## Defining a connection

Subclass `il.Connection`, or decorate a plain class with `@il.connection`:

```py
import interloper as il

@il.connection(name="My API", icon="icon:database", tags=["Integration"])
class MyConnection(il.Connection):
    host: str = "localhost"
    port: int = 5432
    username: str = il.InputField()
    password: str = il.SecretField()
```

Field helpers like `il.InputField` and `il.SecretField` declare how each field is rendered in
the connect form (a plain input vs. a masked secret). See [Resources](resources.md#field-types)
for the full set.

Values resolve from constructor arguments, a `.env` file, or environment variables — the same
precedence as any Pydantic Settings model.

## Injecting a connection into assets

Declare the connection as a typed parameter on the asset function. Interloper inspects the
signature, resolves the connection, and passes the instance in:

```py
@il.asset
def users(connection: MyConnection):
    return connection.client.get("/users").json()
```

A common pattern is to expose a ready-to-use HTTP client from the connection via
`functools.cached_property`, so every asset shares one configured client:

```py
from functools import cached_property

@il.connection(name="My API")
class MyConnection(il.Connection):
    api_key: str = il.SecretField()

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        return il.AsyncRESTClient(
            "https://api.example.com",
            auth=il.HTTPBearerAuth(self.api_key),
        )

@il.source(resources={"connection": MyConnection})
def my_source():
    @il.asset
    async def users(connection: MyConnection):
        async for page in connection.client.paginate("/users", il.PageNumberPaginator()):
            ...

    return [users]
```

See [REST & Pagination](rest.md) for building API clients.

## Providing a connection explicitly

Resolution follows a cascade — asset's own `resources`, then the source's, then
auto-instantiation from the environment. To supply one explicitly, pass it via `resources`
(keyed by the parameter name):

```py
source = my_source(resources={"connection": MyConnection(api_key="...")})
```

## OAuth connections

For services behind OAuth, base your connection on `il.OAuthConnection` and attach an
`il.OAuthConfig`. `OAuthConnection` declares the standard credential trio
(`client_id` / `client_secret` / `refresh_token`):

```py
@il.connection(
    name="LinkedIn Ads",
    oauth=il.OAuthConfig("linkedin", scope="r_ads"),
)
class LinkedinAdsConnection(il.OAuthConnection):
    account_id: str = il.InputField()
```

- The **app credentials** (`client_id`, `client_secret`) are resolved from
  `LINKEDIN_CLIENT_ID` / `LINKEDIN_CLIENT_SECRET` environment variables and never travel
  through the browser or per-connection storage.
- The per-user **refresh token** is filled in by the OAuth sign-in flow.

When a token's shape doesn't match the standard trio (e.g. a single long-lived `access_token`),
subclass `il.OAuthConnectionBase` instead and declare your own fields, mapping them in the
config's `fields`:

```py
@il.connection(oauth=il.OAuthConfig("facebook", scope="ads_read", fields={"access_token": "access_token"}))
class FacebookAdsConnection(il.OAuthConnectionBase):
    access_token: str = il.SecretField()
```

See [OAuth](oauth.md) for providers and the connect flow.
