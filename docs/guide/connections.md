# Connections

A connection is a [resource](resources.md) holding the credentials and client setup for an
external service. Beyond injection, it offers a health check, credential renewal, and the OAuth
sign-in machinery.

## Defining a connection

```py
from functools import cached_property

import interloper as il

@il.connection(name="Shop API", icon="carbon:shopping-cart", tags=["Commerce"])
class ShopConnection(il.Connection):
    base_url: str = il.InputField(default="https://api.shop.example")
    api_key: str = il.SecretField(description="API key")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(self.base_url, auth=il.HTTPBearerAuth(self.api_key))
```

Fields load from constructor arguments, `.env`, or environment variables (`API_KEY=...`, or
prefixed through `model_config`). `SecretField` marks the field as a password in forms.
Exposing the client as a `cached_property` means every asset sharing the connection shares one
configured client.

Decorator options: `key`, `name`, `icon`, `tags`, `oauth`.

## Using a connection

```py
@il.source(resources={"connection": ShopConnection})
class Shop(il.Source):
    @il.asset
    def orders(self, connection: ShopConnection) -> list[dict]:
        return connection.client.get("/orders").json()
```

The `resources=` declaration on the source is optional: the asset's annotation is enough to
create the slot. Declaring it on the source documents the dependency in the source's
definition and lets a source-level instance trickle to every asset.

### REST clients and pagination

`il.RESTClient` and `il.AsyncRESTClient` extend the httpx clients with `paginate()`:

```py
@il.asset
async def orders(self, connection: ShopConnection) -> list[dict]:
    rows: list[dict] = []
    paginator = il.PageNumberPaginator(total_path="meta.pages")
    async for page in connection.client.paginate("/orders", paginator, data_selector="data"):
        rows.extend(page)
    return rows
```

`data_selector` pulls the records out of each response: a dotted JSON path, a callable taking
the response, or `None` for the whole body. The async client fetches the remaining pages
concurrently when the paginator knows the total after the first response (page-number and
offset paginators with `total_path`), bounded by `concurrency` (default 8). Cursor and link
paginators walk sequentially.

| Paginator | Scheme |
|-----------|--------|
| `SinglePagePaginator()` | One request. |
| `PageNumberPaginator(page_param="page", base_page=1, total_path=None, maximum_page=None, stop_on_empty=True)` | `?page=1`, `?page=2`, … |
| `OffsetPaginator(limit, offset_param="offset", limit_param="limit", total_path=None, maximum_offset=None, stop_on_empty=True)` | `?offset=0&limit=100`, … |
| `HeaderLinkPaginator(rel="next")` | RFC 5988 `Link` header. |
| `JSONLinkPaginator(next_url_path)` | Next URL at a JSON path. |
| `JSONCursorPaginator(cursor_path, cursor_param)` | Cursor carried into the next request. |

Custom schemes subclass `BasePaginator` (sequential) or `RangePaginator` (page set known up
front, enabling concurrency).

Authentication helpers to pass as `auth=`:

| Helper | Behaviour |
|--------|-----------|
| `HTTPBearerAuth(token)` | `Authorization: Bearer` header. |
| `OAuth2ClientCredentialsAuth(base_url, client_id, client_secret, scope=None, token_endpoint="/oauth2/token")` | Acquires an access token with the client-credentials grant, refreshes on 401. |
| `OAuth2RefreshTokenAuth(base_url, client_id, client_secret, refresh_token, scope=None, token_endpoint="/oauth2/token")` | Same with the refresh-token grant. |
| `OAuth2Auth` | Base for other grants; override `grant_type`, `auth_data`, `auth_headers`. |

The OAuth2 helpers work with both the sync and the async client. For fan-out beyond
pagination, `il.bounded_gather(coros, limit=8)` awaits coroutines with a concurrency cap.

## Health check

Implement `check()` to verify credentials with the cheapest authenticated call. It may be sync
or async and must use lightweight HTTP, not a heavy SDK, because it may run inside the API
process:

```py
import httpx
from interloper.errors import ConnectionCheckError

@il.connection(name="Shop API")
class ShopConnection(il.Connection):
    ...

    async def check(self) -> bool:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/me", headers={"Authorization": f"Bearer {self.api_key}"})
        if response.status_code == 403:
            raise ConnectionCheckError("The key is valid but lacks the reporting scope.")
        return response.is_success
```

Return `True` when the connection works and `False` when it provably does not. Exceptions count
as failures; raise `ConnectionCheckError` to surface a curated message instead of the generic
categorisation. `ShopConnection.checkable()` reports whether the class overrides the hook, and
the definition advertises it as `checkable`.

## Credential renewal

Implement `renew()` for credentials that age out. It returns a `Renewal`: the fields to persist
(rotated credentials) and, optionally, the new validity in seconds:

```py
def renew(self) -> il.Renewal:
    token = exchange(self.api_key)
    return il.Renewal(fields={"api_key": token.value}, expires_in=token.expires_in)
```

`renewal_interval` (class attribute, default one day) sets the cadence when the provider reports
no validity; a reported validity schedules the next renewal at half of it, floored at fifteen
minutes. The `auto_renew` field lets a user switch scheduled renewal off per connection; it is
hidden from the form when the class cannot renew.

A connection is an [operation](../extending/operations.md): its `execute()` runs `renew()` under
a 60-second cap and hands the rotated fields and the next due time back as effects; `failure()`
turns any error into a short, credential-free message and a one-hour retry slot. The
`ConnectionState` model (`next_renewal_at`, `last_renewed_at`, `last_renewal_error`) is the
connection's machine-owned state. Acting on the schedule is the platform's job; the framework
only declares it.

## OAuth connections

Services behind OAuth2 subclass `il.RefreshTokenOAuthConnection` and attach an `OAuthConfig`:

```py
@il.connection(name="LinkedIn Ads", oauth=il.OAuthConfig("linkedin", scope="r_ads"))
class LinkedinAdsConnection(il.RefreshTokenOAuthConnection):
    pass
```

`RefreshTokenOAuthConnection` declares the credential trio `client_id`, `client_secret`,
`refresh_token`, all required. The first two are the **in-house app credentials**: they are
injected before validation from `INTERLOPER_LINKEDIN_CLIENT_ID` and
`INTERLOPER_LINKEDIN_CLIENT_SECRET` when the caller omits them, so the secret never travels
through a browser or per-connection storage. The refresh token is filled in by the sign-in flow.

Renewal is **derived**: a provider with a refresh flow plus a complete role mapping means the
connection renews with no code of its own. The provider builds the refresh request and parses
the response; a rotated refresh token lands back in the mapped field.

### Non-standard field names

When the service names its credentials differently, or uses a long-lived access token as the
refresh credential, subclass `il.OAuthConnection` directly, declare the fields, and map the
OAuth **roles** to them:

```py
from pydantic import model_validator

@il.connection(
    name="Facebook Ads",
    icon="logos:facebook",
    oauth=il.OAuthConfig(
        "facebook",
        scope="ads_read,ads_management",
        fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
    ),
)
class FacebookAdsConnection(il.OAuthConnection):
    access_token: str = il.SecretField()
    app_id: str = il.InputField(label="App ID")
    app_secret: str = il.SecretField()

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data):
        if isinstance(data, dict):
            cls.resolve_field(data, "app_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "app_secret", cls.env_credential("CLIENT_SECRET"))
        return data
```

`env_credential("CLIENT_ID")` reads `INTERLOPER_FACEBOOK_CLIENT_ID`; `resolve_field` fills a
blank field without masking a missing required value. `fields` drives the form as well: mapped
fields are hidden in sign-in mode, and the token from the sign-in response lands in the
`refresh_token` role's field.

`OAuthConfig` accepts `provider`, `scope`, `fields`, and `auth_url`, `label`, `icon` overrides
for providers outside the registry. The connection's definition carries an `x-oauth` extension
with these values so a UI can render the sign-in button. Providers themselves are covered in
[OAuth providers](oauth.md).

## Definitions

`ShopConnection.definition()` is a `ResourceDefinition` with `checkable`, `renewable`, the
OAuth `provider` when configured, and the config JSON Schema. Connections are `sensitive`
resources and their runs are neither billable nor traceback-capturing, because credential
exchanges embed secrets in URLs.
