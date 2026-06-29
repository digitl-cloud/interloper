# REST & Pagination

Most connectors fetch data from paginated REST APIs. Interloper ships a small, composable REST
toolkit — an HTTP client, pluggable authentication, and a set of paginators — so connectors can
page through an endpoint without hand-rolling the loop. It is built on
[httpx](https://www.python-httpx.org/).

## Clients

`il.RESTClient` (sync) and `il.AsyncRESTClient` (async) share the same constructor:

```py
import interloper as il

client = il.AsyncRESTClient(
    "https://api.example.com",
    auth=il.HTTPBearerAuth("my-token"),
    timeout=30.0,
    headers={"Accept": "application/json"},
    params={"locale": "en"},
)
```

Any extra keyword arguments are forwarded to the underlying `httpx.Client` /
`httpx.AsyncClient`.

## Paginating

`paginate()` yields one page of data at a time. Give it a path, a paginator, and a
`data_selector` describing where the records live in each response:

```py
paginator = il.OffsetPaginator(limit=50, total_path="meta.total")

async for page in client.paginate("/users", paginator, data_selector="data"):
    for row in page:
        ...
```

The sync `RESTClient.paginate()` returns an iterator; the async `AsyncRESTClient.paginate()`
returns an async iterator. For range-style paginators (those that know the total up front), the
async client fetches pages concurrently (bounded by `concurrency`, default 8).

### Data selectors

`data_selector` extracts the records from a response:

| Value | Behaviour |
|-------|-----------|
| `None` | `response.json()` (the whole body) |
| `"data"` | `response.json()["data"]` |
| `"items.list"` | nested lookup `response.json()["items"]["list"]` |
| `lambda r: r.json()["rows"]` | custom callable |

## Authentication

Pass one of the auth helpers as `auth=`:

```py
il.HTTPBearerAuth(token)

il.OAuth2ClientCredentialsAuth(
    base_url, client_id, client_secret,
    scope=None, token_endpoint="/oauth2/token",
)

il.OAuth2RefreshTokenAuth(
    base_url, client_id, client_secret, refresh_token,
    scope=None, token_endpoint="/oauth2/token",
)
```

`OAuth2ClientCredentialsAuth` and `OAuth2RefreshTokenAuth` obtain and refresh access tokens
automatically. `il.OAuth2Auth` is the shared base if you need a custom grant.

## Paginators

Pick the paginator that matches the API's pagination scheme:

| Paginator | Scheme |
|-----------|--------|
| `il.SinglePagePaginator()` | No pagination — one request |
| `il.PageNumberPaginator(page_param="page", base_page=1, total_path=None, ...)` | `?page=1`, `?page=2`, … |
| `il.OffsetPaginator(limit, offset_param="offset", limit_param="limit", total_path=None, ...)` | `?offset=0&limit=100`, … |
| `il.HeaderLinkPaginator(rel="next")` | RFC 5988 `Link` header |
| `il.JSONLinkPaginator(next_url_path)` | Next-page URL at a JSON path |
| `il.JSONCursorPaginator(cursor_path, cursor_param)` | Cursor carried from response to next request |
| `il.RangePaginator` | Base for paginators that know the total up front (enables concurrent fetch) |

`PageNumberPaginator` and `OffsetPaginator` accept `total_path` (a JSON path to the total count)
and stop on an empty page by default (`stop_on_empty=True`).

## Putting it together

A connection that exposes a configured client, consumed by an async asset:

```py
from functools import cached_property
import interloper as il

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
        rows: list[dict] = []
        paginator = il.PageNumberPaginator(total_path="meta.total")
        async for page in connection.client.paginate("/users", paginator, data_selector="data"):
            rows.extend(page)
        return rows

    return [users]
```

See [Connections](connections.md) for wiring credentials and [OAuth](oauth.md) for the sign-in
flow.
