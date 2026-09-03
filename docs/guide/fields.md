# Configuration fields

Sources, destinations, connections, configs, jobs and hooks all have configuration fields. They
are pydantic fields, and a set of helpers annotates them with how a form should render them.
Every helper is a thin wrapper around `pydantic.Field`: all standard keywords (`default`,
`description`, `title`, `ge`, `default_factory`, …) pass through.

## Field helpers

| Helper | Renders as | Typical use |
|--------|-----------|-------------|
| `il.InputField()` | Text input | Ids, hosts, names |
| `il.SecretField()` | Masked input | API keys, tokens, passwords |
| `il.TextField()` | Multi-line textarea | Queries, PEM keys |
| `il.JsonField()` | JSON editor | Dict-valued settings |
| `il.CronField()` | Cron editor with presets | Schedules |
| `il.TimezoneField()` | Searchable IANA timezone picker | Timezones |
| `il.SelectField(options=[...])` | Dropdown with fixed options | Regions, modes |
| `il.SelectField(options_from="destinations")` | Dropdown fed by configured components | Picking one of the instance's destinations |
| `il.FetchField(provider="connection.accounts")` | Dropdown fetched from the service | Ad accounts, properties, projects |

```py
@il.source
class GoogleAnalytics(il.Source):
    property_id: str = il.FetchField(
        provider="connection.properties",
        label_key="displayName",
        value_key="name",
        description="The GA4 property to read",
        discriminator=True,
    )
    region: str = il.SelectField(
        default="eu",
        options=[{"label": "Europe", "value": "eu"}, {"label": "US", "value": "us"}],
    )
```

A field without a helper is still a configuration field; it renders with a generic widget.

## Presentation keywords

Every helper also accepts:

| Keyword | Effect |
|---------|--------|
| `label=` | The form label (sets the JSON Schema `title`). |
| `info=` | Longer help text shown in a tooltip, keeping `description` short. |
| `discriminator=True` | Marks the field whose value distinguishes instances of the component. See [Sources](sources.md#the-discriminator). |

## Fetched options

A `FetchField` resolves its options by instantiating the resource in the named slot from the
credentials the form already holds and calling a method on it. The method must be allow-listed
with `@il.fetch_field_provider`; only such methods may be invoked from a form:

```py
@il.connection(name="Google Analytics", oauth=il.OAuthConfig("google", scope="..."))
class GoogleAnalyticsConnection(il.RefreshTokenOAuthConnection):
    @il.fetch_field_provider
    async def properties(self) -> list[dict]:
        async with httpx.AsyncClient() as client:
            response = await client.get(ADMIN_URL, headers=self.headers)
        return response.json()["properties"]
```

The provider returns a `list[dict]`; the field picks `label_key` and `value_key` from each item.
The reference is `"<slot>.<method>"`, where `<slot>` is a resource slot of the component
declaring the field. Because the method may run inside the API process, which installs
connection classes without their heavy SDK extras, providers must use plain HTTP.

References are validated when the component's definition is built: an unknown slot, or a method
without the decorator, raises `TypeError` at catalog-build time rather than failing silently in
a form. `il.is_fetch_field_provider(obj)` tests whether a callable is a provider.

## What ends up in the config schema

`Component.config_schema()` is the JSON Schema of the component's user-facing fields. The
framework strips `id` and `resources`, plus anything the class lists in `internal_fields`
(sources hide `assets`, `destinations`, `normalizer`, `select`; assets hide `destinations`,
`normalizer`, `dependencies`). Field helpers add `x-widget`, `x-info`, `x-options`,
`x-options-from`, `x-fetch` and `x-discriminator` extensions that forms read. The schema is
part of every [component definition](catalog.md#definitions).
