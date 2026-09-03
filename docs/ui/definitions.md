# Generated from definitions

The web UI contains no per-connector code. Every tile, wizard step, form field and status column
is derived from the same component definitions the framework builds for the
[catalog](../guide/catalog.md). Add a source or a connection to a package, register it with an
entry point, and the UI knows how to list, create, configure, check and run it.

This page follows the data from a component class to the screen.

## From class to catalog

Every component class describes itself through `definition()`: kind, key, import path, display
name, icon, tags, the docstring as description, the JSON Schema of its configuration fields, the
JSON Schema of its state model, and its relation vocabulary. The API serves the catalog as JSON:

| Endpoint | Returns |
|----------|---------|
| `GET /api/catalog/` | Every enabled definition, keyed by component key. |
| `GET /api/catalog/{key}` | One definition. |
| `GET /api/catalog/kind/{kind}` | Definitions of one kind. |
| `GET /api/catalog/resource-kinds` | The resource kinds present (`connection`, `config`, …). |
| `GET /api/oauth/providers` | Which OAuth providers have in-house app credentials configured. |

The app loads the catalog once and reads everything below from it. The sidebar's "Entities"
section is built from the resource kinds: a package that defines a new resource kind gets its own
page without any UI change.

## Tiles

Type pickers render one tile per definition, grouped by the first tag when definitions carry
tags. The tile shows `icon`, `name` and the first line of the docstring, and search matches
name, key, description and tags.

```py
@il.source(name="Facebook Ads", icon="logos:facebook", tags=["Advertising"])
class FacebookAds(il.Source):
    """Campaigns, ads and their daily statistics from the Marketing API."""
```

renders as an "Advertising" tile labelled "Facebook Ads" with the Facebook logo.

## Wizard steps

Creating or editing any component runs through one definition-driven stepper. The steps come
from the definition:

| Step | Derived from |
|------|--------------|
| Type | The kind's definitions, when several classes exist and none is pinned. |
| Assets (sources only) | `SourceDefinition.assets`: one row per asset with its schema and partitioning; the selection becomes the source's children. Cross-source `requires` are resolved against existing sources. |
| One step per relation type | `relations` entries the page asks for, such as a job's `target` or a hook's `watch`, restricted to the `kinds` the relation allows. |
| One step per resource slot | `relations.resource.slots`: each slot names a resource key; the step lists existing instances of that key and offers to create one inline. |
| Destination (sources only) | `relations.destination.keys`: the destination classes the source allows. |
| Details | Name plus the form generated from `config_schema`. |

Relation semantics are enforced the same way when the platform stores an edge: `kinds`, slot
requirements and the `on_delete` / `on_unbind` policies described in the
[component model](../extending/components.md#relations).

## Forms

The details step, the inline resource creation and the edit drawer all render `SchemaForm`, a
generic form over `config_schema`. Widgets resolve from the field helpers' extensions first and
from the JSON Schema type otherwise:

| Field helper | Extension | Widget |
|--------------|-----------|--------|
| `InputField` | `x-widget: text` | Text input. |
| `SecretField` | `x-widget: password` | Masked input with a reveal toggle. |
| `TextField` | `x-widget: textarea` | Multi-line textarea. |
| `JsonField` | `x-widget: json` | Monospace editor; the value is parsed as JSON when it is valid. |
| `CronField` | `x-widget: cron` | Expression input with presets and a human-readable rendering in the sibling timezone. |
| `TimezoneField` | `x-widget: timezone` | Searchable IANA picker, defaulting to the user's profile timezone. |
| `SelectField(options=...)` | `x-widget: select`, `x-options` | Dropdown of fixed options. |
| `SelectField(options_from="destinations")` | `x-options-from` | Dropdown fed by the components configured in the same wizard; hidden when there are none. |
| `FetchField(provider=...)` | `x-widget: fetch`, `x-fetch` | Dropdown whose options are fetched from the service (below). |
| plain `bool` field | | Switch. |
| plain `int` or `float` field | | Number input honouring `ge` and `le`. |
| plain `list[str]` field | | Tag input, or multi-select when items are an enum. |
| `Enum`-typed field | | Dropdown of the enum values. |

Presentation keywords carry over: `label` is the form label, `description` the help line under
it, `info` a tooltip next to the label. `required` follows the pydantic definition, and the form
is only submittable when every required field has a value.

### Fetched options

A `FetchField(provider="connection.accounts")` renders as a dropdown that fills itself once the
`connection` slot is chosen. The form posts the component key, the field name and the slot's
credentials to `POST /api/components/resolve`; the API reads the provider reference from its own
copy of the schema (never from the client), instantiates the resource, calls the method, and
returns the items. Only methods marked `@fetch_field_provider` may be called this way. Until the
slot is filled, the field degrades to a text input.

### OAuth sign-in

A connection with an `OAuthConfig` carries an `x-oauth` extension at the root of its schema. When
the provider's in-house credentials are configured on the server, the form shows two tabs:
"Sign in with X", which hides the mapped credential fields and shows a sign-in button, and
"Manual", which shows every field. After sign-in the returned token lands in the field mapped to
the `refresh_token` role; the client id and secret are resolved on the server and never reach
the browser.

### Instance names

The field marked `discriminator=True` (`x-discriminator`) names the instance: the wizard proposes
the selected option's label (an ad account name rather than its id) as the component name, and
the platform derives the per-instance table names from the same value.

## State columns

A kind with a `state_model` publishes `state_schema`. List pages add one column per property:
jobs show `next_run_at` and `last_run_at`, connections show `next_renewal_at`, `last_renewed_at`
and `last_renewal_error`, hooks show `last_fired_at`. Properties flagged `x-hidden` are plumbing
and stay out of the table.

## Actions the definition advertises

| Definition field | UI |
|------------------|----|
| `ResourceDefinition.checkable` | A "Test connection" action calling `POST /api/components/check`, which runs the class's `check()` against the unsaved form values and surfaces `ConnectionCheckError` messages under the fields. |
| `ResourceDefinition.renewable` | A "Renew now" action and the `auto_renew` toggle; both disappear for classes without a renewal flow. |
| `AssetDefinition.partitioning` | Partition pickers on manual runs and backfills, with the granularity's key shape (`2026-08`, `2026-08-21`, …). |
| `AssetDefinition.asset_schema` | The column list shown for each asset. |

## Drift

Persisted components reference catalog keys. When a key no longer resolves, because the class
was renamed or removed (`missing`) or is not enabled in this deployment's catalog (`disabled`),
the component is shown with a drift status instead of failing to load, and the Sources page
offers a confirmed cleanup. See `ComponentDriftError` in [Errors](../reference/errors.md).

## What a package author gets

Writing this, and nothing else:

```py
@il.connection(name="Shop API", icon="carbon:shopping-cart", tags=["Commerce"])
class ShopConnection(il.Connection):
    api_key: str = il.SecretField(description="API key from the shop admin")

    @il.fetch_field_provider
    async def stores(self) -> list[dict]: ...

    async def check(self) -> bool: ...


@il.source(name="Shop", icon="carbon:shopping-cart", tags=["Commerce"], resources={"connection": ShopConnection})
class Shop(il.Source):
    """Orders and customers from the shop API."""

    store_id: str = il.FetchField(provider="connection.stores", label_key="name", value_key="id", discriminator=True)
    ...
```

gives the UI: a "Commerce" tile for the source and the connection; a connection form with a
masked key and a working "Test connection" button; a source wizard whose connection step lists
existing Shop API connections or creates one inline, whose asset step lists the source's assets
with their schemas, and whose details step shows a store dropdown filled from the API; an
instance named after the chosen store; and a table per store in the destination. Every later
change to the class flows through on the next deploy.
