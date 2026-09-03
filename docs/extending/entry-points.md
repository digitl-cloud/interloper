# Entry points & registries

Interloper discovers what is installed through Python entry points. Installing a package that
declares them is all the registration there is: no import-order dependence, no registration
calls, and discovery works in every process where the package is present.

## The groups

| Group | Declares | Entry value | Consumed by |
|-------|----------|-------------|-------------|
| `interloper.kinds` | a component kind | the anchor class | `il.KINDS`; catalog validation |
| `interloper.components` | component classes | a class, or a module to scan | `il.Catalog.discover()` |
| `interloper.representations` | a table representation | an instance or class | `Representation.of()`, `DatabaseDestination.read_representation` |
| `interloper.runners` | a runner | the class | `il.Runner.from_settings()`, `interloper.yaml` `runner.type` |
| `interloper.oauth_providers` | an OAuth provider | an instance or class | `interloper.oauth.PROVIDERS`, `OAuthConfig` |

Platform packages add groups of their own (`interloper.launchers`).

A complete example for a package shipping a source, a connection, a destination, a
representation and a provider:

```toml
[project.entry-points."interloper.components"]
acme = "acme_interloper"                          # every component class in the module

[project.entry-points."interloper.representations"]
polars = "acme_interloper.polars:POLARS_REPRESENTATION"

[project.entry-points."interloper.oauth_providers"]
acme = "acme_interloper.oauth:ACME"

[project.entry-points."interloper.runners"]
ray = "acme_interloper.runner:RayRunner"
```

## The registry primitive

Every registry is an instance of `il.Registry`: a lazily-populated name-to-object mapping,
optionally fed by an entry-point group:

```py
import interloper as il
from interloper.oauth import PROVIDERS
from interloper.representation import REPRESENTATIONS
from interloper.runner import RUNNERS

il.KINDS["connection"]          # the anchor class; KeyError with the available names on a miss
PROVIDERS.get("google")         # None on a miss
"dataframe" in REPRESENTATIONS
RUNNERS.keys(), RUNNERS.items(), len(RUNNERS)
```

Registration is first-wins and idempotent. Loading happens once, on first lookup, serialized
across threads so concurrent first lookups never observe a half-populated registry, and a failed
load retries on the next lookup. An `adopt` transform lets a registry key entries by the object's
own `key` rather than the entry-point name, which is why providers and representations are
found under their declared keys.

Code can also register explicitly, for tests or dynamic composition:

```py
from interloper.runner import RUNNERS
RUNNERS.register("fake", FakeRunner)
```

## Kinds versus components

`interloper.kinds` says what kinds exist; `interloper.components` says what classes exist.
Kind anchors are framework, not content: they live in `KINDS` and never appear in a catalog. A
component class whose kind has no anchor makes catalog construction fail with `ConfigError`.

## Narrowing

Declared universe and enabled catalog are different things. Installation declares; the
`catalog` setting enables a subset, closed over dependencies. See
[Catalog](../guide/catalog.md#building-a-catalog).
