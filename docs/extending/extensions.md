# Extensions & registries

Every package says what it brings in one YAML manifest, `extension.yaml`, at the root of its
import package. Installing the package is all the registration there is: no import-order
dependence, no registration calls, and discovery works in every process where the package is
present.

## The manifest

In a project made with `uv init --lib`, with `interloper-app` installed, `uv run interloper
extension init` writes both, the manifest declaring no section yet, and `uv run interloper
extension add <part>` adds each part below, its code and its entry.

A package names its import package under the `interloper.extensions` entry point, the one entry
point interloper reads, and ships the manifest inside that package so it lands in the wheel:

```toml
[project.entry-points."interloper.extensions"]
acme = "acme_interloper"
```

```yaml
# acme_interloper/extension.yaml
manifest_version: 1
key: acme
name: Acme
description: "Acme sources, a Polars representation and a Ray runner."
components: acme_interloper
representations:
  - acme_interloper.polars:POLARS_REPRESENTATION
oauth_providers:
  - acme_interloper.oauth:ACME
runners:
  ray: acme_interloper.runner:RayRunner
```

Five keys say what the package is:

| Key | Meaning |
|-----|---------|
| `manifest_version` | `1`, required. |
| `key` | The extension's identifier, required: lowercase letters, digits and underscores, starting with a letter, and unique among the installed packages. |
| `name` | Its display name. |
| `description` | One sentence on what it brings. |
| `icon` | Its icon. |

Its version is its distribution's. Every other top-level key is a **section**. A section is read
by the registry built for it, wherever that registry lives: core knows nothing of the sections
the platform reads, and a section no registry reads is ignored. Name a section an extension of
yours reads after your extension's key (`acme_widgets`), so it can never collide with another.

A section holds import strings, `module` or `module:attribute`, in one of three shapes:

- a **map** of names to import strings, for a registry that looks entries up by the name you
  declare (`runners`, `launchers`, `cli`, `settings`);
- a **list** of import strings, for a registry that keys an entry by the object itself
  (`kinds`, `representations`, `oauth_providers`, `components`);
- a **single** import string, named by the extension's key.

Nothing is imported when the manifest is read: an entry is imported when its registry first
needs it.

## The sections

| Section | Declares | Entry | Read by |
|---------|----------|-------|---------|
| `kinds` | a component kind | the anchor class | `il.KINDS`; catalog validation |
| `components` | component classes | a class, or a module whose component classes are all declared | `il.Catalog.discover()` |
| `representations` | a table representation | an instance or class | `TableData.from_data()`, `il.Upstream.rows` |
| `runners` | a runner | the class | `il.Runner.from_settings()`, `interloper.yaml` `runner.type` |
| `oauth_providers` | an OAuth provider | an instance or class | `interloper.oauth.PROVIDERS`, `OAuthConfig` |
| `cli` | an `interloper` command | the `Command` class | the `interloper` CLI |
| `launchers` | a launcher | the class | the scheduler's queue worker |
| `settings` | a settings section | the `Settings` class | its own `get()`; the admin configuration page; the settings reference |
| `routes` | HTTP routes | a FastAPI `APIRouter` | the API, which mounts it under `/api/organisations/{org_id}/ext/<name>` |
| `ui` | a prebuilt UI | an `interloper_api.UIBundle` | the API, which serves it under `/api/ext/<name>/ui/<digest>/` |

Core declares its own kinds, jobs, hooks, runners, representation, providers, commands and
settings in its own manifest, the same way.

`interloper extension add source [name]` and `add connection [name]` write a source or a
connection in `<name>/source.py` or `<name>/connection.py` and list it under `components`.

## Settings

A `settings` entry names a subclass of `interloper.settings.Settings`, and the entry's name is
the section's: its block of `interloper.yaml` and the prefix of its environment variables.
`interloper extension add settings [name]` writes one in `settings/<name>.py` and declares it.

```yaml
settings:
  acme_widgets: acme_interloper.settings:WidgetSettings
```

```py
from typing import ClassVar

from interloper.settings import Settings
from pydantic import Field


class WidgetSettings(Settings):
    """Widget settings."""

    public: ClassVar[frozenset[str]] = frozenset({"size"})

    size: int = Field(default=1, description="Widget size.")
    token: str = Field(default="", description="Widget service token.")
```

`WidgetSettings.get()` is the section this process runs with, loaded on the first read from
`INTERLOPER_ACME_WIDGETS_SIZE` and the `acme_widgets` block, the environment winning. The class
finds its name by looking up its own import path, so declare it by the module that defines it.
Two packages declaring the same section, or one class declared under two names, fail every load
with an error naming both.

Each field's `description` is the settings reference's copy. `public` names the fields whose
values the admin configuration page shows; every other field shows only whether the deployment
set it, so a secret never leaves the process and a new field stays hidden until listed. A dotted
name (`config.image`) shows one key of a mapping field.

## Commands

A `cli` entry names a subclass of `interloper.cli.Command`, and the entry's name is the
command's (`interloper extension add command [name]` writes one in `commands/<name>.py` and
declares it):

```yaml
cli:
  backup: acme_interloper.cli:BackupCommand
```

```py
import argparse

from interloper.cli import Command


class BackupCommand(Command):
    """Back up the database.

    Writes a dump of every table to the given directory.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("directory", help="Where the dump is written")

    def run(self, args: argparse.Namespace) -> None:
        ...
```

The docstring is the command's copy: the whole docstring is the description `interloper backup
--help` shows, and its first line the help a group lists the command with. `add_arguments`
declares the arguments on the command's `argparse` parser and `run` receives them parsed.
`interloper --help` lists the installed commands by name without importing any; `interloper
backup --help` imports the command's module and its package's `__init__`, so keep heavy imports
inside `run`, out of both.

A group lists its commands, each named the same way, and prints its help when named alone:

```py
from collections.abc import Mapping
from typing import ClassVar


class StorageCommand(Command):
    """Storage operations."""

    commands: ClassVar[Mapping[str, type[Command]]] = {"backup": BackupCommand, "restore": RestoreCommand}
```

## Routes

A `routes` entry names a FastAPI `APIRouter`. Declared as a single import string, it is named
by the extension's key, and the API mounts it under the organisation the request acts in
(`interloper extension add routes` writes `routes.py` with one route and declares it):

```yaml
routes: acme_interloper.routes:router
```

```py
from uuid import UUID

from fastapi import APIRouter
from interloper_api.dependencies import EditorDep, StoreDep, ViewerDep, load_authorized

router = APIRouter()


@router.get("/buckets/{bucket_id}")
def get_bucket(bucket_id: UUID, store: StoreDep, scope: ViewerDep) -> dict:
    bucket = load_authorized(store.components.get, bucket_id, scope, label="Bucket")
    return {"id": str(bucket.id), "name": bucket.name}


@router.post("/buckets")
def create_bucket(scope: EditorDep) -> dict:
    ...
```

`GET /api/organisations/{org_id}/ext/acme/buckets/{bucket_id}` reaches `get_bucket`. Every
route of the router requires the viewer role in the organisation the path names, so a caller
outside it reads the organisation as missing; a route that writes raises the floor with
`EditorDep` or `AdminDep`. The scope also names the caller, `scope.credential.user`, read in
the same statement as the organisation and the role. Keeping one organisation's data from
another is the router's job: every row it stores carries the organisation's id, and a fetch by
id goes through `load_authorized`, which answers a record of another organisation with the
same 404 as a missing one.

The routes reach Python in every deployment shape, since only `/api` goes to the API, and the
package declaring them must be installed in the API's image.

## UI

A `ui` entry names a `UIBundle`: the output directory of the extension's build, whatever built
it, shipped inside its package. The host imports the entry, an ES module, and adds the
stylesheets beside it. `interloper extension add ui` writes one built with the SDK below (one
page reading a route, one sidebar entry, routes added first when there are none), and
`interloper extension build` builds it into the wheel.

```yaml
ui: acme_interloper.ui:UI
```

```py
# acme_interloper/ui.py
from pathlib import Path

from interloper_api import UIBundle

UI = UIBundle(Path(__file__).parent / "dist", entry="index.js", css=["index.css"])
```

`entry` defaults to `index.js` and `css` to none, for an entry that adds its own styles. A
Vite build names them itself: declared with `UIBundle.from_vite(Path(__file__).parent /
"dist")`, the bundle reads its entry and the stylesheets of the entry and of every chunk it
imports from the build's Vite manifest (`build.manifest: true`), so the entry may carry a
content hash in its name. Either way, a package installed without its build fails to load and
shows as failed, like any other entry.

Whatever the bundler, the chunks the entry loads must be addressed relative to it (Vite's
`base: './'`), since the bundle is served under a path of its own: `/api/ext/acme/ui/<digest>/`,
`<digest>` being the hash of every file of the directory. A rebuild that changes any file
changes every URL, so each file is cached for good whatever its name. The files are served to
anyone, as the SPA's own are: they hold code, and the data the code reads goes through the
authenticated routes. `/api` is the one prefix that reaches the API in every deployment shape
(the dev server's proxy, the single-process app, nginx), so the bundle is served the same way
in each, and the package must be installed in the API's image.

### What the UI runs against

The app loads every extension's UI before its first navigation. An extension's code imports
what it shares with the app by module name, and the page's import map resolves each name to
the app's own instance, so an extension runs on the app's Vue, router, Pinia, API client,
realtime connection, stores and theme rather than copies of its own. Leave these modules out of
the bundle (`external` in any bundler):

| Module | What it is |
|--------|------------|
| `vue`, `vue-router`, `pinia`, `@vueuse/core` | the packages, as the app runs them |
| `@iconify/vue` | the icon store `UIcon` renders from: `addCollection` registers an extension's own icons |
| `@interloper/host` | the app itself: `defineExtension`, `useExtension`, `useApi`, `useOrgApi`, `useRealtimeSubscription`, `useToast`, `useConfirm`, the organisation, user, catalog and components stores, `errorToast`, `componentIcon`, the `AppNavbar`, `CardHeader`, `DataTable`, `LoadError` and `StatusBadge` components and a set of Nuxt UI components (`UButton`, `UCard`, `UDashboardPanel`, `UTable`, `UModal`, `UForm`, ...) that render with the app's theme |

The entry module's default export declares the extension. Its `setup` receives the extension's
host, which adds its pages and its sidebar entries:

```js
// dist/index.js
import { defineExtension } from '@interloper/host'

export default defineExtension({
    setup(host) {
        host.addRoute({ path: '', name: 'buckets', component: () => import('./pages/buckets.js') })
        host.addRoute({ path: 'buckets/:id', name: 'bucket', component: () => import('./pages/bucket.js') })
        host.addNavDestination({ label: 'Buckets', icon: 'i-lucide-boxes', path: '', keywords: ['matcher'] })
    },
})
```

A page lives inside the organisation, under `/<organisation>/ext/<key>/`: `/acme/ext/acme` and
`/acme/ext/acme/buckets/<id>` above, with the organisation resolved and the catalog loaded
before it renders, like the app's own pages. It renders its own frame, a `UDashboardPanel` with
`AppNavbar` in its header, and reaches the rest of its extension through `useExtension()`,
without repeating its key:

```js
import { useRouter } from 'vue-router'
import { useExtension } from '@interloper/host'

const router = useRouter()
const { apiFetch, path } = useExtension()
const buckets = await apiFetch('/buckets')      // GET /api/organisations/<id>/ext/acme/buckets
router.push(path(`buckets/${buckets[0].id}`))   // /<organisation>/ext/acme/buckets/<id>
```

`apiFetch` and `fetchAll` behave as `useOrgApi`'s do, below the extension's routes; `path` takes
a page's path as `addRoute` does. A sidebar entry, its `views` a submenu, follows the app's own
and is found by the command palette under its label and keywords. An extension that fails to
load is reported in a toast and skipped; the app and the other extensions carry on.

### Building it with the SDK

`@interloper/ui` (`packages/platform/interloper-app/sdk/`) builds an extension's UI the way the
app is built: Vue single-file components, Nuxt UI components used in templates without an
import, Tailwind classes that resolve to the app's theme, and types for `@interloper/host`. The
repository's pnpm workspace holds the app, the SDK, every `packages/extensions/*/ui` and the
examples' UIs; a first-party extension keeps its UI there, beside its Python package.
`examples/extension/` is a complete one, an API route and a page:

```
examples/extension/
  pyproject.toml
  src/interloper_example/
    extension.yaml         routes and ui entries
    routes.py
    ui.py                  UI = UIBundle.from_vite(Path(__file__).parent / "static")
    static/                the UI's build, gitignored
  ui/
    package.json           devDependencies: @interloper/ui, vite, vue, typescript, vue-tsc
    vite.config.ts         export default interloper({ outDir: '../src/interloper_example/static' })
    tsconfig.json          { "extends": "@interloper/ui/tsconfig", "include": ["src"] }
    src/index.ts           export default defineExtension({ setup(host) { ... } })
    src/main.css           @import "@interloper/ui/tailwind.css";
    src/pages/Greeting.vue
```

`interloper()` is the whole Vite configuration: it leaves the shared modules to the import map,
resolves the components `@interloper/host` exports (`UButton`, `UCard`, `AppNavbar`, ...) where
a template uses them, compiles Tailwind and writes the bundle with its manifest and relative
chunks into `outDir`, for `UIBundle.from_vite`. The Tailwind entry adds utilities only: the
app's page already carries the preflight and the theme, so `text-muted` resolves to
`var(--ui-text-muted)` and nothing in the extension's stylesheet redefines a variable the app
sets.

The SDK derives what it knows of the app from the app itself, so it is built after the app is
prepared, and again when `host.ts` or the theme changes:

```sh
pnpm install                                # the workspace; prepares the app
pnpm --filter @interloper/ui build          # @interloper/host's types, its components, the theme
pnpm --filter @interloper/example build     # the UI, into the Python package
pnpm --filter @interloper/example typecheck
```

The types are the app's own declarations of `host.ts` and every file it reaches, so a page is
checked against the stores, composables and component props the app runs; a declaration error
in any of those files fails the SDK's build. The components `@interloper/host` exports are the
contract: one it does not export cannot be used, since a copy bundled with the extension would
miss the app's theme. Exporting every Nuxt UI component would add about 450 KB, gzipped, to
every page that shows an extension, so each is added to `host.ts` when an extension needs it.

`make build-extensions` runs all of it for every UI in the workspace, and
`make check-typescript` builds the SDK and type-checks it and every UI, as CI does. A package's
wheel carries whatever its `static/` holds, so the release runs `make build-extensions` before
it builds the wheels, and the loaded api image builds the `packages/extensions` UIs into the
packages it installs.

That is how the repository's own extensions build, through the workspace and the Makefile;
[Writing an extension in its own repository](own-repository.md) walks through the other path. An
extension in its own repository installs `@interloper/ui` from npm instead, and
`uv run interloper extension build` runs npm in its `ui/`, type-checks and builds the UI, then
builds the wheel and checks that it carries the bundle; `make check-extension` runs that whole
path in a fresh project against the SDK packed as npm publishes it, as CI does.

Such an extension runs inside the real app with `uv run interloper dev`, from its project. It
serves the platform on `127.0.0.1:3000` (`--port` to change it) against the Postgres its
`interloper.yaml` names, and prints a sign-in link that needs no login provider: it signs in a
local developer, super-admin of an organisation named after the project, and lands on the
extension's page. The UI comes from its Vite dev server, built or not: a stylesheet edit
applies in place, a `.vue` edit reloads the page, and an edit to a Python file, the manifest or
`interloper.yaml` restarts the platform. Class paths given as arguments join the catalog, so a
plain project serves its sources the same way (`uv run interloper dev weather.OpenMeteo`).

The collection can come from files instead of the app. An instance path (`uv run interloper
dev weather.paris`) declares that instance and what it binds, and `-f collection.yaml`
(repeatable) declares each document of a spec file, the format `interloper run -f` reads.
Either puts `dev` in file mode: the collection is served from those files alone, from a database
of its own beside the configured one (one per checkout), and the app shows it read-only, saying
which files serve it. Runs, backfills and cancels work as usual. Every start writes the files'
components as they are: an edit updates a component in place, its id and run history kept (give
a spec component an `id` to keep them across a reordering of the file), and a component the
files stop declaring is deleted. The files hold everything, credentials included (as
environment references), since nothing made in the app joins a collection served from files.
Without them, `dev` serves the collection you build in the app. `uv run interloper run
weather.paris` runs the same instance in the terminal.

To work on a UI with hot reload, install its Python package editable and run `make dev-up`:
`interloper dev` serves every extension UI the project holds from its own Vite dev server
(`uv pip install -e examples/extension` for the example, which is not a workspace package and is
never published). The app then imports the extension's entry from that server instead of its
bundle, built or not, and an edit to a page, a class or a stylesheet applies in place. Only the
processes `interloper dev` starts learn the servers' URLs, so no deployment can be made to import
code from elsewhere.

## The installed packages

`GET /api/extensions` lists the installed packages that ship a manifest, the platform's own
included, for any signed-in user: each one's key, name, description, icon, version,
`status`, `ok` when everything it declares loaded and `failed` otherwise, and `ui`, the URLs of
its UI's entry module and stylesheets, or `null`. The admin
configuration page lists the same packages with what failed to load and why.

## When something does not load

One package never takes the others down. A manifest that cannot be read, a section of another
shape and an entry that fails to import are each skipped with a warning naming the package,
section and entry, and recorded in `interloper.extension.MANIFESTS.failures`. When two packages
declare the same name in a section, the first one found wins and the other is skipped with a
warning.

`interloper extension check`, which `interloper-app` installs, imports every entry of every
installed manifest and names each manifest, section or entry that fails, and each section no
registry reads, which is usually a misspelt section name. It exits non-zero when it names
anything:

```sh
$ interloper extension check
package 'acme_interloper', section 'runner': no registry reads this section
Checked 48 entries in 8 sections: 1 problem(s).
```

## The registry primitive

Every registry is an instance of `il.Registry`: a lazily-populated name-to-object mapping,
optionally fed by one manifest section:

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

A package that wants extensions of its own builds one, naming its section:

```py
from interloper.registry import Registry

WIDGETS: Registry[type[Widget]] = Registry("acme_widgets")
```

Registration is first-wins and idempotent. Loading happens once, on first lookup, serialized
across threads so concurrent first lookups never observe a half-populated registry, and a failed
read retries on the next lookup. An `adopt` transform lets a registry key entries by the object's
own `key` rather than the declared name, which is why providers and representations are found
under their declared keys.

Code can also register explicitly, for tests or dynamic composition:

```py
from interloper.runner import RUNNERS
RUNNERS.register("fake", FakeRunner)
```

## Kinds versus components

`kinds` says what kinds exist; `components` says what classes exist. Kind anchors are framework,
not content: they live in `KINDS` and never appear in a catalog. A component class whose kind has
no anchor makes catalog construction fail with `ConfigError`.

## Narrowing

Declared universe and enabled catalog are different things. Installation declares; the
`catalog.include` setting enables a subset, closed over dependencies. See
[Catalog](../guide/catalog.md#building-a-catalog).
