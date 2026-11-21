# Writing an extension in its own repository

An extension is a Python package that adds to interloper: sources and connections, settings,
commands, API routes, a page in the app. This page builds one from an empty directory with the
packages PyPI and npm publish, runs it inside the real app while you edit it, and ships it as a
wheel. [Extensions & registries](extensions.md) explains what each part does once it is
installed.

## What you need

- [uv](https://docs.astral.sh/uv/), which runs every command below.
- A PostgreSQL you can create a database in: the app keeps its state there, also on your machine.
  `docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres` is enough.
- Node.js and npm, for a UI only.
- macOS or Linux. On Windows, work inside WSL.

## Create the project

An extension starts as an ordinary uv library, with `interloper-app` as its one development
dependency: it carries the platform you run locally and the `interloper extension` commands.

```sh
uv init --lib acme-interloper
cd acme-interloper
uv add --dev interloper-app
uv run interloper extension init
```

`init` makes the project an extension and declares nothing yet:

- `pyproject.toml` gains the `interloper.extensions` entry point naming the package, and
  `interloper-core` as a dependency;
- `src/acme_interloper/extension.yaml` is the manifest: the extension's key (`acme`), its name and
  description, and later every part it declares;
- `interloper.yaml` is the configuration every `interloper` command run in this directory reads:
  the Postgres to use and a key that encrypts what the local instance stores. It holds that key,
  so `init` adds it to `.gitignore`. Point its `postgres` section at your database.

## Add what it declares

`add` writes one part, its files and its manifest entry, and adds the dependency the part needs.
A name defaults to the extension's key.

```sh
uv run interloper extension add source weather
uv run interloper extension add connection weather
uv run interloper extension add settings
uv run interloper extension add command sync
uv run interloper extension add routes
uv run interloper extension add ui
```

| Part | Writes | Declares |
|---|---|---|
| `source` | `src/acme_interloper/weather/source.py`: a source and its assets | `components` |
| `connection` | `src/acme_interloper/weather/connection.py`: the credentials a source reads | `components` |
| `settings` | `src/acme_interloper/settings/acme.py`: a settings section `interloper.yaml` fills | `settings` |
| `command` | `src/acme_interloper/commands/sync.py`: an `interloper sync` command | `cli` |
| `routes` | `src/acme_interloper/routes.py`: API routes under `/api/organisations/{org_id}/ext/acme` | `routes` |
| `ui` | `ui/`, a Vue page built with `@interloper/ui`, and `src/acme_interloper/ui.py` declaring its build | `ui` |

`add ui` adds the routes when the extension has none, since its page reads them. Edit the files
as you like: the manifest names them, nothing else does.

## Run it in the app

```sh
uv run interloper dev
```

`dev` serves the project with the real app on `http://127.0.0.1:3000`, and prints a sign-in link
it opens in your browser: it needs no login provider, signs in a local developer and lands on
your extension's page, in an organisation named after the project. Keep it running while you
work:

- an edit to a Python file, the manifest or `interloper.yaml` restarts the platform;
- the UI comes from its own Vite dev server, built or not: a stylesheet edit applies in place and
  a `.vue` edit reloads the page.

`--port` serves on another port, and `--no-browser` only prints the link. A source class
can join the catalog without an extension, from any module: `uv run interloper dev weather.OpenMeteo`.
The collection can also come from files: an instance in a module (`uv run interloper dev
weather.paris`), or the documents of a spec file (`uv run interloper dev -f collection.yaml`).
Either serves the collection from those files alone, read-only in the app, from a database of its
own; without them, `dev` serves the collection you build in the app.
[Extensions & registries](extensions.md#building-it-with-the-sdk) says more about both.

## Build and ship it

```sh
uv run interloper extension build
uv run interloper extension check
```

`build` installs the UI's dependencies, type-checks and builds it into `src/acme_interloper/static/`,
then builds the sdist and the wheel into `dist/`, and checks that the wheel carries the UI.
`check` imports every entry of every installed manifest and names what fails or what nothing
reads. Publish the wheel the way you publish any package (`uv publish`); a deployment installs it
beside the platform, and its parts appear with no other change.
