# Running the app

The web UI is part of the platform: a FastAPI backend, a Postgres database, a scheduler, and the
Nuxt single-page app served as static assets. The core framework does not need any of it; the
platform needs the core.

## Packages

| Package | Role |
|---------|------|
| `interloper-db` | Persistence and migrations. |
| `interloper-api` | The HTTP backend the UI talks to. |
| `interloper-scheduler` | Cron controller, hook evaluator, credential renewal, queue worker, reaper, launchers. |
| `interloper-app` | The built SPA, served by the API process. |

## Images

Every release publishes one image per role to `ghcr.io/digitl-cloud`, plus a Helm chart that
deploys them. Each image comes in two variants:

| Tag | Carries |
|-----|---------|
| `interloper-<role>:<version>` | **Loaded**: the role's own packages, every component class a catalog can name (ready-made sources, BigQuery and GCS destinations, the Slack hook) and what the role runs on (both launchers on the scheduler, the agent on the api, telemetry). |
| `interloper-<role>:<version>-slim` | **Slim**: `interloper-core` plus the role's own packages, nothing optional. No sources, no destinations, no launcher beyond in-process, no telemetry SDK. |

The roles are `api`, `scheduler`, `core`, `mcp` and `frontend` (nginx, single variant). The
loaded images are what the Helm chart deploys and what a deployment configured from the
catalog wants: `launcher.type` and `agent.enabled` select between things already in the image,
so switching one is a restart and never a rebuild.

`core` is the framework itself, with no platform around it: the Kubernetes runner uses it as
the per-asset Job target, and `interloper-core:<version>-slim` is `interloper-core` and nothing
else, which makes it the base to build on when you ship your own components.

Two rules decide what a loaded image carries. Any role that builds a catalog must be able to
import every class the catalog can name, which is why the api and mcp ship sources,
destinations and hooks they never execute: they *describe* them, and a class that fails to
import is skipped with a warning rather than an error, so the answer would quietly be wrong.
The vendor SDKs (`bingads`, `facebook-business`, `google-ads`) are the exception, because the
asset modules guard those imports: only the roles that actually execute assets, `scheduler` and
`core`, carry them. That single distinction is why the api and mcp images are a fraction of the
size of the two that run assets.

## Extending an image

The slim images exist to be built on. Every package is on PyPI, so adding what a deployment
needs is an install in a child image:

```dockerfile
FROM ghcr.io/digitl-cloud/interloper-scheduler:<version>-slim

USER root
RUN pip --python /interloper/.venv/bin/python install --no-cache-dir \
      "interloper-assets[google]" \
      interloper-google-cloud \
      my-own-connectors
USER app
```

The `--python` flag matters, and it goes before the subcommand. The application lives in the
virtualenv at `/interloper/.venv`, which was built by uv and therefore has no `pip` of its own;
a bare `pip install` would resolve to the base image's interpreter instead and land the package
somewhere the app never looks, with no error to show for it.

Anything installed this way registers itself: a package declaring components under the
`interloper.components` entry point joins the catalog on the next start, with no change to the
image's command. List what you want enabled under `catalog` and the deployment sees exactly
that, as described in [Catalog](../guide/catalog.md) and
[Entry points & registries](../extending/entry-points.md).

The same recipe works on a loaded image when you want everything it ships plus your own
components.

## One process

```sh
interloper app --api --cron --worker --reaper
```

runs every service in one process against the configured Postgres, serving the UI on port 3000.
Each service can be toggled off, so the API and worker scale out while cron and reaper stay
singletons. `--dev` runs the Nuxt dev server with hot reload instead of the built assets.

## Configuration

`interloper.yaml` in the working directory, or `INTERLOPER_*` variables:

```yaml
postgres:
  host: localhost
  user: interloper
  database: interloper          # password: INTERLOPER_POSTGRES_PASSWORD

auth:
  google_client_id: ...
  google_client_secret: ...
  google_redirect_uri: https://app.example.com/api/auth/google/callback
  allowed_domains: [example.com]

catalog:
  - my_package.sources.Shop
  - interloper_assets.facebook_ads.source.FacebookAds
```

The file is not interpolated: a field left out of a block is read from its `INTERLOPER_<SECTION>_<FIELD>`
variable, which is where secrets go. `INTERLOPER_ENCRYPTION_KEY` is required: resource configs are
encrypted at rest. Login is Google OAuth. `allowed_domains` restricts who may sign up; `super_admin_emails`
bootstraps platform administrators. In-house OAuth app credentials for connectors live in
`INTERLOPER_<PROVIDER>_CLIENT_ID`, `_CLIENT_SECRET` and `_REDIRECT_URI`; a provider without them
has no sign-in tab, only manual credential entry. The full settings list is in
[Settings](../reference/settings.md).

## Database

```sh
interloper db init          # create the database, tables and run migrations; idempotent
interloper db upgrade       # migrate an existing database to head
```

`interloper app` also creates missing tables on startup unless `--no-create-tables` is passed.

## Local development

The repository ships a dev harness: `make dev` provisions a local Postgres database, seeds a
super-admin, an organisation, the demo source and a daily job, and starts every service with the
Nuxt dev server on port 3000. `make compose-up` does the same in Docker. The screenshots on the
[Tour](index.md) come from that harness with a few more demo components added.
