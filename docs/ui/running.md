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

Container images for each role and a Helm chart are published with every release; see the
repository README.

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
  password: ${POSTGRES_PASSWORD}
  database: interloper

secrets:
  encryption_key: ${INTERLOPER_ENCRYPTION_KEY}   # required: resource configs are encrypted at rest

auth:
  google_client_id: ...
  google_client_secret: ...
  google_redirect_uri: https://app.example.com/api/auth/google/callback
  allowed_domains: [example.com]

catalog:
  - my_package.sources.Shop
  - interloper_assets.facebook_ads.source.FacebookAds
```

Login is Google OAuth. `allowed_domains` restricts who may sign up; `super_admin_emails`
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
