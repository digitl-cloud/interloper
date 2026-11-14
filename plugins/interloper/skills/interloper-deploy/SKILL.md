---
name: interloper-deploy
description: Use when configuring or deploying the Interloper platform (interloper app, API, scheduler, web UI): interloper.yaml, INTERLOPER_* environment variables, Postgres, encryption at rest, Google sign-in, restricting the catalog, provider OAuth apps, telemetry, container images or the Helm chart.
---

# Deploying the Interloper platform

## Overview

The platform reads settings sections, each declared by the package it configures: an
`interloper.yaml` in the working directory plus `INTERLOPER_<SECTION>_<FIELD>` variables. The
environment wins for every field it sets; the YAML fills what the environment leaves out, and a
block no installed package declares is ignored. The YAML is not interpolated, so secrets stay out
of it and come from the environment. Reference: https://docs.interloper.dev/reference/settings/
and https://docs.interloper.dev/ui/running/

## Recipe

1. **`interloper.yaml`** with everything that is not a secret:

   ```yaml
   postgres:
     host: db.internal
     port: 5432
     user: interloper
     database: interloper          # password: INTERLOPER_POSTGRES_PASSWORD

   auth:
     allowed_domains: [example.com]
     super_admin_emails: [ops@example.com]
     cookie_secure: true           # needs TLS in front of the app

   server:
     host: 0.0.0.0
     port: 3000
     external_url: https://interloper.example.com   # login callbacks and OAuth links are built on it

   launcher:
     type: in_process              # runs execute inside the worker; docker / kubernetes launch a container per run

   catalog:
     include:                      # import paths; empty means everything installed
       - interloper_assets.facebook_ads.connector.source.FacebookAds
       - interloper_assets.google_ads.connector.source.GoogleAds

   otel:
     enabled: true
     endpoint: http://otel-collector:4317   # grpc by default; also protocol, traces, metrics, service_name
   ```

   The catalog closes over dependencies (the sources' connections appear on their own) and
   always contains the framework components (cron job, hooks). List the shipped paths with
   `python -c "import interloper as il; print(sorted(il.Catalog.discover().to_paths()))"`.

2. **Environment** (`.env.example` in the deployment, real values in the secret store):

   ```sh
   INTERLOPER_POSTGRES_PASSWORD=...
   INTERLOPER_ENCRYPTION_KEY=...                 # any strong secret string; derived to a Fernet key, encrypts stored credentials
   INTERLOPER_AUTH_GOOGLE_CLIENT_ID=...          # Google OAuth web client; authorised redirect URI = <server.external_url>/api/auth/google/callback
   INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET=...
   INTERLOPER_FACEBOOK_CLIENT_ID=...             # in-house app so users get "Sign in with Facebook" on connections
   INTERLOPER_FACEBOOK_CLIENT_SECRET=...
   ```

   Provider apps need both variables or the provider counts as unconfigured (manual
   credential entry only). The provider redirect URI is the app's `/auth/<provider>` page under
   `server.external_url` (`https://interloper.example.com/auth/facebook`), registered as-is in the
   provider console; `INTERLOPER_<PROVIDER>_REDIRECT_URI` overrides it. Same pattern for `GOOGLE`, `LINKEDIN`, `TIKTOK`,
   `MICROSOFT`, `PINTEREST`, `SNAPCHAT`, `CRITEO`, `AMAZON`.

3. **Database, then services:**

   ```sh
   interloper db init                                  # create database, tables, migrate to head; idempotent
   interloper api --no-create-tables                   # the api role (what the api image runs)
   interloper scheduler --no-create-tables             # cron, worker, reaper (what the scheduler image runs)
   interloper db upgrade                               # on every release
   ```

   `interloper app --api --cron --worker --reaper` runs everything in one process when
   `interloper-app` is installed. `cron` and `reaper` are singletons (`interloper scheduler
   --no-worker` for them, `--no-cron --no-reaper` for worker replicas); `api` and `worker` scale
   out. `--no-create-tables` skips the startup bootstrap because `db init` owns the schema.

4. **Verify the resolved settings** from the deployment directory with the variables exported:

   ```py
   import interloper as il
   from interloper.settings import SETTINGS
   s = {name: SETTINGS[name].get() for name in SETTINGS.keys()}   # every installed package's sections
   print(s["postgres"].host, s["auth"].allowed_domains, s["catalog"].include, s["otel"].endpoint, s["launcher"].type)
   print(bool(s["encryption"].key))                         # encryption at rest enabled
   print(sorted(il.Catalog.from_settings().components))     # dict keyed by component key; the provider key is `facebook`, the component key `facebook_ads`
   ```

## Quick reference

| Need | Use |
|------|-----|
| Containers | `ghcr.io/digitl-cloud/interloper-<role>:<version>` (loaded) or `:<version>-slim` (core + the role's own packages), for `api`, `frontend`, `core`, `scheduler`, `mcp` |
| Kubernetes | `helm repo add interloper https://docs.interloper.dev` then `helm install interloper interloper/interloper`; the loaded scheduler carries every launcher, so `launcher.type` is a runtime setting |
| Runner for in-process runs | `runner: {type: async, config: {max_workers: 8}}`; types `async`, `serial`, plus registered keys such as `k8s` |
| Who can sign up | `auth.allowed_domains`; listed `super_admin_emails` addresses are promoted when the API starts and at login |
| Local http | `INTERLOPER_AUTH_COOKIE_SECURE=false` |
| Every section prefix | `INTERLOPER_POSTGRES_`, `_AUTH_`, `_SERVER_`, `_CRON_`, `_WORKER_`, `_REAPER_`, `_RENEWAL_`, `_LAUNCHER_`, `_RUNNER_`, `_OTEL_`, `_LOG_`, `_CATALOG_`, `_ENCRYPTION_`, `_SMTP_`, `_AGENT_`, `_MCP_`, `_QUOTA_`, each followed by the field name (`INTERLOPER_ENCRYPTION_KEY`); a list takes comma-separated values |

## Common mistakes

- `password: ${POSTGRES_PASSWORD}` in `interloper.yaml`: the literal string is used. Omit the
  key and set the variable; `${VAR}` only works in job spec files.
- Setting a field in both places and expecting the environment to win: the YAML field wins.
- Leaving `catalog.include` empty in production: every installed source and connection is offered.
- Confusing `runner` (how assets execute inside a process) with `launcher` (how the scheduler
  starts a run); the launcher keys are `in_process`, `docker`, `kubernetes`.
- Registering `/api/oauth/providers/<provider>` or `/oauth/callback` as the provider redirect URI: the
  page is `/auth/<provider>`.
- `cookie_secure: true` behind plain http: the session cookie never sticks.
