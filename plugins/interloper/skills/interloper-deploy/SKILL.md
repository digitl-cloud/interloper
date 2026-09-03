---
name: interloper-deploy
description: Use when configuring or deploying the Interloper platform (interloper app, API, scheduler, web UI): interloper.yaml, INTERLOPER_* environment variables, Postgres, encryption at rest, Google sign-in, restricting the catalog, provider OAuth apps, telemetry, container images or the Helm chart.
---

# Deploying the Interloper platform

## Overview

The platform reads `AppSettings`: an `interloper.yaml` in the working directory plus
`INTERLOPER_<SECTION>_<FIELD>` variables. A field present in the YAML wins for that field; the
environment fills what the YAML leaves out. The YAML is not interpolated, so secrets stay out
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
     google_redirect_uri: https://interloper.example.com/api/auth/google/callback
     allowed_domains: [example.com]
     super_admin_emails: [ops@example.com]
     cookie_secure: true           # needs TLS in front of the app

   server:
     host: 0.0.0.0
     port: 3000

   launcher:
     type: in_process              # runs execute inside the worker; docker / kubernetes launch a container per run

   catalog:                        # import paths; empty means everything installed
     - interloper_assets.facebook_ads.source.FacebookAds
     - interloper_assets.google_ads.source.GoogleAds

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
   INTERLOPER_AUTH_GOOGLE_CLIENT_ID=...          # Google OAuth web client; authorised redirect URI = auth.google_redirect_uri
   INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET=...
   INTERLOPER_FACEBOOK_CLIENT_ID=...             # in-house app so users get "Sign in with Facebook" on connections
   INTERLOPER_FACEBOOK_CLIENT_SECRET=...
   INTERLOPER_FACEBOOK_REDIRECT_URI=https://interloper.example.com/auth/facebook
   ```

   Provider apps need all three variables or the provider counts as unconfigured (manual
   credential entry only). The provider redirect URI is the app's `/auth/<provider>` page,
   registered as-is in the provider console. Same pattern for `GOOGLE`, `LINKEDIN`, `TIKTOK`,
   `MICROSOFT`, `PINTEREST`, `SNAPCHAT`, `CRITEO`, `AMAZON`.

3. **Database, then services:**

   ```sh
   interloper db init                                                     # create database, tables, migrate to head; idempotent
   interloper app --api --cron --worker --reaper --no-create-tables       # one process; drop flags to split roles across replicas
   interloper db upgrade                                                  # on every release
   ```

   `cron` and `reaper` are singletons; `api` and `worker` scale out. `--no-create-tables`
   skips the startup bootstrap because `db init` owns the schema.

4. **Verify the resolved settings** from the deployment directory with the variables exported:

   ```py
   from interloper.settings import AppSettings
   import interloper as il
   s = AppSettings()
   print(s.postgres.host, s.auth.allowed_domains, s.catalog, s.otel.endpoint, s.launcher.type)
   print(bool(s.secrets.encryption_key))                    # encryption at rest enabled
   print(sorted(il.Catalog.from_settings().components))     # dict keyed by component key; the provider key is `facebook`, the component key `facebook_ads`
   ```

## Quick reference

| Need | Use |
|------|-----|
| Containers | `ghcr.io/digitl-cloud/interloper-<role>:<version>` for `api` (`-agent` flavour), `frontend`, `worker`, `scheduler` (`-k8s`, `-docker` flavours), `mcp` |
| Kubernetes | `helm repo add interloper https://docs.interloper.dev` then `helm install interloper interloper/interloper`; `launcher.type` selects the scheduler flavour |
| Runner for in-process runs | `runner: {type: async, config: {max_workers: 8}}`; types `async`, `serial`, `multi_process`, plus registered keys such as `k8s` |
| Who can sign up | `auth.allowed_domains`; first login of a listed `super_admin_emails` address is promoted |
| Local http | `INTERLOPER_AUTH_COOKIE_SECURE=false` |
| Every section prefix | `INTERLOPER_POSTGRES_`, `_AUTH_`, `_SERVER_`, `_CRON_`, `_WORKER_`, `_REAPER_`, `_RENEWAL_`, `_LAUNCHER_`, `_RUNNER_`, `_OTEL_`, `_SMTP_`, `_AGENT_`, `_MCP_`, `_QUOTA_`; the encryption key is `INTERLOPER_ENCRYPTION_KEY` |

## Common mistakes

- `password: ${POSTGRES_PASSWORD}` in `interloper.yaml`: the literal string is used. Omit the
  key and set the variable; `${VAR}` only works in job spec files.
- Setting a field in both places and expecting the environment to win: the YAML field wins.
- Leaving `catalog` empty in production: every installed source and connection is offered.
- Confusing `runner` (how assets execute inside a process) with `launcher` (how the scheduler
  starts a run); the launcher keys are `in_process`, `docker`, `kubernetes`.
- Registering `/api/oauth/<provider>` or `/oauth/callback` as the provider redirect URI: the
  page is `/auth/<provider>`.
- `cookie_secure: true` behind plain http: the session cookie never sticks.
