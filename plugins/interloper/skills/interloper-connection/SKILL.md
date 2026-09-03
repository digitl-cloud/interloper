---
name: interloper-connection
description: Use when writing or fixing an Interloper connection or resource: credentials for a service, OAuth sign-in through a provider, a health check, an account picker that feeds a dropdown, or settings loaded from environment variables.
---

# Writing an Interloper connection

## Overview

A connection is a pydantic-settings model. Fields are the credentials, methods are the client,
the health check and the option pickers. The UI form, the "Test connection" button, the
sign-in tab and the auto-renew toggle are all derived from `definition()`, so the class is the
whole feature. Reference: https://docs.interloper.dev/guide/connections/ and
https://docs.interloper.dev/guide/oauth/

## Recipe

1. **Plain credentials.** Prefix the environment lookup and give the class a client:

   ```py
   from functools import cached_property

   import interloper as il
   from interloper.errors import ConnectionCheckError
   from pydantic_settings import SettingsConfigDict

   @il.connection(name="Acme Ads", icon="carbon:bullhorn")
   class AcmeConnection(il.Connection):
       model_config = SettingsConfigDict(env_prefix="acme_")      # ACME_API_KEY

       api_key: str = il.SecretField(description="API key")
       base_url: str = il.InputField(default="https://api.acme.example/v1")

       @cached_property
       def client(self) -> il.RESTClient:
           return il.RESTClient(self.base_url, auth=il.HTTPBearerAuth(self.api_key))

       def check(self) -> bool:                                    # sync or async, both work
           response = self.client.get("/me")
           if response.status_code == 401:
               raise ConnectionCheckError("API key rejected")      # message shown in the UI
           return response.is_success
   ```

2. **OAuth.** Subclass `il.RefreshTokenOAuthConnection` and name the provider; the base class
   already declares `client_id`, `client_secret` and `refresh_token`, and renewal is derived
   from the provider, so there is no `renew()` to write and no field to redeclare:

   ```py
   @il.connection(name="Acme Ads", oauth=il.OAuthConfig("google", scope="https://www.googleapis.com/auth/adwords"))
   class AcmeConnection(il.RefreshTokenOAuthConnection):
       model_config = SettingsConfigDict(env_prefix="acme_")      # for your own fields only
       developer_token: str = il.SecretField()
   ```

   `client_id` and `client_secret` are required and filled before validation from
   `INTERLOPER_GOOGLE_CLIENT_ID` / `INTERLOPER_GOOGLE_CLIENT_SECRET`, whatever `env_prefix`
   says. A script needs those two; `INTERLOPER_GOOGLE_REDIRECT_URI` is only needed for the
   sign-in tab in the UI. `refresh_token` is required too, so a local instance needs one even
   for tests of the other fields. Providers shipped: amazon, criteo, facebook, google, linkedin,
   microsoft, pinterest, snapchat, tiktok. `OAuthConfig` also takes `fields`, `auth_url`,
   `label`, `icon`; non-standard token fields go on `il.OAuthConnection` with `fields={...}`.

3. **Pickers.** A provider method on the connection, a `FetchField` on the source that names
   it through the slot:

   ```py
   class AcmeConnection(...):
       @il.fetch_field_provider
       async def accounts(self) -> list[dict]:
           return self.client.get("/accounts").json()["accounts"]     # [{"id": ..., "name": ...}]

   @il.source(name="Acme Ads", resources={"connection": AcmeConnection})
   class AcmeAds(il.Source):
       account_id: str = il.FetchField(provider="connection.accounts", label_key="name", value_key="id", discriminator=True)
   ```

   The wiring is validated when `definition()` runs (the catalog, the UI, your verify step): a
   wrong slot or method name is a `TypeError` there, not at import, and the slot must appear in
   `resources={...}`. The assets themselves (`il.Schema`, `il.ExecutionContext`,
   `context.partition_date`) are covered by the interloper-source skill.

4. **Verify the definition** before touching a UI:

   ```py
   d = AcmeConnection.definition()
   d.provider, d.checkable, d.renewable          # 'google', True, True
   d.config_schema["x-oauth"]                    # provider, auth_url, scope, field mapping
   AcmeAds.definition().config_schema["properties"]["account_id"]["x-fetch"]
   ```

   `d.schema` is pydantic's method, not the form schema; the form is `config_schema`.

5. Instantiate once with real values to confirm env loading: `AcmeConnection()` with
   `ACME_API_KEY` exported, or `AcmeConnection(refresh_token="...")` with the provider vars.

## Quick reference

| Need | Use |
|------|-----|
| Non-credential settings shared by sources | `il.Config` subclass, same field helpers, injected the same way |
| Field hidden behind a secret input | `il.SecretField()`; encrypted at rest on the platform |
| Dropdown with fixed options | `il.SelectField(options=[...])` |
| Name instances by a field | `discriminator=True` |
| Read a picker from Python | `await connection.accounts()` |
| Custom OAuth provider | `il.OAuthProvider` subclass registered under the `interloper.oauth_providers` entry point |
| Errors | `from interloper.errors import ConnectionCheckError` (not exported on `il`) |

## Common mistakes

- Writing `renew()` or redeclaring `refresh_token` on an OAuth connection: both are already there.
- Reading `definition().schema`: use `config_schema`, `provider`, `checkable`, `renewable`.
- `FetchField(provider="conn.accounts")` with the slot declared as `connection`; the error
  message says `not declared in resources={}` even when other slots are declared, and it only
  surfaces once something calls `definition()`.
- Passing `client_id=` by hand in production code; set the `INTERLOPER_<PROVIDER>_*` variables.
- Assuming `check()` must be async, or returning `None`: return a bool, raise
  `ConnectionCheckError` for a message.
