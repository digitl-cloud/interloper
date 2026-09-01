# OAuth

Many connectors authenticate with OAuth2. Interloper drives the "Sign in with X" flow from two
pieces:

- **`OAuthProvider`** — the identity and token-exchange spec for a provider (auth URL, token
  URL, how the exchange request is shaped). Providers are registered via entry points; the core
  library ships built-ins for the common platforms.
- **`OAuthConfig`** — per-connection OAuth settings: which provider to use, the scope to
  request, and how token-response keys map onto connection fields.

You attach an `OAuthConfig` to a [connection](connections.md) and Interloper does the rest:
it injects an `x-oauth` extension into the connection's schema so the web UI renders a sign-in
button, and after sign-in it maps the returned tokens onto the connection's fields.

## Configuring OAuth on a connection

```py
import interloper as il

@il.connection(
    name="LinkedIn Ads",
    oauth=il.OAuthConfig("linkedin", scope="r_ads"),
)
class LinkedinAdsConnection(il.OAuthConnection):
    account_id: str = il.InputField()
```

By default `OAuthConfig` maps only `{"refresh_token": "refresh_token"}` — the per-user refresh
token. `OAuthConnection` already declares the `client_id` / `client_secret` / `refresh_token`
trio that this targets. App credentials (`client_id`, `client_secret`) are resolved from
`<PROVIDER>_CLIENT_ID` / `<PROVIDER>_CLIENT_SECRET` environment variables at run time.

### Custom token shapes

When the token response doesn't match the standard refresh-token trio, base the connection on
`il.OAuthConnectionBase`, declare your own fields, and map them via `fields`:

```py
@il.connection(oauth=il.OAuthConfig("facebook", scope="ads_read", fields={"access_token": "access_token"}))
class FacebookAdsConnection(il.OAuthConnectionBase):
    access_token: str = il.SecretField()
```

## OAuthConfig

```py
il.OAuthConfig(
    provider="linkedin",        # provider key in the registry
    scope="r_ads",              # OAuth scope to request
    fields={"refresh_token": "refresh_token"},  # token key -> connection field
    auth_url="",                # override (required only for unregistered providers)
    label="",                   # display label override
    icon="",                    # icon override
)
```

If you reference an unregistered provider key without supplying `auth_url`, a `ConfigError` is
raised.

## Built-in providers

The core library registers providers for: **Amazon**, **Facebook**, **Google**, **LinkedIn**,
**Microsoft**, **Pinterest**, **Snapchat**, and **TikTok**. Reference one by its key (e.g.
`"google"`).

## Custom providers

Define an `OAuthProvider` and register it under the `interloper.oauth_providers` entry-point
group so it joins the registry:

```py
import interloper as il

ACME = il.OAuthProvider(
    key="acme",
    auth_url="https://auth.acme.com/oauth/authorize",
    token_url="https://auth.acme.com/oauth/token",
    label="ACME",
    icon="icon:acme",
    token_encoding="form",   # "json" (default) or "form"
)
```

The base class speaks plain RFC 6749: the authorization-code and refresh-token
grants are POSTs to `token_url` with the standard parameters, and `token_encoding` is the one
knob a plain instance may set. A provider whose dialect deviates — a different method,
parameter names, an extra parameter, another grant entirely — subclasses and overrides the
request builders instead:

```py
import httpx


class AcmeProvider(il.OAuthProvider):
    def refresh_token_request(self, *, client_id, client_secret, refresh_token, scope=None):
        """ACME requires the scope on refresh grants."""
        return self._token_request({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope or "",
        })
```

The overridable surface is `authorization_code_request`, `refresh_token_request`, and
`parse_refresh_token_response` — the sign-in exchange and connection renewal both send
whatever the provider builds, so a quirk lives in exactly one place. Connection renewal is
derived from these declarations: a connection whose provider has a refresh flow and whose
`fields` mapping covers the credential roles renews with no renewal code at all (a provider
without one — TikTok's tokens never expire — sets `supports_refresh = False` and its
connections derive as non-renewable). The built-ins show the
patterns: TikTok renames the exchange parameters, Facebook renews through its
`fb_exchange_token` grant, Microsoft adds the scope to refresh grants, Pinterest adds a
Basic `Authorization` header.
