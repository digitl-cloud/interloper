# OAuth providers

An `OAuthProvider` is the identity and token-flow dialect of one OAuth2 service: where to send the
user, where to exchange codes and refresh tokens, and how those requests are shaped.
[Connections](connections.md#oauth-connections) reference providers by key through
`OAuthConfig`; the sign-in exchange and credential renewal both send whatever request the
provider builds.

## Built-in providers

| Key | Notes |
|-----|-------|
| `amazon` | Plain RFC 6749, JSON body. |
| `criteo` | Form-encoded token requests. |
| `facebook` | GET token requests; renews through the `fb_exchange_token` grant, a long-lived access token playing the refresh-token role. |
| `google` | Plain RFC 6749. |
| `linkedin` | Form-encoded. |
| `microsoft` | Form-encoded; refresh grants carry the connection's scope. |
| `pinterest` | Form-encoded; client credentials also sent as a Basic `Authorization` header. |
| `snapchat` | Form-encoded. |
| `tiktok` | Bespoke exchange parameters (`app_id`, `secret`, `auth_code`); no refresh flow, tokens do not expire. |

`PROVIDERS` in `interloper.oauth` is the registry; `PROVIDERS["google"]` returns the instance
and `PROVIDERS.keys()` lists what is installed.

## Defining a provider

A plain instance covers any service speaking RFC 6749:

```py
import interloper as il

ACME = il.OAuthProvider(
    key="acme",
    auth_url="https://auth.acme.example/oauth/authorize",
    token_url="https://auth.acme.example/oauth/token",
    label="ACME",
    icon="simple-icons:acme",
    token_encoding="form",          # "json" (default) or "form"
)
```

`token_encoding` is the only wire knob an instance sets. Any other deviation is a method
override on a subclass:

| Method | Default | Override when |
|--------|---------|---------------|
| `authorization_code_request(code, redirect_uri, client_id, client_secret)` | POST `grant_type=authorization_code` | the code exchange uses another method or parameter names |
| `refresh_token_request(client_id, client_secret, refresh_token, scope)` | POST `grant_type=refresh_token`, scope omitted | the refresh grant needs the scope, extra parameters, or is another grant entirely |
| `parse_refresh_token_response(payload)` | reads `refresh_token` and `refresh_token_expires_in` | the rotated credential or its validity arrive under other keys |
| `supports_refresh` (class attribute) | `True` | the service has no refresh flow at all; connections on it derive as non-renewable |

```py
class AcmeProvider(il.OAuthProvider):
    def refresh_token_request(self, *, client_id, client_secret, refresh_token, scope=None):
        params = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope:
            params["scope"] = scope
        return self._token_request(params)

ACME = AcmeProvider(key="acme", auth_url="...", token_url="...", token_encoding="form")
```

`_token_request(params)` builds the POST in the provider's encoding. Requests are `httpx.Request`
objects; the caller owns the client and error handling.

## Registering a provider

Providers register through the `interloper.oauth_providers` entry-point group. The entry may
point at an instance or at a class that constructs one:

```toml
[project.entry-points."interloper.oauth_providers"]
acme = "my_package.oauth:ACME"
```

The registry keys the provider by its own `key`, not by the entry-point name. It loads lazily
from installed-package metadata, so no import-order dependence exists. An `OAuthConfig`
naming an unregistered provider without an explicit `auth_url` raises `ConfigError`.

## In-house app credentials

Each provider's OAuth app is configured in the environment as a complete trio:

```
INTERLOPER_<PROVIDER>_CLIENT_ID
INTERLOPER_<PROVIDER>_CLIENT_SECRET
INTERLOPER_<PROVIDER>_REDIRECT_URI
```

`OAuthAppCredentials` in `interloper.oauth` resolves them: `from_env("google")` returns the
trio or `None` when any variable is missing; `is_configured(key)` answers whether sign-in is
possible; `env_name(key, "CLIENT_ID")` is the single owner of the naming convention.
Connections read the first two through `OAuthConnection.env_credential()`.
