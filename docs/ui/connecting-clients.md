# Connecting MCP clients

The platform runs an MCP server (`interloper-mcp`, served at `/mcp`) that gives an AI client the
same view of an organisation the app has: the catalog, the collection, lineage, runs and
backfills, and, for editors, the writes. A client authenticates in one of two ways: it signs in
through OAuth, or it is handed a personal access token.

## Connecting claude.ai

claude.ai and any other MCP client that supports remote servers connect as a custom connector,
with nothing to copy between the two sides.

1. In the client, add a custom connector and enter the MCP endpoint as its URL, for example
   `https://app.example.com/mcp`.
2. The client discovers the authorization server on its own, registers itself with it, and opens
   the app's consent page in a browser.
3. Sign in to the app if you are not already, check that the application named on the page is the
   one you just started, and approve.
4. The client returns with an access token and a refresh token and keeps itself signed in.

The consent page names the application, where it returns you to and the account it will act as.
Approve only a connection you started.

A connection acts as you, in every organisation you belong to, with the role you hold in each:
an editor can use the write tools in the organisations where they are editor and only read in the
others, and a role change applies to the connection at once. Every tool takes an `organisation`
argument naming the organisation it acts in (an id from `list_organisations`), which can be left
out when you belong to exactly one.

### Revoking a connection

Open **Settings**, **Authentication**, **Authorised apps**. Each application you have approved is
listed; revoking one ends its refresh token and its access tokens straight away, and it must ask
for your consent again to reconnect.

### Lifetimes

Access tokens are short lived and refresh tokens replace themselves on every use, so a client that
stays in use never asks you again. Both lifetimes are settings:

| Setting | Default | Meaning |
|---------|---------|---------|
| `auth.access_token_expiry_minutes` | `60` | Minutes an OAuth access token stays valid. |
| `auth.refresh_token_expiry_days` | `30` | Days a refresh token stays valid, counted again from each rotation. |

A client idle for longer than the refresh lifetime has to ask for consent again. Presenting a
refresh token that was already used is treated as theft: the whole chain it belongs to is revoked.

## Personal access tokens

A client that cannot run a browser flow (a script, a CI job, an older MCP client) takes a
personal access token instead. Create one under **Settings**, **Authentication**, **Personal
Access Tokens**, and send it as `Authorization: Bearer <token>`. The token is shown once, acts as
you in every organisation you belong to with your role in each, and works until it expires or
you revoke it on the same page. Treat it like a password.

## The deployment

Both sides need to know where they are reachable.

- `server.external_url` (`INTERLOPER_SERVER_EXTERNAL_URL`, the chart's `externalUrl`) is the
  public base URL of the app. It is the issuer of the authorization server, whose metadata the
  API serves at `/.well-known/oauth-authorization-server`, and the MCP server names it as the
  authorization server in its own metadata. Every pod of the deployment gets it.
- `mcp.external_url` (`INTERLOPER_MCP_EXTERNAL_URL`, the chart's `mcp.externalUrl`) is the public
  URL of the MCP endpoint, `https://app.example.com/mcp`, which clients take as the resource they
  are signing in to.

The chart's HTTPRoute sends `/mcp` and `/.well-known/oauth-protected-resource` to the MCP service
and `/.well-known/oauth-authorization-server` to the API, all on the app hostnames. The Ingress
sends the authorization server's metadata to the API as well; it does not route the MCP
endpoint, so an Ingress deployment that serves MCP clients adds those routes itself.

Outside the chart, `interloper app`, the compose stack's nginx and the Nuxt dev server route
`/.well-known/oauth-authorization-server` to the API too, and `make dev-up` and the compose stack
set `server.external_url` to the address the browser opens.

Without `server.external_url`, the issuer falls back to `http://localhost:<port>`, which serves a
local development instance and nothing else. The API and the MCP server derive it the same way,
so the issuer the MCP server names is the one the API serves.

The metadata, registration, token and revocation endpoints answer cross-origin requests from any
origin, without credentials, so a client running in a browser (MCP Inspector) can sign in. The
authorization and consent steps stay on the app's own origin.

## Signing in to the API

The authorization server also issues tokens for the HTTP API, for an application that calls it
rather than the MCP server (the Slack backend, a connector a vendor hosts). Such a client names
the API as the `resource` of its authorization request, `<issuer>/api`, for example
`https://app.example.com/api`, and sends the access token it receives as
`Authorization: Bearer <token>` on `/api/...` calls. A token is bound to the one resource it was
granted for: an API token is refused by the MCP server, and an MCP token by the API. Every
authorization request names exactly one resource; a trailing slash on it is ignored. Such a client
usually has a fixed client id and secret, registered as below.

## Registering a client yourself

A client that registers itself needs no setup, and it is shown to the user by the name it gave.
An application that cannot, because it is configured with a fixed client id and secret (a Slack
app, a connector a vendor hosts), is registered by an operator from the command line:

```sh
interloper oauth create --name "Acme connector" \
    --redirect-uri https://connector.example.com/oauth/callback
```

The command prints the `client_id` and the `client_secret`. The secret is shown once and cannot
be recovered: only its hash is stored, so a lost secret means deleting the client and creating
another. Repeat `--redirect-uri` to register several. A redirect URI must be `https`, `http` on
`localhost`, `127.0.0.1` or `[::1]`, or the private-use scheme of a known client (`cursor`,
`vscode`, `vscode-insiders`, `windsurf`); the same rule applies to clients that register
themselves. A request must name a registered redirect URI exactly, except that a loopback `http`
one matches on any port, since a native app picks a free port for each sign-in.

```sh
interloper oauth list
interloper oauth delete <client_id>
```

`list` shows the clients an operator registered, not the ones that registered themselves.
`delete` removes a client together with its codes and refresh tokens and revokes its access
tokens for every user, so nothing it was given keeps working. The commands read and write the
database the settings point at, so run them where the database is configured, like
`interloper db`. Every flag is in [CLI](../reference/cli.md#interloper-oauth).
