# interloper-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server exposing
the interloper platform to AI agents: catalog definitions, the collection,
lineage, run/backfill monitoring, analytics and, for tokens holding the
editor role, the writes (edits, creates, triggers, retries, cancels).

A client authenticates with a bearer token of one of two kinds: an
**OAuth access token**, which a client such as claude.ai obtains on its own
from the authorization server the interloper API runs (discovery, dynamic
registration, the user's consent in the app), or a **personal access token**
(PAT) the user creates in the app's settings. Either acts as its user in
every organisation they belong to, with their live role in each; revocation
and membership removal apply immediately. The server publishes its
protected-resource metadata at `/.well-known/oauth-protected-resource/mcp`,
naming the authorization server, so an OAuth client needs only the endpoint
URL. See [Connecting MCP clients](../../docs/ui/connecting-clients.md).

Every tool takes an `organisation` argument, the id of the organisation the
call acts in. It may be left out when the user belongs to exactly one;
otherwise the call returns an error listing the user's organisations, which
`list_organisations` also lists, with the role in each. A super-admin acts
as `admin` in any organisation they name.

## Running

Streamable HTTP (default; binds `INTERLOPER_MCP_HOST:INTERLOPER_MCP_PORT`,
endpoint `/mcp`):

```sh
interloper-mcp
```

Clients authenticate every request with `Authorization: Bearer <token>`.

stdio (single-user, authenticates once at startup; the token is required,
so local development uses a PAT created in the dev instance):

```sh
INTERLOPER_MCP_TOKEN=ilp_... interloper-mcp --transport stdio
```

## Connecting clients

claude.ai, Claude Code and any client that supports OAuth: add the MCP
endpoint as a remote server and approve the consent page the client opens.

```sh
claude mcp add --transport http interloper https://app.example.com/mcp
```

With a personal access token instead, MCP Inspector:

```sh
npx @modelcontextprotocol/inspector
# Transport: Streamable HTTP, URL: http://localhost:3001/mcp
# Header: Authorization: Bearer ilp_...
```

Claude Code:

```sh
claude mcp add --transport http interloper http://localhost:3001/mcp \
  --header "Authorization: Bearer ilp_..."
```

## Settings

| Env var | Default | Purpose |
| --- | --- | --- |
| `INTERLOPER_MCP_HOST` | `0.0.0.0` | HTTP bind host |
| `INTERLOPER_MCP_PORT` | `3001` | HTTP bind port |
| `INTERLOPER_MCP_EXTERNAL_URL` | (unset) | Public URL of the MCP endpoint, the OAuth resource clients sign in to; unset means `/mcp` on `INTERLOPER_SERVER_EXTERNAL_URL`. The API reads it too |
| `INTERLOPER_MCP_TOKEN` | (unset) | stdio only: PAT to authenticate as (required) |

Database and catalog configuration is the standard interloper set
(`INTERLOPER_POSTGRES_*`, `INTERLOPER_CATALOG`, ...).

## Deployment

The `mcp` dockerfile target ships as `interloper-mcp:<version>` alongside the
other role images (Makefile `ROLES` + the publish workflow). The Helm chart
deploys it with `mcp.enabled=true`: the workload shares the release's
Postgres/catalog configuration, gives every pod `INTERLOPER_MCP_EXTERNAL_URL`
when `mcp.externalUrl` is set, and the chart's HTTPRoute forwards `/mcp` and
`/.well-known/oauth-protected-resource` on the app hostnames to it. What
remains for the operator is supplying the environment values and, if it should
not share the app's identity, a database identity of its own.
