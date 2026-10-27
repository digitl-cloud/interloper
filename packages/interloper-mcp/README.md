# interloper-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server exposing
the interloper platform to AI agents: catalog definitions, the collection,
lineage, run/backfill monitoring, analytics and, for tokens holding the
editor role, the writes (edits, creates, triggers, retries, cancels).

Authentication uses **personal access tokens** (PATs) minted via the
interloper API (`POST /api/tokens`, session-authenticated). A token acts as
its holder in every organisation they belong to, with their live role in
each; revocation and membership removal apply immediately.

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

Clients authenticate every request with `Authorization: Bearer ilp_...`.

stdio (single-user, authenticates once at startup; the token is required,
so local development uses a PAT created in the dev instance):

```sh
INTERLOPER_MCP_TOKEN=ilp_... interloper-mcp --transport stdio
```

## Connecting clients

MCP Inspector:

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
| `INTERLOPER_MCP_EXTERNAL_URL` | (unset) | Public URL of the MCP endpoint (e.g. `https://app.interloper.dev/mcp`), used in OAuth protected-resource metadata |
| `INTERLOPER_MCP_TOKEN` | (unset) | stdio only: PAT to authenticate as (required) |

Database and catalog configuration is the standard interloper set
(`INTERLOPER_POSTGRES_*`, `INTERLOPER_CATALOG`, ...).

## Deployment

The `mcp` dockerfile target ships as `interloper-mcp:<version>` alongside the
other role images (Makefile `ROLES` + the publish workflow). The Helm chart
deploys it with `mcp.enabled=true` — the workload shares the release's
Postgres/catalog configuration, sets `INTERLOPER_MCP_EXTERNAL_URL` from
`mcp.externalUrl`, and the chart's HTTPRoute forwards `/mcp` and
`/.well-known/oauth-protected-resource` on the app hostnames to it. What
remains for the operator is supplying the environment values and, if it should
not share the app's identity, a database identity of its own.
