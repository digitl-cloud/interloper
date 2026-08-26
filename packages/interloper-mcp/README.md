# interloper-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server exposing
read-only interloper platform access to AI agents: catalog definitions,
collection listings, lineage, run/backfill monitoring, and analytics.

Authentication uses **personal access tokens** (PATs) minted via the
interloper API (`POST /api/tokens`, session-authenticated). Tokens are
org-scoped and carry the holder's live role; revocation and membership
removal apply immediately.

## Running

Streamable HTTP (default; binds `INTERLOPER_MCP_HOST:INTERLOPER_MCP_PORT`,
endpoint `/mcp`):

```sh
interloper-mcp
```

Clients authenticate every request with `Authorization: Bearer ilp_...`.

stdio (single-user, authenticates once at startup):

```sh
INTERLOPER_MCP_TOKEN=ilp_... interloper-mcp --transport stdio
```

For local development without a token, `INTERLOPER_MCP_ORG_ID=<uuid>` scopes
the stdio server to one organisation directly.

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
| `INTERLOPER_MCP_EXTERNAL_URL` | — | Public base URL (e.g. `https://mcp.interloper.app`), used in OAuth protected-resource metadata |
| `INTERLOPER_MCP_TOKEN` | — | stdio only: PAT to authenticate as |
| `INTERLOPER_MCP_ORG_ID` | — | stdio only: dev fallback, direct org scope |

Database and catalog configuration is the standard interloper set
(`INTERLOPER_POSTGRES_*`, `INTERLOPER_CATALOG`, ...).

## Deployment

The `mcp` dockerfile target ships as `interloper-mcp:<version>` alongside the
other role images (Makefile `ROLES` + the publish workflow). The Helm chart
deploys it with `mcp.enabled=true` — the workload shares the release's
Postgres/catalog configuration, sets `INTERLOPER_MCP_EXTERNAL_URL` from
`mcp.externalUrl`, and gets its own HTTPRoute when `httpRoute.mcpHostnames`
names its host. What remains cluster-side is the environment values (Flux)
and, if it should not share the app's identity, its own GSA/IAM DB user in
terraform.
