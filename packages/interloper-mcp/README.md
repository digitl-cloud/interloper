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

## Deployment (not yet wired)

The container target would follow the existing dockerfile pattern:

```dockerfile
FROM base AS build-mcp
COPY packages ./packages
RUN docker/uv-sync.sh interloper-core interloper-assets interloper-db interloper-toolkit interloper-mcp

FROM runtime AS mcp
COPY --from=build-mcp /interloper/.venv /interloper/.venv
USER app
EXPOSE 3001
CMD ["interloper-mcp"]
```

plus a `mcp` role in the Makefile/publish workflow, a Helm/Flux workload
(`apps/interloper/mcp/` with an HTTPRoute for `mcp.interloper.app` — wildcard
DNS + TLS already cover it), and its own GSA/IAM DB user in terraform if it
should not share the app's identity.
