# interloper-toolkit

The read-only tool functions shared by interloper's AI surfaces — the ADK
chat agent (`interloper-agent`) and the MCP server (`interloper-mcp`).

Every function takes a frozen `ToolkitContext(store, catalog, org_id)` as its
first argument and returns a JSON-safe `{"status": "success" | "error", ...}`
dict — LLM-facing structured results, never raising. The docstrings are
LLM-facing too: both consumers surface them verbatim as tool descriptions.

Modules:

- `catalog` — the component definitions the platform ships (list, detail,
  asset schemas, field search, schema comparison)
- `collection` — the org's component instances (read-only listing; sensitive
  kinds project identity/metadata only)
- `lineage` — dependency analysis, impact assessment, DAG traversal
- `scheduling` — jobs, runs, and backfills: read-only monitoring
- `analytics` — run statistics, partition coverage, data freshness

Mutating operations (trigger/toggle/create) deliberately live with the agent,
not here — this package is safe to expose on read-only surfaces.

Depends only on `interloper-core` and `interloper-db`; no LLM-framework
dependencies (no google-adk, no mcp).
