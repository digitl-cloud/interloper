# interloper-toolkit

The read-only tool functions shared by interloper's AI surfaces — the ADK
chat agent (`interloper-agent`) and the MCP server (`interloper-mcp`).

Every function takes a frozen `ToolkitContext(store, catalog, org_id)` as its
first argument and returns `<SuccessModel> | ToolError` — typed pydantic
results (see `models.py`) discriminated by the literal `status` field, never
raising. The models are the tool contract: MCP derives per-tool output
schemas from them, row-projecting models act as an allowlist of what leaves
the platform, and full-row payloads embed the interloper-db models to keep
that coupling visible rather than duplicated. The docstrings are LLM-facing:
both consumers surface them verbatim as tool descriptions.

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
