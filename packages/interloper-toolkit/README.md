# interloper-toolkit

The tool functions shared by interloper's AI surfaces: the chat agent
(`interloper-agent`) and the MCP server (`interloper-mcp`).

Every function takes a frozen `ToolkitContext(store, catalog, org_id, role)`
as its first argument and returns `<SuccessModel> | ToolError`: typed
pydantic results (see `models.py`) discriminated by the literal `status`
field, never raising. The models are the tool contract: MCP derives per-tool
output schemas from them, row-projecting models act as an allowlist of what
leaves the platform, and full-row payloads embed the interloper-db models to
keep that coupling visible rather than duplicated. The docstrings are
LLM-facing: both surfaces adopt them verbatim as tool descriptions.

Reads take no role; writes declare the role they need with `requires_role`
and refuse below it, as a structured `ToolError`.

`tools.TOOLS` is the table both surfaces register: every tool with its
`Effect` (read, read through a provider, edit, create, launch, cancel). A
surface derives its behaviour from the effect (MCP annotations, the agent's
approvals) and `Tool.bind` adapts a function to the way the surface supplies
the context. A tool that `carries_secrets` in its arguments is left off any
surface that transports arguments through a third party.

Modules:

- `catalog`: the component definitions the platform ships (list, detail,
  asset schemas, field search, schema comparison)
- `collection`: the org's component instances (listing, edits, relations,
  connection checks and setup; sensitive kinds project identity only)
- `sources` and `jobs`: creating sources, resolving their fields, creating jobs
- `lineage`: dependency analysis, impact assessment, DAG traversal
- `scheduling`: jobs, runs and backfills, monitoring and control
- `analytics`: run statistics, partition coverage, data freshness
- `tools`: the table above

Depends only on `interloper-core` and `interloper-db`; no LLM-framework
dependencies (no pydantic-ai, no mcp).
