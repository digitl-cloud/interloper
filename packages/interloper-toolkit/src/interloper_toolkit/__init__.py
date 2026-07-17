"""Read-only tool functions shared by AI surfaces (agent, MCP server).

Every function takes a :class:`~interloper_toolkit.context.ToolkitContext`
as its first argument and returns a JSON-safe ``{"status": "success" | "error",
...}`` dict — LLM-facing structured results, never raising. The docstrings are
LLM-facing too: both the ADK agent and the MCP server surface them verbatim as
tool descriptions.
"""

from interloper_toolkit.context import ToolkitContext, serialize

__all__ = ["ToolkitContext", "serialize"]
