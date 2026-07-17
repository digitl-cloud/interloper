"""Read-only tool functions shared by AI surfaces (agent, MCP server).

Every function takes a :class:`~interloper_toolkit.context.ToolkitContext`
as its first argument and returns ``<SuccessModel> | ToolError`` — typed
pydantic results (see :mod:`interloper_toolkit.models`) discriminated by
the literal ``status`` field, never raising. The docstrings are LLM-facing:
both the ADK agent and the MCP server surface them verbatim as tool
descriptions.
"""

from interloper_toolkit.context import ToolkitContext, serialize
from interloper_toolkit.models import ToolError

__all__ = ["ToolkitContext", "ToolError", "serialize"]
