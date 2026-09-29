"""Tool functions shared by AI surfaces (agent, MCP server).

Every function takes a :class:`~interloper_toolkit.context.ToolkitContext`
as its first argument and returns ``<SuccessModel> | ToolError`` — typed
pydantic results (see :mod:`interloper_toolkit.models`) discriminated by
the literal ``status`` field, never raising. The docstrings are LLM-facing:
both the ADK agent and the MCP server surface them verbatim as tool
descriptions.

Reads take no role; writes declare the role they need with
:func:`~interloper_toolkit.authz.requires_role` and refuse below it. Which
functions a surface exposes is that surface's registration list (the MCP
server, for one, never registers ``create_connections``, whose arguments
would carry credentials through the client).
"""

from interloper_toolkit.authz import requires_role
from interloper_toolkit.collection import bind_relation, unbind_relation
from interloper_toolkit.context import ToolkitContext, serialize
from interloper_toolkit.models import ToolError

__all__ = ["ToolError", "ToolkitContext", "bind_relation", "requires_role", "serialize", "unbind_relation"]
