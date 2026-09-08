"""Tool functions shared by AI surfaces (agent, MCP server).

Every function takes a :class:`~interloper_toolkit.context.ToolkitContext`
as its first argument and returns ``<SuccessModel> | ToolError`` — typed
pydantic results (see :mod:`interloper_toolkit.models`) discriminated by
the literal ``status`` field, never raising. The docstrings are LLM-facing:
both the ADK agent and the MCP server surface them verbatim as tool
descriptions.

Almost every function here is read-only; the sole exceptions are
:func:`interloper_toolkit.collection.bind_relation` and
:func:`interloper_toolkit.collection.unbind_relation`, which write and are
re-exported here as this package's whole write surface. A surface that must
stay read-only (the MCP server's own registration is one) never registers
those two; the ADK agent, whose own write tools already live beside them,
does.
"""

from interloper_toolkit.collection import bind_relation, unbind_relation
from interloper_toolkit.context import ToolkitContext, serialize
from interloper_toolkit.models import ToolError

__all__ = ["ToolError", "ToolkitContext", "bind_relation", "serialize", "unbind_relation"]
