"""Tool functions shared by the AI surfaces (agent, MCP server).

Every function takes a :class:`~interloper_toolkit.context.ToolkitContext`
as its first argument and returns ``<SuccessModel> | ToolError``: typed
pydantic results (see :mod:`interloper_toolkit.models`) discriminated by
the literal ``status`` field, never raising. The docstrings are LLM-facing:
the surfaces adopt them verbatim as tool descriptions.

Reads take no role; writes declare the role they need with
:func:`~interloper_toolkit.authz.requires_role` and refuse below it.
:data:`~interloper_toolkit.tools.TOOLS` is the table the surfaces register,
each tool with its :class:`~interloper_toolkit.tools.Effect`; a surface
derives its behaviour from that and leaves out what it cannot carry (the
MCP server never registers a tool whose arguments carry credentials).
"""

from interloper_toolkit.authz import requires_role
from interloper_toolkit.collection import bind_relation, unbind_relation
from interloper_toolkit.context import ToolkitContext, serialize
from interloper_toolkit.models import ToolError, UserRequest
from interloper_toolkit.tools import TOOLS, Effect, Tool

__all__ = [
    "TOOLS",
    "Effect",
    "Tool",
    "ToolError",
    "ToolkitContext",
    "UserRequest",
    "bind_relation",
    "requires_role",
    "serialize",
    "unbind_relation",
]
