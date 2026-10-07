"""MCP tool registration: the toolkit's table, bound to the request's context.

Each tool carries the MCP annotations a client keys its behaviour on
(``readOnlyHint``, ``destructiveHint``, ``idempotentHint``,
``openWorldHint``), mapped from the tool's effect; the spec's defaults are
the pessimistic ones, so reads say so explicitly. The writes are gated in
the toolkit on the token's role; the annotations only tell the client when
to ask the user first. A tool whose arguments carry credentials is not
registered: they would travel through the client.
"""

from __future__ import annotations

from interloper_toolkit import TOOLS, Effect
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from interloper_mcp.context import get_ctx

ANNOTATIONS: dict[Effect, ToolAnnotations] = {
    Effect.READ: ToolAnnotations(readOnlyHint=True, openWorldHint=False),
    Effect.READ_PROVIDER: ToolAnnotations(readOnlyHint=True, openWorldHint=True),
    Effect.EDIT: ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
    Effect.CREATE: ToolAnnotations(
        readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False
    ),
    Effect.LAUNCH: ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True),
    Effect.CANCEL: ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False),
    Effect.DELETE: ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False),
}


def register_tools(mcp: FastMCP) -> None:
    """Register the interloper tools on the server.

    Args:
        mcp: The FastMCP server instance.
    """
    for tool in TOOLS:
        if tool.carries_secrets:
            continue
        mcp.add_tool(tool.bind(get_ctx), description=tool.fn.__doc__, annotations=ANNOTATIONS[tool.effect])
