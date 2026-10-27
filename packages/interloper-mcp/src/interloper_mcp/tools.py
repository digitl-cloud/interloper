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
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from interloper_mcp.context import get_ctx

ANNOTATIONS: dict[Effect, ToolAnnotations] = {
    Effect.READ: ToolAnnotations(read_only_hint=True, open_world_hint=False),
    Effect.READ_PROVIDER: ToolAnnotations(read_only_hint=True, open_world_hint=True),
    Effect.EDIT: ToolAnnotations(
        read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=False
    ),
    Effect.CREATE: ToolAnnotations(
        read_only_hint=False, destructive_hint=False, idempotent_hint=False, open_world_hint=False
    ),
    Effect.LAUNCH: ToolAnnotations(
        read_only_hint=False, destructive_hint=False, idempotent_hint=False, open_world_hint=True
    ),
    Effect.CANCEL: ToolAnnotations(
        read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
    ),
    Effect.DELETE: ToolAnnotations(
        read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
    ),
}


def register_tools(mcp: MCPServer) -> None:
    """Register the interloper tools on the server.

    Args:
        mcp: The MCP server instance.
    """
    for tool in TOOLS:
        if tool.carries_secrets:
            continue
        mcp.add_tool(tool.bind(get_ctx), description=tool.fn.__doc__, annotations=ANNOTATIONS[tool.effect])
