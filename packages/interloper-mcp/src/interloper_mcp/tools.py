"""MCP tool registration: the toolkit's table, bound to the request's context.

Every toolkit tool takes a trailing ``organisation`` argument, the id of the
organisation the call acts in (see :func:`~interloper_mcp.context.get_ctx`),
and ``list_organisations``, which this surface adds, lists the ones a user
can name. Each tool carries the MCP annotations a client keys its behaviour on
(``readOnlyHint``, ``destructiveHint``, ``idempotentHint``,
``openWorldHint``), mapped from the tool's effect; the spec's defaults are
the pessimistic ones, so reads say so explicitly. The writes are gated in
the toolkit on the caller's role there; the annotations only tell the client when
to ask the user first. A tool whose arguments carry credentials is not
registered: they would travel through the client.
"""

from __future__ import annotations

import inspect
from typing import Annotated, Literal
from uuid import UUID

from interloper_toolkit import TOOLS, Effect
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from interloper_mcp.context import get_ctx, organisations

ORGANISATION = inspect.Parameter(
    "organisation",
    inspect.Parameter.KEYWORD_ONLY,
    default=None,
    annotation=Annotated[
        str | None,
        Field(
            description="The id of the organisation the call acts in, from list_organisations; "
            "may be left out when you belong to exactly one."
        ),
    ],
)

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


class OrganisationEntry(BaseModel):
    """One organisation the user can name, with the role tool calls hold in it."""

    id: UUID
    key: str
    name: str
    role: str


class OrganisationList(BaseModel):
    """The organisations ``list_organisations`` returns."""

    status: Literal["success"] = "success"
    organisations: list[OrganisationEntry]


def list_organisations() -> OrganisationList:
    """List the organisations you belong to, with your role in each.

    Every other tool takes an `organisation` argument, the id of the
    organisation it acts in. It may be left out when you belong to exactly one.
    """
    return OrganisationList(
        organisations=[
            OrganisationEntry(id=org.id, key=org.key, name=org.name, role=role) for org, role in organisations()
        ]
    )


def register_tools(mcp: MCPServer) -> None:
    """Register the interloper tools on the server.

    Args:
        mcp: The MCP server instance.
    """
    for tool in TOOLS:
        if tool.carries_secrets:
            continue
        mcp.add_tool(
            tool.bind(get_ctx, trailing=[ORGANISATION]),
            description=tool.fn.__doc__,
            annotations=ANNOTATIONS[tool.effect],
        )
    mcp.add_tool(list_organisations, description=list_organisations.__doc__, annotations=ANNOTATIONS[Effect.READ])
