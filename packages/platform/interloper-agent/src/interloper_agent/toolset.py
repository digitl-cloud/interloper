"""The agent's toolset: the toolkit's table, plus the cards the app puts to the user.

pydantic-ai hands a tool its :class:`~pydantic_ai.RunContext`, whose
``deps`` is the :class:`~interloper_toolkit.ToolkitContext`; binding
supplies it in the context's place. A create, a cancel or a delete waits
for the user's approval in the app, a card defers its call for the app to
answer (:mod:`interloper_agent.interaction`), and every result reaches the
model without its empty fields.
"""

from __future__ import annotations

import inspect
from typing import Any

from interloper_toolkit import TOOLS as TOOLKIT_TOOLS
from interloper_toolkit import Effect, Tool, ToolkitContext
from pydantic import BaseModel
from pydantic_ai import RunContext
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset, WrapperToolset
from pydantic_ai.toolsets.abstract import ToolsetTool

from interloper_agent import interaction

TOOLS: tuple[Tool, ...] = (
    *TOOLKIT_TOOLS,
    Tool(interaction.request_user_selection, Effect.READ, "Ask the user to choose"),
    Tool(interaction.request_connection_setup, Effect.READ, "Set up a connection"),
)
"""Every tool the agent registers: the toolkit's, and the app's cards."""

RUN_CONTEXT = inspect.Parameter("run_context", inspect.Parameter.POSITIONAL_ONLY, annotation=RunContext[ToolkitContext])


def deps(run_context: RunContext[ToolkitContext]) -> ToolkitContext:
    """The toolkit context a run carries.

    Args:
        run_context: The run's context.

    Returns:
        Its ``deps``.
    """
    return run_context.deps


class Compacting(WrapperToolset[ToolkitContext]):
    """Hand the model each result without its empty fields: every null is tokens the model reads for nothing."""

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[ToolkitContext], tool: ToolsetTool[ToolkitContext]
    ) -> Any:
        """Call the tool and drop the null fields from a typed result.

        Args:
            name: The tool's name.
            tool_args: The validated arguments.
            ctx: The run context.
            tool: The tool definition being called.

        Returns:
            The result, as JSON without its nulls when it is a model.
        """
        result = await super().call_tool(name, tool_args, ctx, tool)
        return result.model_dump(mode="json", exclude_none=True) if isinstance(result, BaseModel) else result


def toolset() -> AbstractToolset[ToolkitContext]:
    """Build the agent's toolset from :data:`TOOLS`.

    Returns:
        The toolset, one tool per entry, its results compacted.
    """
    functions: FunctionToolset[ToolkitContext] = FunctionToolset()
    for tool in TOOLS:
        functions.add_function(tool.bind(deps, RUN_CONTEXT), takes_ctx=True, requires_approval=tool.needs_approval)
    return Compacting(functions)
