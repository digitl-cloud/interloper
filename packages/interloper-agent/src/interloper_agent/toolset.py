"""The agent's toolset: the toolkit's table, plus the question the app puts to the user.

pydantic-ai hands a tool its :class:`~pydantic_ai.RunContext`, whose
``deps`` is the :class:`~interloper_toolkit.ToolkitContext`; binding
supplies it in the context's place. Two behaviours follow from the table
and the results rather than from per-tool code: a create or a cancel waits
for the user's approval in the app, and a result that awaits the user (a
:class:`~interloper_toolkit.UserRequest`) stops the run for the app to
answer.
"""

from __future__ import annotations

import inspect
from typing import Any

from interloper_toolkit import TOOLS as TOOLKIT_TOOLS
from interloper_toolkit import Effect, Tool, ToolkitContext, UserRequest
from pydantic_ai import CallDeferred, RunContext
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset, WrapperToolset
from pydantic_ai.toolsets.abstract import ToolsetTool

from interloper_agent import interaction

TOOLS: tuple[Tool, ...] = (*TOOLKIT_TOOLS, Tool(interaction.request_user_selection, Effect.READ))
"""Every tool the agent registers: the toolkit's, and the app's selection card."""

RUN_CONTEXT = inspect.Parameter("run_context", inspect.Parameter.POSITIONAL_ONLY, annotation=RunContext[ToolkitContext])


def deps(run_context: RunContext[ToolkitContext]) -> ToolkitContext:
    """The toolkit context a run carries.

    Args:
        run_context: The run's context.

    Returns:
        Its ``deps``.
    """
    return run_context.deps


class Deferring(WrapperToolset[ToolkitContext]):
    """Stop the run when a tool's result awaits the user; the app answers the call."""

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[ToolkitContext], tool: ToolsetTool[ToolkitContext]
    ) -> Any:
        """Call the tool, deferring it when its result is a question for the user.

        Args:
            name: The tool's name.
            tool_args: The validated arguments.
            ctx: The run context.
            tool: The tool definition being called.

        Returns:
            The tool's result.

        Raises:
            CallDeferred: When the result awaits the user's input.
        """
        result = await super().call_tool(name, tool_args, ctx, tool)
        if isinstance(result, UserRequest) and result.awaits_user:
            raise CallDeferred()
        return result


def toolset() -> AbstractToolset[ToolkitContext]:
    """Build the agent's toolset from :data:`TOOLS`.

    Returns:
        The toolset, one tool per entry, with deferral on results that await the user.
    """
    functions: FunctionToolset[ToolkitContext] = FunctionToolset()
    for tool in TOOLS:
        functions.add_function(tool.bind(deps, RUN_CONTEXT), takes_ctx=True, requires_approval=tool.needs_approval)
    return Deferring(functions)
