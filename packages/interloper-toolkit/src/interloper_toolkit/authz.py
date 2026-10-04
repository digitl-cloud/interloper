"""Role gating for the toolkit's write tools.

The context carries the caller's role in the organisation; a write declares
the role it needs and refuses, as a structured :class:`ToolError`, before its
body runs. The ranks are :class:`~interloper_db.Role`'s, the ones the API's
route gates apply, so a caller can do through a tool exactly what it could do
through the app.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar, cast

from interloper_db.models import Role

from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import ToolError

F = TypeVar("F", bound=Callable[..., Any])


def requires_role(minimum: str) -> Callable[[F], F]:
    """Refuse a tool call whose context holds a role below *minimum*.

    Works on sync and async tool functions alike; the context is the first
    positional argument, as it is for every toolkit function. The wrapped
    function keeps its name and docstring, which the AI surfaces adopt.

    Args:
        minimum: The lowest role allowed: ``viewer``, ``editor`` or ``admin``;
            any other name is a ``ConfigError`` at decoration time.

    Returns:
        The decorator to apply to a tool function.
    """
    Role.parse(minimum)

    def decorate(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_gate(ctx: ToolkitContext, *args: Any, **kwargs: Any) -> Any:
                return denied(ctx.role, minimum) or await func(ctx, *args, **kwargs)

            return cast(F, async_gate)

        @functools.wraps(func)
        def gate(ctx: ToolkitContext, *args: Any, **kwargs: Any) -> Any:
            return denied(ctx.role, minimum) or func(ctx, *args, **kwargs)

        return cast(F, gate)

    return decorate


def denied(role: str, minimum: str) -> ToolError | None:
    """The refusal for a role below *minimum*, or ``None`` when the role suffices.

    Args:
        role: The caller's role; an unknown one ranks below every known role.
        minimum: The lowest role allowed.

    Returns:
        The structured error, or ``None`` when the call may proceed.
    """
    if not Role.at_least(role, minimum):
        return ToolError(error=f"Requires {minimum} role or higher")
    return None
