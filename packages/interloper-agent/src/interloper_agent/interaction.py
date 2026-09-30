"""The agent's own tool: a choice the app puts to the user.

It changes nothing on the platform, so it is not in the toolkit's table; the
agent's toolset adds it. Its result is a :class:`~interloper_toolkit.UserRequest`,
which the toolset defers to the app like any other: the app renders the
card and submits the user's choice as the tool's result.
"""

from __future__ import annotations

from typing import Literal

from interloper_toolkit import ToolkitContext, UserRequest
from interloper_toolkit.models import ToolError

MAX_CHOICES = 50
"""The most options one selection card carries."""


class SelectionRequest(UserRequest):
    """The choice put to the user."""

    status: Literal["success"] = "success"
    prompt: str
    options: list[dict[str, str]]
    multi: bool


def request_user_selection(
    ctx: ToolkitContext, prompt: str, options: list[dict[str, str]], multi: bool = False
) -> SelectionRequest | ToolError:
    """Ask the user to pick from known options, through a card in the app.

    Use this instead of listing choices as text whenever the user must pick
    from known options: an account from a provider, assets to enable. The
    app renders checkboxes (``multi``) or radios; the user's choice comes back
    as this tool's result, ``{"selected": [<value>, ...]}``.

    Args:
        prompt: Short question shown above the choices (e.g. "Which ad
            account should this source use?").
        options: The choices, each ``{"label": ..., "value": ...}``; pass
            provider options or asset keys through as-is.
        multi: True when several options may be selected (e.g. assets);
            False for exactly one (e.g. an account).
    """
    cleaned = [
        {"label": str(o.get("label") or o.get("value")), "value": str(o.get("value"))}
        for o in options
        if isinstance(o, dict) and o.get("value") is not None
    ]
    if not cleaned:
        return ToolError(error="options must carry at least one {label, value} entry")
    if len(cleaned) > MAX_CHOICES:
        return ToolError(error=f"Too many options ({len(cleaned)} > {MAX_CHOICES}): narrow them down first")
    return SelectionRequest(prompt=prompt, options=cleaned, multi=multi)
