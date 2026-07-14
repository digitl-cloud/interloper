"""Interaction tools — structured user input through app-rendered components.

Like ``request_connection_setup``, these tools' function responses are the
signal the app keys on: the frontend renders an interactive card (checkboxes
or radios) and reports the user's choice back into the chat.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext

#: The most options one selection card carries.
_MAX_CHOICES = 50


def request_user_selection(
    prompt: str,
    options: list[dict[str, str]],
    multi: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    """Present a selection card to the user in the app.

    Use this instead of listing choices as text whenever the user must pick
    from known options — an account from a provider, assets to enable. The
    app renders checkboxes (``multi``) or radios and reports the selection
    back as the user's next message.

    Args:
        prompt: Short question shown above the choices (e.g. "Which ad
            account should this source use?").
        options: The choices, each ``{"label": ..., "value": ...}`` — pass
            provider options or asset keys through as-is.
        multi: True when several options may be selected (e.g. assets);
            False for exactly one (e.g. an account).
    """
    try:
        cleaned = [
            {"label": str(o.get("label") or o.get("value")), "value": str(o.get("value"))}
            for o in options
            if isinstance(o, dict) and o.get("value") is not None
        ]
        if not cleaned:
            return {"status": "error", "error": "options must carry at least one {label, value} entry"}
        if len(cleaned) > _MAX_CHOICES:
            return {
                "status": "error",
                "error": f"Too many options ({len(cleaned)} > {_MAX_CHOICES}) — narrow them down first",
            }
        return {
            "status": "success",
            "message": "Selection presented to the user. Wait for their choice before continuing.",
            "prompt": prompt,
            "options": cleaned,
            "multi": multi,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
