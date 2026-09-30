"""Tools the app answers on the user's behalf.

Both stop the run with :class:`~pydantic_ai.CallDeferred`: the app renders a
card for the pending tool call and submits the user's answer as the tool's
result, and the run resumes with it. They take the toolkit context like every
other tool so :func:`~interloper_agent.toolset.bind` treats them alike.
"""

from __future__ import annotations

from interloper_toolkit import ToolkitContext, collection
from interloper_toolkit.models import ConnectionSetup, ToolError
from pydantic_ai import CallDeferred

#: The most options one selection card carries.
_MAX_CHOICES = 50


def request_user_selection(
    ctx: ToolkitContext, prompt: str, options: list[dict[str, str]], multi: bool = False
) -> ToolError:
    """Ask the user to pick from known options, through a card in the app.

    Use this instead of listing choices as text whenever the user must pick
    from known options — an account from a provider, assets to enable. The
    app renders checkboxes (``multi``) or radios; the user's choice comes back
    as this tool's result, ``{"selected": [<value>, ...]}``.

    Args:
        prompt: Short question shown above the choices (e.g. "Which ad
            account should this source use?").
        options: The choices, each ``{"label": ..., "value": ...}`` — pass
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
    if len(cleaned) > _MAX_CHOICES:
        return ToolError(error=f"Too many options ({len(cleaned)} > {_MAX_CHOICES}) — narrow them down first")
    raise CallDeferred()


def request_connection_setup(
    ctx: ToolkitContext, connection_key: str, name: str | None = None, force_new: bool = False
) -> ConnectionSetup | ToolError:
    """Let the user create a connection through the app's secure form.

    Call this to let the user create a connection: the app renders the form
    for the given definition (OAuth sign-in when available, manual credential
    entry otherwise) and the credentials go directly to the API; never ask the
    user to share credentials in the chat instead. The created connection
    comes back as this tool's result, ``{"connection_id": ..., "name": ...,
    "verified": bool}``, and you continue from there (a source, a schedule).

    When the collection already holds connections of this definition, no form
    is presented: the result lists them under ``existing`` so you can ask the
    user whether to reuse one — call again with ``force_new`` only when they
    want another account connected. An unknown key fails with the list of
    valid connection keys.

    Args:
        connection_key: Catalog key of the connection definition — usually
            ``<source_key>_connection`` (e.g. 'facebook_ads_connection').
        name: Optional display name to prefill in the form.
        force_new: Present the form even though fitting connections exist.
    """
    result = collection.request_connection_setup(ctx, connection_key, name, force_new)
    if isinstance(result, ToolError) or result.existing:
        return result
    raise CallDeferred()

