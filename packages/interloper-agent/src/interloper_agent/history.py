"""What the model sees of a long conversation.

A turn's bulk is its tool returns: a page of components, a run's events, an
error breakdown. The model reads such a return once, answers from it, and
the answer stays in the history; the return itself is dead weight from then
on, re-sent with every later request. :func:`elide_tool_returns` replaces
the large returns of older turns with a stub that names the tool, so the
call and its answer stay paired and the model can call again if it needs
the detail. The last turns are kept whole: the model may still be working
from what it just fetched.

pydantic-ai writes the processed history back into the run, so the stored
conversation shrinks with it.
"""

from __future__ import annotations

import dataclasses

from pydantic_ai.messages import ModelMessage, ModelRequest, ToolReturnPart, UserPromptPart

RETAINED_TURNS = 3
"""How many of the latest turns keep their tool returns whole."""

ELISION_THRESHOLD = 1_000
"""The rendered size, in characters, above which an older tool return is elided."""


def elide_tool_returns(messages: list[ModelMessage]) -> list[ModelMessage]:
    """Replace the large tool returns of older turns with a stub.

    A turn starts at the request carrying the user's prompt. The last
    :data:`RETAINED_TURNS` turns are returned untouched; before them, every
    tool return rendering longer than :data:`ELISION_THRESHOLD` characters
    becomes a one-line stub with the same tool name and call id. Small
    returns, errors among them, are kept: they are cheap and often explain
    why the conversation turned.

    Args:
        messages: The history, as pydantic-ai hands it to a history processor.

    Returns:
        The history with older tool returns elided; the input is not mutated.
    """
    starts = [i for i, message in enumerate(messages) if _starts_turn(message)]
    boundary = starts[-RETAINED_TURNS] if len(starts) >= RETAINED_TURNS else 0
    return [_elide(message) if i < boundary else message for i, message in enumerate(messages)]


def _starts_turn(message: ModelMessage) -> bool:
    """Whether a message opens a turn, carrying the user's prompt.

    Args:
        message: A message of the history.

    Returns:
        True for a request with a user prompt part.
    """
    return isinstance(message, ModelRequest) and any(isinstance(part, UserPromptPart) for part in message.parts)


def _elide(message: ModelMessage) -> ModelMessage:
    """Elide the large tool returns of one message.

    Args:
        message: A message of the history, before the retained turns.

    Returns:
        The message with its large tool returns stubbed, or the message itself
        when it carries none.
    """
    if not isinstance(message, ModelRequest):
        return message
    parts = [
        dataclasses.replace(part, content=_stub(part))
        if isinstance(part, ToolReturnPart) and len(part.model_response_str()) > ELISION_THRESHOLD
        else part
        for part in message.parts
    ]
    return message if parts == list(message.parts) else dataclasses.replace(message, parts=parts)


def _stub(part: ToolReturnPart) -> str:
    """The text standing in for an elided tool return.

    Args:
        part: The tool return being elided.

    Returns:
        A one-line stub naming the tool.
    """
    return f"({part.tool_name} result elided from the history; call the tool again if you need it)"
