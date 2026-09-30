"""What the model sees of a long conversation.

The model works from the last :data:`RETAINED_TURNS` turns; everything
older is dead weight re-sent with every request, in two sizes:

- A turn's bulk is its tool returns: a page of components, a run's events.
  The model read such a return once and answered from it, and the answer
  stays. :func:`elide_tool_returns` replaces the large returns of older
  turns with a stub that names the tool, so the call and its answer stay
  paired and the model can call again if it needs the detail.
- Once the history itself grows past :data:`COMPACTION_THRESHOLD` input
  tokens, :func:`summarise` replaces the older turns with a summary the
  model writes for itself, carried as a system prompt at the head of the
  history: every provider folds those into its system instruction, and the
  app reads it back through :func:`summary_of`. Providers with native
  compaction do this server-side instead (see
  :func:`~interloper_agent.agent.capabilities`).

pydantic-ai writes the processed history back into the run, so the stored
conversation shrinks with it.
"""

from __future__ import annotations

import dataclasses
from typing import cast

from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import Model

RETAINED_TURNS = 3
"""How many of the latest turns the model always sees whole."""

ELISION_THRESHOLD = 1_000
"""The rendered size, in characters, above which an older tool return is elided."""

COMPACTION_THRESHOLD = 100_000
"""Input tokens past which the older turns are summarised.

Half of the smallest window in use (Claude's 200k): room for a long,
tool-heavy turn after the summary.
"""

SUMMARY_LEAD = "Summary of the conversation so far:\n\n"
"""What precedes the summary text in the system prompt that carries it."""

SUMMARY_INSTRUCTIONS = """\
You are summarising a conversation between a user and Interloper Assistant,
for the assistant itself to continue it. Write in the third person, in
prose, at most 300 words. Keep every fact the assistant still needs: what
the user wants and decided, components, jobs and runs by name, ids the
assistant was given, numbers and errors it reported, and what remains open.
Drop pleasantries, reasoning, and the detail of tool results already acted
on. Never invent anything that is not in the transcript.
"""

TRANSCRIPT_RETURN_LIMIT = 500
"""How much of a tool return the summariser's transcript carries."""


def elide_tool_returns(messages: list[ModelMessage]) -> list[ModelMessage]:
    """Replace the large tool returns of older turns with a stub.

    Before the retained turns, every tool return rendering longer than
    :data:`ELISION_THRESHOLD` characters becomes a stub with the same tool
    name and call id. Small returns, errors among them, are kept: they are
    cheap and often explain why the conversation turned.

    Args:
        messages: The history, as pydantic-ai hands it to a history processor.

    Returns:
        The history with older tool returns elided; the input is not mutated.
    """
    boundary = _retained_from(messages)
    return [_elide(message) if i < boundary else message for i, message in enumerate(messages)]


async def summarise(ctx: RunContext[ToolkitContext], messages: list[ModelMessage]) -> list[ModelMessage]:
    """Replace the older turns with a summary once the history is too large.

    The trigger is the input token count the model reported on its latest
    response, against :data:`COMPACTION_THRESHOLD`. The summary is written
    by the run's own model from a compact transcript of the older turns
    (a previous summary included), and carried as a system prompt at the
    head of the returned history, ahead of the retained turns.

    Args:
        ctx: The run context, for the model that writes the summary.
        messages: The history, as pydantic-ai hands it to a history processor.

    Returns:
        The history, summarised when over the threshold, else as given.
    """
    boundary = _retained_from(messages)
    if boundary == 0 or _input_tokens(messages) < COMPACTION_THRESHOLD:
        return messages
    summariser = Agent[None, str](cast(Model, ctx.model), instructions=SUMMARY_INSTRUCTIONS, output_type=str)
    result = await summariser.run(_transcript(messages[:boundary]))
    return [ModelRequest(parts=[SystemPromptPart(content=SUMMARY_LEAD + result.output)]), *messages[boundary:]]


def summary_of(messages: list[ModelMessage]) -> str | None:
    """The summary a history starts with, when it was compacted.

    Args:
        messages: A conversation's history.

    Returns:
        The summary text, or ``None`` when the history is whole.
    """
    if not messages:
        return None
    first = messages[0]
    if isinstance(first, ModelRequest) and any(isinstance(part, SystemPromptPart) for part in first.parts):
        content = "\n".join(part.content for part in first.parts if isinstance(part, SystemPromptPart))
        return content.removeprefix(SUMMARY_LEAD)
    return None


def _retained_from(messages: list[ModelMessage]) -> int:
    """The index where the retained turns start.

    A turn starts at the request carrying the user's prompt.

    Args:
        messages: The history.

    Returns:
        The index of the first retained message; ``0`` when the history is
        short enough to be retained whole.
    """
    starts = [i for i, message in enumerate(messages) if _starts_turn(message)]
    return starts[-RETAINED_TURNS] if len(starts) > RETAINED_TURNS else 0


def _starts_turn(message: ModelMessage) -> bool:
    """Whether a message opens a turn, carrying the user's prompt.

    Args:
        message: A message of the history.

    Returns:
        True for a request with a user prompt part.
    """
    return isinstance(message, ModelRequest) and any(isinstance(part, UserPromptPart) for part in message.parts)


def _input_tokens(messages: list[ModelMessage]) -> int:
    """The input tokens the model reported on its latest response.

    Args:
        messages: The history.

    Returns:
        The count, or ``0`` when no response reported usage yet.
    """
    for message in reversed(messages):
        if isinstance(message, ModelResponse) and message.usage.input_tokens:
            return message.usage.input_tokens
    return 0


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


def _stub(part: ToolReturnPart) -> dict[str, str]:
    """The result standing in for an elided tool return.

    It speaks the toolkit's ``status`` envelope, so the app can tell an
    elided result from a real one the way it tells errors.

    Args:
        part: The tool return being elided.

    Returns:
        The stub result.
    """
    return {
        "status": "elided",
        "message": f"The {part.tool_name} result was dropped from the history; call the tool again if you need it.",
    }


def _transcript(messages: list[ModelMessage]) -> str:
    """Render older turns as the text the summariser reads.

    Tool calls appear by name and arguments, tool returns clipped to
    :data:`TRANSCRIPT_RETURN_LIMIT` characters; thinking is left out.

    Args:
        messages: The messages to summarise.

    Returns:
        The transcript, one line per part.
    """
    lines: list[str] = []
    for message in messages:
        for part in message.parts:
            if isinstance(part, SystemPromptPart):
                lines.append(part.content)
            elif isinstance(part, UserPromptPart) and isinstance(part.content, str):
                lines.append(f"User: {part.content}")
            elif isinstance(part, TextPart):
                lines.append(f"Assistant: {part.content}")
            elif isinstance(part, ToolCallPart):
                lines.append(f"Assistant called {part.tool_name}({part.args_as_json_str()})")
            elif isinstance(part, ToolReturnPart):
                lines.append(f"{part.tool_name} returned: {part.model_response_str()[:TRANSCRIPT_RETURN_LIMIT]}")
    return "\n".join(lines)
