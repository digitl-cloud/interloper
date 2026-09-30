"""The assistant: one pydantic-ai agent over the toolkit."""

from __future__ import annotations

import datetime

from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent, DeferredToolRequests, UsageLimits
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.models.anthropic import AnthropicModelSettings
from pydantic_ai.models.google import GoogleModelSettings
from pydantic_ai.settings import ModelSettings

from interloper_agent.history import elide_tool_returns
from interloper_agent.prompts import INSTRUCTIONS
from interloper_agent.toolset import toolset

TURN_LIMITS = UsageLimits(request_limit=40)
"""What one turn may spend: enough for a long setup flow, a bound on a loop that never converges."""


def build_agent(model: str) -> Agent[ToolkitContext, str | DeferredToolRequests]:
    """Build the assistant for a model.

    The model is named the pydantic-ai way, ``provider:model`` (e.g.
    ``google:gemini-2.5-flash``, ``anthropic:claude-sonnet-4-5``); its client
    is created on the first run, so building needs no credentials. A turn
    ends either with the answer or with :class:`DeferredToolRequests`: the
    tool calls waiting for the user's approval or for an answer the app
    collects (a selection, a connection set up in the secure form). Before
    each request the history is trimmed of older tool returns
    (:mod:`interloper_agent.history`), which is what keeps a long-lived
    conversation affordable.

    Args:
        model: The ``provider:model`` name.

    Returns:
        The agent, ready to run with a :class:`ToolkitContext` as deps.

    Raises:
        ValueError: If *model* names no provider, which pydantic-ai would
            only reject on the first turn.
    """
    if ":" not in model:
        raise ValueError(
            f"agent.model {model!r} names no provider; use pydantic-ai's provider:model form, "
            "e.g. google:gemini-2.5-flash, google-cloud:gemini-2.5-flash, anthropic:claude-sonnet-4-5"
        )
    agent = Agent[ToolkitContext, str | DeferredToolRequests](
        model,
        name="interloper",
        deps_type=ToolkitContext,
        output_type=[str, DeferredToolRequests],
        instructions=INSTRUCTIONS,
        toolsets=[toolset()],
        capabilities=[ProcessHistory(elide_tool_returns)],
        model_settings=model_settings(model),
        defer_model_check=True,
    )

    @agent.instructions
    def current_time() -> str:
        # The model has no reliable notion of "now"; without this the relative
        # timestamps the presentation rules ask for drift to its training data.
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"Current date and time: {now:%Y-%m-%d %H:%M} UTC. Compute relative timestamps from it."

    return agent


def model_settings(model: str) -> ModelSettings | None:
    """The settings that surface the model's reasoning to the app.

    The app shows thought summaries while a turn is in flight, which is the
    only account of a long turn the model can give. Each provider asks for
    them differently; a provider with no such switch gets none.

    Args:
        model: The ``provider:model`` name.

    Returns:
        The provider's settings, or ``None``.
    """
    provider, _, _ = model.partition(":")
    if provider in ("google", "google-cloud"):
        return GoogleModelSettings(google_thinking_config={"include_thoughts": True})
    if provider == "anthropic":
        return AnthropicModelSettings(anthropic_thinking={"type": "adaptive"})
    return None
