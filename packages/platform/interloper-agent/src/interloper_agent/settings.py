"""The agent package's settings section."""

from __future__ import annotations

from typing import ClassVar

from interloper.settings import Settings
from pydantic import Field


class AgentSettings(Settings):
    """AI agent settings: the chat assistant the API serves.

    The agent is only available when the ``agent`` extra is installed
    (``interloper-api[agent]``).
    """

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "model"})

    enabled: bool = Field(default=True, description="Serve the agent; off leaves the installation untouched.")
    model: str = Field(
        default="google:gemini-2.5-flash",
        description="pydantic-ai `provider:model` name (`google:gemini-2.5-flash`, `google-cloud:gemini-2.5-flash` "
        "for Vertex, `anthropic:claude-sonnet-4-5`, `openai:gpt-5`); provider credentials come from the provider's "
        "standard environment variables.",
    )
