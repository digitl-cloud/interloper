"""The agent package's settings section."""

from __future__ import annotations

from typing import ClassVar, Self

from interloper.settings import Settings
from pydantic import Field, model_validator


class AgentSettings(Settings):
    """AI agent settings: the chat assistant the API serves.

    The agent is opt-in, and enabling it takes a model: none is chosen for the
    deployment. It is only available when the ``agent`` extra is installed
    (``interloper-api[agent]``).
    """

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "model"})

    enabled: bool = Field(default=False, description="Serve the agent; enabling it requires `model`.")
    model: str | None = Field(
        default=None,
        description="pydantic-ai `provider:model` name, required when the agent is enabled (`google:gemini-2.5-flash`, "
        "`google-cloud:gemini-2.5-flash` for Vertex, `anthropic:claude-sonnet-4-5`, `openai:gpt-5`); provider "
        "credentials come from the provider's standard environment variables.",
    )

    @model_validator(mode="after")
    def _model_when_enabled(self) -> Self:
        """Refuse an enabled agent without a model.

        Returns:
            The section, unchanged.

        Raises:
            ValueError: When the agent is enabled and names no model.
        """
        if self.enabled and not self.model:
            raise ValueError(
                "agent.model is required when the agent is enabled: name a pydantic-ai provider:model in "
                "INTERLOPER_AGENT_MODEL (or the agent block's model), or disable the agent"
            )
        return self
