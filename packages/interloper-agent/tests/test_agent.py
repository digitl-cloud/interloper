"""Tests for ``interloper_agent.agent``."""

from __future__ import annotations

import datetime

import pytest
from interloper_toolkit import ToolkitContext
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.messages import ModelRequest
from pydantic_ai.models.anthropic import AnthropicCompaction
from pydantic_ai.models.openai import OpenAICompaction
from pydantic_ai.models.test import TestModel

from interloper_agent import TURN_LIMITS, build_agent
from interloper_agent.agent import COMPACTION_THRESHOLD, capabilities, model_settings


class TestBuildAgent:
    def test_a_model_without_a_provider_is_refused_up_front(self):
        with pytest.raises(ValueError, match="provider:model"):
            build_agent("gemini-2.5-flash")

    def test_builds_without_credentials_and_names_the_model(self):
        agent = build_agent("google:gemini-2.5-flash")

        assert str(agent.model) == "google:gemini-2.5-flash"
        assert TURN_LIMITS.request_limit is not None

    def test_a_turn_carries_the_instructions_and_the_current_time(self, ctx: ToolkitContext):
        agent = build_agent("google:gemini-2.5-flash")

        with agent.override(model=TestModel(call_tools=[], custom_output_text="hello")):
            result = agent.run_sync("hi", deps=ctx)

        first = result.all_messages()[0]
        assert isinstance(first, ModelRequest)
        assert first.instructions is not None
        assert "Interloper Assistant" in first.instructions
        assert f"Current date and time: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d}" in first.instructions
        assert result.output == "hello"


class TestModelSettings:
    def test_thinking_is_asked_for_per_provider(self):
        assert model_settings("google:gemini-2.5-flash") == {"google_thinking_config": {"include_thoughts": True}}
        assert model_settings("anthropic:claude-sonnet-4-5") == {"anthropic_thinking": {"type": "adaptive"}}
        assert model_settings("openai:gpt-5") is None


class TestCapabilities:
    def test_every_provider_elides_and_only_native_ones_compact(self):
        google = capabilities("google:gemini-2.5-flash")
        anthropic = capabilities("anthropic:claude-sonnet-4-5")
        openai = capabilities("openai:gpt-5")
        chat = capabilities("openai-chat:gpt-5")

        assert [type(c) for c in google] == [ProcessHistory]
        assert [type(c) for c in anthropic] == [ProcessHistory, AnthropicCompaction]
        assert [type(c) for c in openai] == [ProcessHistory, OpenAICompaction]
        assert [type(c) for c in chat] == [ProcessHistory]

    def test_anthropic_compacts_at_the_threshold_and_openai_at_its_default(self):
        anthropic = capabilities("anthropic:claude-sonnet-4-5")[1]
        openai = capabilities("openai:gpt-5")[1]

        assert isinstance(anthropic, AnthropicCompaction) and anthropic.token_threshold == COMPACTION_THRESHOLD
        assert isinstance(openai, OpenAICompaction) and not openai.stateless and openai.token_threshold is None
