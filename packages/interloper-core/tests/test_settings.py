"""Tests for runtime settings."""

import pytest

from interloper.settings import AgentSettings, AppSettings


def test_agent_settings_defaults():
    """Agent defaults: enabled, native Gemini model."""
    settings = AgentSettings()

    assert settings.enabled is True
    assert settings.model == "gemini-2.5-flash"


def test_agent_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_AGENT_* env vars reach the nested agent settings."""
    monkeypatch.setenv("INTERLOPER_AGENT_ENABLED", "false")
    monkeypatch.setenv("INTERLOPER_AGENT_MODEL", "anthropic/claude-sonnet-4-5")

    settings = AppSettings()

    assert settings.agent.enabled is False
    assert settings.agent.model == "anthropic/claude-sonnet-4-5"
