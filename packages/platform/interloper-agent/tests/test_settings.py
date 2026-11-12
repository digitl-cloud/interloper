"""Tests for ``interloper_agent.settings``."""

import pytest

from interloper_agent.settings import AgentSettings


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered."""
    monkeypatch.chdir(tmp_path)


def test_the_agent_package_declares_its_section():
    """The section is read from the ``agent`` block and the ``INTERLOPER_AGENT_`` variables."""
    assert AgentSettings.section() == "agent"
    assert AgentSettings.declared()["agent"].extension == "agent"
    assert {name.partition(".")[0] for name in AgentSettings.public} <= set(AgentSettings.model_fields)


def test_agent_settings_defaults():
    """Agent defaults: enabled, native Gemini model."""
    settings = AgentSettings()

    assert settings.enabled is True
    assert settings.model == "google:gemini-2.5-flash"


def test_agent_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_AGENT_* env vars reach the agent section."""
    monkeypatch.setenv("INTERLOPER_AGENT_ENABLED", "false")
    monkeypatch.setenv("INTERLOPER_AGENT_MODEL", "anthropic/claude-sonnet-4-5")

    settings = AgentSettings.get()

    assert settings.enabled is False
    assert settings.model == "anthropic/claude-sonnet-4-5"
