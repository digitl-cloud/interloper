"""Tests for ``interloper_agent.settings``."""

import pytest
from pydantic import ValidationError

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


def test_the_agent_is_opt_in_and_names_no_model():
    """An unconfigured deployment serves no agent and has no model chosen for it."""
    settings = AgentSettings()

    assert (settings.enabled, settings.model) == (False, None)


def test_an_enabled_agent_runs_on_the_model_the_environment_names(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_AGENT_* env vars reach the agent section."""
    monkeypatch.setenv("INTERLOPER_AGENT_ENABLED", "true")
    monkeypatch.setenv("INTERLOPER_AGENT_MODEL", "anthropic:claude-sonnet-4-5")

    settings = AgentSettings.get()

    assert (settings.enabled, settings.model) == (True, "anthropic:claude-sonnet-4-5")


def test_an_enabled_agent_without_a_model_fails_naming_the_variable(monkeypatch: pytest.MonkeyPatch):
    """Enabling the agent without a model is a configuration error, not a silent default."""
    monkeypatch.setenv("INTERLOPER_AGENT_ENABLED", "true")

    with pytest.raises(ValidationError, match="INTERLOPER_AGENT_MODEL"):
        AgentSettings.get()


def test_a_disabled_agent_needs_no_model():
    """The model is only required to serve the agent."""
    assert AgentSettings(enabled=False).model is None
