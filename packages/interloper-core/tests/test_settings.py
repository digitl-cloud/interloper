"""Tests for runtime settings."""

import pytest

from interloper.settings import AgentSettings, AppSettings, AuthSettings


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


def test_auth_settings_super_admin_emails_default():
    """No super-admin emails unless configured."""
    settings = AuthSettings()

    assert settings.super_admin_emails == []


def test_auth_settings_super_admin_emails_env(monkeypatch: pytest.MonkeyPatch):
    """The env var takes a comma-separated list; entries are trimmed and lowercased."""
    monkeypatch.setenv("INTERLOPER_AUTH_SUPER_ADMIN_EMAILS", " Admin@Example.com ,second@example.com,")

    settings = AppSettings()

    assert settings.auth.super_admin_emails == ["admin@example.com", "second@example.com"]


def test_auth_settings_super_admin_emails_list():
    """A YAML/init list passes through, normalised to lowercase."""
    settings = AuthSettings(super_admin_emails=["Admin@Example.com"])

    assert settings.super_admin_emails == ["admin@example.com"]
