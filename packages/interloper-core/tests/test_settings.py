"""Tests for runtime settings."""

import pytest

from interloper.settings import AgentSettings, AppSettings, AuthSettings, TelemetrySettings


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered.

    ``AppSettings`` resolves its YAML source relative to the working
    directory, so without this these tests assert against whatever the
    repo-root config happens to contain — a block added there silently
    overrides the env vars under test.
    """
    monkeypatch.chdir(tmp_path)


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


def test_auth_settings_allowed_domains_default():
    """Signup stays open (empty allowlist) unless configured."""
    settings = AuthSettings()

    assert settings.allowed_domains == []


def test_auth_settings_allowed_domains_env(monkeypatch: pytest.MonkeyPatch):
    """Comma-separated env list; entries trimmed, lowercased, leading @ stripped."""
    monkeypatch.setenv("INTERLOPER_AUTH_ALLOWED_DOMAINS", " Digitlcloud.com ,@example.com,")

    settings = AppSettings()

    assert settings.auth.allowed_domains == ["digitlcloud.com", "example.com"]


def test_telemetry_settings_defaults():
    """Telemetry is off by default; both signals default on once enabled."""
    settings = TelemetrySettings()

    assert settings.enabled is False
    assert settings.protocol == "grpc"
    assert settings.traces is True
    assert settings.metrics is True
    assert settings.sample_ratio == 1.0


def test_telemetry_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_OTEL_* env vars reach the nested otel settings."""
    monkeypatch.setenv("INTERLOPER_OTEL_ENABLED", "true")
    monkeypatch.setenv("INTERLOPER_OTEL_ENDPOINT", "http://collector:4317")
    monkeypatch.setenv("INTERLOPER_OTEL_PROTOCOL", "http/protobuf")
    monkeypatch.setenv("INTERLOPER_OTEL_SAMPLE_RATIO", "0.25")

    settings = AppSettings()

    assert settings.otel.enabled is True
    assert settings.otel.endpoint == "http://collector:4317"
    assert settings.otel.protocol == "http/protobuf"
    assert settings.otel.sample_ratio == 0.25


def test_telemetry_metric_export_interval_default():
    """Delta temporality makes this a freshness knob, so the SDK default stands."""
    settings = TelemetrySettings()

    assert settings.metric_export_interval == 60


def test_telemetry_metric_export_interval_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_OTEL_METRIC_EXPORT_INTERVAL reaches the nested otel settings."""
    monkeypatch.setenv("INTERLOPER_OTEL_METRIC_EXPORT_INTERVAL", "5")

    settings = AppSettings()

    assert settings.otel.metric_export_interval == 5
