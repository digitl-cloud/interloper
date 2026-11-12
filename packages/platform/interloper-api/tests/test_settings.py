"""Tests for ``interloper_api.settings``."""

import sys
from datetime import timedelta

import pytest
from interloper.settings import ServerSettings
from interloper_agent.settings import AgentSettings
from interloper_db.settings import EncryptionSettings

from interloper_api.settings import ApiSettings, AuthSettings, SmtpSettings


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered."""
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(("section", "name"), [(AuthSettings, "auth"), (SmtpSettings, "smtp")])
def test_the_api_declares_its_own_sections(section, name):
    """Each section is read from the block and the variables of the name the API declares."""
    assert section.section() == name
    assert section.declared()[name].extension == "api"


@pytest.mark.parametrize("section", [AuthSettings, SmtpSettings])
def test_every_public_name_reaches_a_field(section):
    """A public name is a field of its section."""
    assert {name.partition(".")[0] for name in section.public} <= set(section.model_fields)


def test_the_api_reads_each_section_as_the_process_runs_with_it():
    """The bundle holds the active sections, the agent's included when its package is installed."""
    server = ServerSettings(port=3100)
    ServerSettings.activate(server)
    EncryptionSettings.activate(EncryptionSettings(key="key-material"))

    settings = ApiSettings.get()

    assert settings.server is server
    assert settings.encryption.key == "key-material"
    assert isinstance(settings.agent, AgentSettings)


def test_the_api_reads_no_agent_section_where_the_agent_package_is_not_installed(monkeypatch: pytest.MonkeyPatch):
    """An API installed without the agent holds no agent section."""
    monkeypatch.setitem(sys.modules, "interloper_agent.settings", None)

    assert ApiSettings.get().agent is None


def test_auth_settings_super_admin_emails_default():
    """No super-admin emails unless configured."""
    assert AuthSettings().super_admin_emails == []


def test_auth_settings_super_admin_emails_env(monkeypatch: pytest.MonkeyPatch):
    """The env var takes a comma-separated list; entries are trimmed and lowercased."""
    monkeypatch.setenv("INTERLOPER_AUTH_SUPER_ADMIN_EMAILS", " Admin@Example.com ,second@example.com,")

    assert AuthSettings.get().super_admin_emails == ["admin@example.com", "second@example.com"]


def test_auth_settings_super_admin_emails_list():
    """A YAML/init list passes through, normalised to lowercase."""
    assert AuthSettings(super_admin_emails=["Admin@Example.com"]).super_admin_emails == ["admin@example.com"]


def test_auth_settings_token_lifetimes_are_durations():
    """Each lifetime reads as the duration its field states, in its field's unit."""
    settings = AuthSettings(session_expiry_days=2, access_token_expiry_minutes=15, refresh_token_expiry_days=7)

    assert (settings.session_expiry, settings.access_token_expiry, settings.refresh_token_expiry) == (
        timedelta(days=2),
        timedelta(minutes=15),
        timedelta(days=7),
    )


def test_auth_settings_allowed_domains_default():
    """Signup stays open (empty allowlist) unless configured."""
    assert AuthSettings().allowed_domains == []


def test_auth_settings_allowed_domains_env(monkeypatch: pytest.MonkeyPatch):
    """Comma-separated env list; entries trimmed, lowercased, leading @ stripped."""
    monkeypatch.setenv("INTERLOPER_AUTH_ALLOWED_DOMAINS", " Digitlcloud.com ,@example.com,")

    assert AuthSettings.get().allowed_domains == ["digitlcloud.com", "example.com"]


def test_smtp_is_enabled_once_host_user_and_password_are_set():
    """Sending needs the three; any one missing keeps it off."""
    assert SmtpSettings(host="smtp.example.com", user="me", password="secret").enabled is True
    assert SmtpSettings(host="smtp.example.com", user="me").enabled is False
