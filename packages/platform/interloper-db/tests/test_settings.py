"""Tests for ``interloper_db.settings``."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from interloper_db.settings import EncryptionSettings, PostgresSettings, QuotaSettings


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered."""
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(
    ("section", "name"), [(EncryptionSettings, "encryption"), (PostgresSettings, "postgres"), (QuotaSettings, "quota")]
)
def test_the_database_package_declares_its_own_sections(section, name):
    """Each section is read from the block and the variables of the name this package declares."""
    assert section.section() == name
    assert section.declared()[name].extension == "db"


@pytest.mark.parametrize("section", [EncryptionSettings, PostgresSettings, QuotaSettings])
def test_every_public_name_reaches_a_field(section):
    """A public name is a field of its section."""
    assert {name.partition(".")[0] for name in section.public} <= set(section.model_fields)


def test_encryption_key_env(monkeypatch: pytest.MonkeyPatch):
    """``encryption.key`` reads ``INTERLOPER_ENCRYPTION_KEY``."""
    monkeypatch.setenv("INTERLOPER_ENCRYPTION_KEY", "key-material")

    assert EncryptionSettings.get().key == "key-material"


def test_postgres_statement_timeout_defaults_to_unset():
    """No statement timeout unless configured, so existing deployments keep their behaviour."""
    assert PostgresSettings().statement_timeout is None


def test_postgres_statement_timeout_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_POSTGRES_STATEMENT_TIMEOUT reaches the postgres section, in seconds."""
    monkeypatch.setenv("INTERLOPER_POSTGRES_STATEMENT_TIMEOUT", "2.5")

    assert PostgresSettings.get().statement_timeout == 2.5


def test_postgres_statement_timeout_must_be_positive():
    """Zero would disable the cap in Postgres, so it is rejected instead of silently meaning 'off'."""
    with pytest.raises(ValidationError):
        PostgresSettings(statement_timeout=0)


def test_dsn_escapes_user_and_password():
    """Reserved characters in the credentials do not break the connection string."""
    settings = PostgresSettings(user="me@corp", password="p@ss:w/rd%", host="db", port=5432, database="interloper")

    assert settings.dsn == "postgresql+psycopg://me%40corp:p%40ss%3Aw%2Frd%25@db:5432/interloper"


def test_dsn_names_the_psycopg_driver():
    """A bare ``postgresql://`` would let a SQLAlchemy release pick the driver; 2.1 switched it silently."""
    assert make_url(PostgresSettings().dsn).drivername == "postgresql+psycopg"


def test_location_names_the_target_without_credentials():
    """``location`` is what a message may show: host, port and database."""
    settings = PostgresSettings(user="me", password="secret", host="db", port=5433, database="warehouse")

    assert settings.location == "db:5433/warehouse"
    assert "secret" not in settings.location


class TestConfigured:
    """A Postgres is configured once the config file or the environment sets any of its fields."""

    @pytest.fixture(autouse=True)
    def _no_ambient_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Clear any ``INTERLOPER_POSTGRES_*`` variable the shell running the tests set.

        Args:
            monkeypatch: Fixture used to clear the variables.
        """
        for name in [name for name in os.environ if name.startswith("INTERLOPER_POSTGRES_")]:
            monkeypatch.delenv(name)

    def test_defaults_alone_configure_none(self) -> None:
        assert PostgresSettings().configured is False

    def test_an_environment_variable_configures_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INTERLOPER_POSTGRES_HOST", "db.internal")

        assert PostgresSettings().configured is True

    def test_a_config_block_setting_a_field_configures_one(self, tmp_path: Path) -> None:
        (tmp_path / "interloper.yaml").write_text("postgres:\n  database: interloper\n")

        assert PostgresSettings().configured is True

    def test_an_empty_config_block_configures_none(self, tmp_path: Path) -> None:
        (tmp_path / "interloper.yaml").write_text("postgres:\n")

        assert PostgresSettings().configured is False
