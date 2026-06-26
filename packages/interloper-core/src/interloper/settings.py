"""CLI runtime settings via Pydantic Settings.

Loads settings from three sources with this priority (highest first):

1. Environment variables
2. ``interloper.yaml`` in the current directory
3. Field defaults

Each section has its own env prefix so field names with underscores
are unambiguous.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource

PREFIX = "INTERLOPER_"


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}POSTGRES_")

    host: str = "localhost"
    port: int = 5432
    user: str = ""
    password: str = ""
    database: str = "interloper"

    @property
    def dsn(self) -> str:
        """Assemble a PostgreSQL connection string from individual fields."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class AuthSettings(BaseSettings):
    """Authentication settings (Google OAuth, cookies)."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}AUTH_")

    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = ""
    cookie_secure: bool = True
    session_expiry_days: int = 30


class SecretsSettings(BaseSettings):
    """Secrets used to protect sensitive data at rest.

    ``encryption_key`` enables symmetric encryption of resource ``data`` blobs.
    Encryption is the default, so a key is required to persist resources: with
    none set, writes fail closed (rejected) rather than storing plaintext. It is
    read from ``INTERLOPER_ENCRYPTION_KEY``; the Helm chart and runner launchers
    forward that exact variable into spawned containers.
    """

    model_config = SettingsConfigDict(env_prefix=PREFIX)

    encryption_key: str = ""


class ServerSettings(BaseSettings):
    """HTTP server settings (API + frontend)."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}SERVER_")

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 3000


class CronSettings(BaseSettings):
    """Cron controller settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}CRON_")

    enabled: bool = True
    reconcile_interval: int = 10
    max_execution_delay: int | None = None
    batch_size: int = 50


class SmtpSettings(BaseSettings):
    """SMTP settings for sending invitation emails."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}SMTP_")

    host: str = ""
    port: int = 587
    user: str = ""
    password: str = ""
    from_addr: str = "noreply@interloper.dev"

    @property
    def enabled(self) -> bool:
        """SMTP is enabled when host, user, and password are all set."""
        return bool(self.host and self.user and self.password)


class LauncherSettings(BaseSettings):
    """Launcher settings (type + launcher-specific config)."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}LAUNCHER_")

    type: str = "in_process"
    config: dict[str, Any] = Field(default_factory=dict)


class RunnerSettings(BaseSettings):
    """Runner settings (type + runner-specific config).

    Built-in types: ``async`` (default, in-process concurrency via ``max_workers``),
    ``serial`` (``async`` with a single slot), ``multi_process``. The ``docker``
    and ``kubernetes`` runners register through their own packages.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}RUNNER_")

    type: str = "async"
    config: dict[str, Any] = Field(default_factory=dict)


class WorkerSettings(BaseSettings):
    """Queue worker settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}WORKER_")

    enabled: bool = True
    poll_interval: int = 5


class ReaperSettings(BaseSettings):
    """Reaper settings (timed-out run cleanup; singleton)."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}REAPER_")

    enabled: bool = True
    timeout: int = 600
    poll_interval: int = 60


class AppSettings(BaseSettings):
    """Top-level runtime settings for the CLI."""

    model_config = SettingsConfigDict(
        env_prefix=PREFIX,
        yaml_file="interloper.yaml",
        yaml_file_encoding="utf-8",
    )

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    secrets: SecretsSettings = Field(default_factory=SecretsSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    cron: CronSettings = Field(default_factory=CronSettings)
    smtp: SmtpSettings = Field(default_factory=SmtpSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    reaper: ReaperSettings = Field(default_factory=ReaperSettings)
    launcher: LauncherSettings = Field(default_factory=LauncherSettings)
    runner: RunnerSettings = Field(default_factory=RunnerSettings)
    catalog: list[str] = Field(default_factory=list)

    _active: ClassVar[AppSettings | None] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        **kwargs: Any,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings sources: init > env > yaml.

        Returns:
            Ordered tuple of settings sources.
        """
        return (
            kwargs["init_settings"],
            kwargs["env_settings"],
            YamlConfigSettingsSource(settings_cls),
        )

    @classmethod
    def from_sources(cls) -> AppSettings:
        """Load settings from YAML + env vars + defaults.

        Returns:
            Fully resolved runtime settings.
        """
        return cls()

    @classmethod
    def get(cls) -> AppSettings:
        """Load settings for the current CLI invocation.

        Returns:
            Active settings (if set), otherwise source-loaded settings.
        """
        return cls._active or cls.from_sources()

    @classmethod
    def activate(cls, settings: AppSettings) -> None:
        """Set active settings for the current CLI invocation."""
        cls._active = settings

    @classmethod
    def clear_active(cls) -> None:
        """Clear active settings for the current CLI invocation."""
        cls._active = None
