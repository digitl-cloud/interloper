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


class OAuthSettings(BaseSettings):
    """Per-connector OAuth *app* credentials for the data-source connect flow.

    These are the ``client_id`` / ``client_secret`` / ``redirect_uri`` of the
    OAuth apps used to authorize external ad-platform connectors (distinct from
    the Google app-login credentials in :class:`AuthSettings`). One trio per
    provider; a provider is only offered in the connect UI once all three are set.

    ``client_id`` and ``redirect_uri`` are public and can live in YAML config;
    ``client_secret`` is sensitive and should come from the environment, e.g.
    ``INTERLOPER_OAUTH_AMAZON_CLIENT_SECRET``.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}OAUTH_")

    amazon_client_id: str = ""
    amazon_client_secret: str = ""
    amazon_redirect_uri: str = ""

    criteo_client_id: str = ""
    criteo_client_secret: str = ""
    criteo_redirect_uri: str = ""

    facebook_client_id: str = ""
    facebook_client_secret: str = ""
    facebook_redirect_uri: str = ""

    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = ""

    linkedin_client_id: str = ""
    linkedin_client_secret: str = ""
    linkedin_redirect_uri: str = ""

    microsoft_client_id: str = ""
    microsoft_client_secret: str = ""
    microsoft_redirect_uri: str = ""

    pinterest_client_id: str = ""
    pinterest_client_secret: str = ""
    pinterest_redirect_uri: str = ""

    snapchat_client_id: str = ""
    snapchat_client_secret: str = ""
    snapchat_redirect_uri: str = ""

    tiktok_client_id: str = ""
    tiktok_client_secret: str = ""
    tiktok_redirect_uri: str = ""


class SecretsSettings(BaseSettings):
    """Secrets used to protect sensitive data at rest.

    ``encryption_key`` enables symmetric encryption of resource ``data`` blobs
    marked ``encrypted=True``. It is read from ``SECRETS_ENCRYPTION_KEY`` (no
    ``INTERLOPER_`` prefix) to match the Helm chart and runner launchers, which
    forward that exact variable into spawned containers.
    """

    model_config = SettingsConfigDict(env_prefix="SECRETS_")

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

    Supported types: ``serial``, ``multi_thread``, ``multi_process``, ``async``.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}RUNNER_")

    type: str = "multi_thread"
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
    oauth: OAuthSettings = Field(default_factory=OAuthSettings)
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
        return cls()  # type: ignore[call-arg]

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
