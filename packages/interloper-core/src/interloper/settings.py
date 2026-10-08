"""CLI runtime settings via Pydantic Settings.

Loads settings from three sources with this priority (highest first):

1. Environment variables
2. ``interloper.yaml`` in the current directory
3. Field defaults

Each section has its own env prefix so field names with underscores
are unambiguous. Every field's ``description`` is the settings reference
the documentation site renders, so it is user-facing copy.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    NoDecode,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

PREFIX = "INTERLOPER_"


class RunnerSettings(BaseSettings):
    """Runner settings: the runner type and its own configuration.

    Built-in types: ``async`` (default, in-process concurrency via ``max_workers``),
    ``serial`` (``async`` with a single slot), ``multi_process``. The ``docker``
    and ``kubernetes`` runners register through their own packages.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}RUNNER_")

    type: str = Field(
        default="async",
        description="Registry key of the runner: `async`, `serial`, `multi_process`, or one another package registers.",
    )
    config: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the runner class.")


class TelemetrySettings(BaseSettings):
    """OpenTelemetry settings: OTLP traces and metrics.

    Exporting requires the ``otel`` extra (``interloper[otel]``); when it is
    missing, enabling telemetry logs a warning and stays a no-op.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}OTEL_")

    enabled: bool = Field(
        default=False,
        description="The single master switch; the SDK is never activated from the standard `OTEL_*` variables alone.",
    )
    endpoint: str = Field(
        default="", description="OTLP endpoint; empty falls through to `OTEL_EXPORTER_OTLP_ENDPOINT`."
    )
    protocol: str = Field(default="grpc", description="OTLP protocol: `grpc` or `http/protobuf`.")
    headers: str = Field(
        default="",
        description="OTLP headers as `key=value` pairs; empty falls through to `OTEL_EXPORTER_OTLP_HEADERS`.",
    )
    service_name: str = Field(default="", description="Reported service name; empty reports `interloper`.")
    traces: bool = Field(default=True, description="Export traces.")
    metrics: bool = Field(default=True, description="Export metrics.")
    sample_ratio: float = Field(default=1.0, description="Fraction of traces sampled, parent-based.")
    metric_export_interval: int = Field(default=60, description="Seconds between metric exports.")


class SecretsSettings(BaseSettings):
    """Secrets used to protect sensitive data at rest.

    Encryption is the default, so a key is required to persist resources:
    with none set, writes fail closed rather than storing plaintext. The
    Helm chart and the runner launchers forward ``INTERLOPER_ENCRYPTION_KEY``
    into spawned containers.
    """

    model_config = SettingsConfigDict(env_prefix=PREFIX)

    encryption_key: str = Field(
        default="",
        description="Fernet key encrypting stored resource payloads; required by the platform to persist resources.",
    )


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}POSTGRES_")

    host: str = Field(default="localhost", description="Database host.")
    port: int = Field(default=5432, description="Database port.")
    user: str = Field(default="", description="Database user.")
    password: str = Field(default="", description="Database password.")
    database: str = Field(default="interloper", description="Database name.")
    statement_timeout: float | None = Field(
        default=None,
        gt=0,
        description="Seconds after which the server cancels a statement of the process engine, sent as the libpq "
        "`statement_timeout` option; unset leaves the server's own setting untouched.",
    )

    @property
    def dsn(self) -> str:
        """Assemble a PostgreSQL connection string from individual fields."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class AuthSettings(BaseSettings):
    """Authentication settings: Google OAuth and the session cookie.

    The list fields take a comma-separated string from the environment
    (``INTERLOPER_AUTH_SUPER_ADMIN_EMAILS=a@x.com,b@x.com``) or a list from
    YAML.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}AUTH_")

    google_client_id: str = Field(default="", description="Google OAuth web client id.")
    google_client_secret: str = Field(default="", description="Google OAuth web client secret.")
    google_redirect_uri: str = Field(
        default="", description="Redirect URI registered on the Google OAuth client (`/api/auth/google/callback`)."
    )
    cookie_secure: bool = Field(default=True, description="Send the session cookie over HTTPS only.")
    session_expiry_days: int = Field(default=30, description="Days a session stays valid.")
    super_admin_emails: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Google emails promoted to platform-wide super-admin by `interloper db init` and on login; "
        "promotion only, removing an email never demotes.",
    )
    allowed_domains: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Email domains allowed to sign up; empty keeps signup open. Configured super-admins, pending "
        "invitations and existing profiles always pass.",
    )

    @field_validator("super_admin_emails", "allowed_domains", mode="before")
    @classmethod
    def _parse_comma_list(cls, value: Any) -> list[str]:
        """Accept a comma-separated string (env) or a list (YAML).

        Args:
            value: The raw value: a comma-separated string from env, or an
                iterable of entries from YAML.

        Returns:
            Trimmed, lowercased entries with empties dropped and any leading
            ``@`` stripped (so ``@example.com`` and ``example.com`` both work).
        """
        if isinstance(value, str):
            value = value.split(",")
        return [entry.strip().lower().lstrip("@") for entry in value if entry and entry.strip()]


class ServerSettings(BaseSettings):
    """HTTP server settings: the API and the app."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}SERVER_")

    enabled: bool = Field(default=True, description="Serve the API and the app.")
    host: str = Field(default="0.0.0.0", description="Bind host.")
    port: int = Field(default=3000, description="Bind port.")
    external_url: str = Field(
        default="",
        description="Public base URL of the app (`https://app.example.com`), for links built outside a browser "
        "request such as the setup hand-off an MCP tool returns; empty means the deployment has none.",
    )


class CronSettings(BaseSettings):
    """Cron controller settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}CRON_")

    enabled: bool = Field(default=True, description="Run the cron controller.")
    reconcile_interval: int = Field(default=10, description="Seconds between reconciliations.")
    max_execution_delay: int = Field(
        default=3600,
        description="Seconds a due job may still fire after its slot, so a scheduler restart doesn't drop it; "
        "at least the reconcile interval.",
    )
    batch_size: int = Field(default=50, description="Due jobs dispatched per reconciliation.")


class RenewalSettings(BaseSettings):
    """Renewal controller settings: connection credential renewal (singleton)."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}RENEWAL_")

    enabled: bool = Field(default=True, description="Run the renewal controller.")
    reconcile_interval: int = Field(default=60, description="Seconds between reconciliations.")
    batch_size: int = Field(default=50, description="Connections renewed per reconciliation.")


class WorkerSettings(BaseSettings):
    """Queue worker settings."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}WORKER_")

    enabled: bool = Field(default=True, description="Run the queue worker.")
    poll_interval: int = Field(default=5, description="Seconds between queue polls.")


class ReaperSettings(BaseSettings):
    """Run liveness: the heartbeat an executing run records, and the reaper that fails silent or overdue runs.

    The reaper is a singleton. A run's executor reads the heartbeat fields
    too, so the container launchers forward them into every run's environment.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}REAPER_")

    enabled: bool = Field(default=True, description="Run the reaper.")
    poll_interval: int = Field(default=15, description="Seconds between sweeps.")
    startup_timeout: int = Field(
        default=600, description="Seconds a dispatched run may take to start before the reaper fails it."
    )
    heartbeat_interval: int = Field(default=10, description="Seconds between an executing run's heartbeats.")
    heartbeat_timeout: int = Field(
        default=90,
        description="Seconds a running run may go without a heartbeat before the reaper fails it as lost; a run "
        "that cannot record one for half of it stops itself first.",
    )
    run_timeout: int | None = Field(
        default=43200,
        description="Seconds a run may take when its job declares no `timeout`; unset lets those runs run without "
        "a deadline.",
    )


class LauncherSettings(BaseSettings):
    """Launcher settings: the launcher type and its own configuration."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}LAUNCHER_")

    type: str = Field(
        default="in_process", description="Registry key of the launcher: `in_process`, `docker` or `kubernetes`."
    )
    config: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the launcher class.")


class SmtpSettings(BaseSettings):
    """SMTP settings for sending invitation emails."""

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}SMTP_")

    host: str = Field(default="", description="SMTP host; sending is enabled once host, user and password are set.")
    port: int = Field(default=587, description="SMTP port.")
    user: str = Field(default="", description="SMTP user.")
    password: str = Field(default="", description="SMTP password.")
    from_addr: str = Field(default="noreply@interloper.dev", description="Sender address of invitation emails.")

    @property
    def enabled(self) -> bool:
        """SMTP is enabled when host, user, and password are all set."""
        return bool(self.host and self.user and self.password)


class AgentSettings(BaseSettings):
    """AI agent settings: the chat assistant the API serves.

    The agent is only available when the ``agent`` extra is installed
    (``interloper-api[agent]``).
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}AGENT_")

    enabled: bool = Field(default=True, description="Serve the agent; off leaves the installation untouched.")
    model: str = Field(
        default="google:gemini-2.5-flash",
        description="pydantic-ai `provider:model` name (`google:gemini-2.5-flash`, `google-cloud:gemini-2.5-flash` "
        "for Vertex, `anthropic:claude-sonnet-4-5`, `openai:gpt-5`); provider credentials come from the provider's "
        "standard environment variables.",
    )


class McpSettings(BaseSettings):
    """MCP server settings (``interloper-mcp``).

    ``token`` and ``org_id`` only apply to the stdio transport, which
    authenticates once at startup.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}MCP_")

    host: str = Field(default="0.0.0.0", description="Bind host.")
    port: int = Field(default=3001, description="Bind port.")
    external_url: str = Field(
        default="",
        description="Public base URL of the hosted server (`https://mcp.example.com`), fed to the OAuth "
        "protected-resource metadata so clients discover the auth requirements.",
    )
    token: str = Field(default="", description="Personal access token the stdio transport authenticates with.")
    org_id: str = Field(
        default="",
        description="Organisation the stdio transport is scoped to without a token; local development only.",
    )


class QuotaSettings(BaseSettings):
    """Default per-organisation quota limits; unset means unlimited.

    Per-organisation overrides live in the ``quotas`` table and win over
    these defaults.
    """

    model_config = SettingsConfigDict(env_prefix=f"{PREFIX}QUOTA_")

    max_sources: int | None = Field(default=None, description="Sources an organisation may hold.")
    max_assets_per_source: int | None = Field(default=None, description="Assets a source may enable.")
    max_successful_runs_per_month: int | None = Field(
        default=None, description="Successful runs an organisation may complete per UTC month."
    )
    max_backfill_partitions: int | None = Field(default=None, description="Partitions one backfill may span.")


class AppSettings(BaseSettings):
    """Top-level runtime settings for the CLI.

    The framework reads ``runner``, ``otel``, ``catalog`` and ``secrets``;
    the other sections live here because ``AppSettings`` does, and configure
    the platform packages (``interloper-db``, ``interloper-api``,
    ``interloper-scheduler``, ``interloper-agent``, ``interloper-mcp``).
    """

    model_config = SettingsConfigDict(
        env_prefix=PREFIX,
        yaml_file="interloper.yaml",
        yaml_file_encoding="utf-8",
    )

    runner: RunnerSettings = Field(default_factory=RunnerSettings)
    otel: TelemetrySettings = Field(default_factory=TelemetrySettings)
    catalog: list[str] = Field(
        default_factory=list,
        description="Import paths of the enabled components; empty enables everything installed.",
    )
    secrets: SecretsSettings = Field(default_factory=SecretsSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    cron: CronSettings = Field(default_factory=CronSettings)
    renewal: RenewalSettings = Field(default_factory=RenewalSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    reaper: ReaperSettings = Field(default_factory=ReaperSettings)
    launcher: LauncherSettings = Field(default_factory=LauncherSettings)
    smtp: SmtpSettings = Field(default_factory=SmtpSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    mcp: McpSettings = Field(default_factory=McpSettings)
    quota: QuotaSettings = Field(default_factory=QuotaSettings)

    _active: ClassVar[AppSettings | None] = None

    # -- Sources ---------------------------------------------------------------

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        **kwargs: Any,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings sources: init > env > yaml.

        Args:
            settings_cls: The settings class being built, passed to the YAML
                source so it reads the class's own ``yaml_file`` config.
            **kwargs: Pydantic's default sources, of which ``init_settings``
                and ``env_settings`` are kept.

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

    # -- Active instance -------------------------------------------------------

    @classmethod
    def get(cls) -> AppSettings:
        """Load settings for the current CLI invocation.

        Returns:
            Active settings (if set), otherwise source-loaded settings.
        """
        return cls._active or cls.from_sources()

    @classmethod
    def activate(cls, settings: AppSettings) -> None:
        """Set active settings for the current CLI invocation.

        Args:
            settings: The settings every later ``get()`` returns until cleared.
        """
        cls._active = settings

    @classmethod
    def clear_active(cls) -> None:
        """Clear active settings for the current CLI invocation."""
        cls._active = None
