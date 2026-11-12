"""The framework's own settings sections: what every role reads.

``server`` is here whole although the framework reads only part of it: it is
the address of the deployment, which every role reads.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field, field_validator
from pydantic_settings import NoDecode

from interloper.settings.base import Settings


class RunnerSettings(Settings):
    """Runner settings: the runner type and its own configuration.

    Built-in types: ``async`` (default, in-process concurrency via ``max_workers``)
    and ``serial`` (``async`` with a single slot). The ``docker`` and
    ``kubernetes`` runners register through their own packages.
    """

    public: ClassVar[frozenset[str]] = frozenset(
        {
            "type",
            "config.max_workers",
            "config.image",
            "config.namespace",
            "config.service_account_name",
            "config.image_pull_policy",
            "config.ttl_seconds_after_finished",
            "config.node_selector",
            "config.resources",
        }
    )

    type: str = Field(
        default="async",
        description="Registry key of the runner: `async`, `serial`, or one another package registers.",
    )
    config: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the runner class.")


class TelemetrySettings(Settings):
    """OpenTelemetry settings: OTLP traces and metrics.

    Exporting requires the ``otel`` extra (``interloper[otel]``); when it is
    missing, enabling telemetry logs a warning and stays a no-op.
    """

    public: ClassVar[frozenset[str]] = frozenset(
        {"enabled", "protocol", "service_name", "traces", "metrics", "sample_ratio", "metric_export_interval"}
    )

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


class LogSettings(Settings):
    """How the commands write their log records to stderr."""

    public: ClassVar[frozenset[str]] = frozenset({"format"})

    format: Literal["text", "json"] = Field(
        default="text",
        description="`text` for a terminal; `json` writes one object per line with the record's trace and span ids, "
        "for a log collector to parse.",
    )


class CatalogSettings(Settings):
    """Which installed components the deployment enables.

    ``include`` takes a comma-separated string from the environment
    (``INTERLOPER_CATALOG_INCLUDE=pkg.sources.Shop,pkg.sources.Finance``) or a
    list from YAML.
    """

    public: ClassVar[frozenset[str]] = frozenset({"include"})

    include: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Import paths of the enabled components; empty enables everything installed.",
    )

    @field_validator("include", mode="before")
    @classmethod
    def _parse_comma_list(cls, value: Any) -> list[str]:
        """Accept a comma-separated string (env) or a list (YAML).

        Args:
            value: The raw value: a comma-separated string from env, or an
                iterable of import paths from YAML.

        Returns:
            Trimmed import paths with empties dropped, case kept.
        """
        if isinstance(value, str):
            value = value.split(",")
        return [entry.strip() for entry in value if entry and entry.strip()]


class ServerSettings(Settings):
    """HTTP server settings: the API and the app."""

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "host", "port", "external_url", "internal_url"})

    enabled: bool = Field(default=True, description="Serve the API and the app.")
    host: str = Field(default="0.0.0.0", description="Bind host.")
    port: int = Field(default=3000, description="Bind port.")
    external_url: str = Field(
        default="",
        description="Public base URL of the app (`https://app.example.com`), for links built outside a browser "
        "request such as the setup hand-off an MCP tool returns, and the base of each connector OAuth app's "
        "redirect URI (`/auth/<provider>`); empty means the deployment has none.",
    )
    internal_url: str = Field(
        default="http://localhost:3000",
        description="Base URL a run's pod reports its events to: the API as reached from inside the deployment.",
    )

    @property
    def public_url(self) -> str:
        """The app's public base URL: ``external_url`` without a trailing slash, else the local server.

        It is the OAuth authorization server's issuer, which the API and the
        MCP server must name identically.

        Returns:
            The URL, ``http://localhost:<port>`` when ``external_url`` is empty.
        """
        return self.external_url.rstrip("/") or f"http://localhost:{self.port}"


class RunSettings(Settings):
    """The platform run this process executes: its token and heartbeat cadence.

    Only the platform writes these. The launcher sets them for the run it
    starts, and a container runner forwards them to the operations it starts
    in their own processes; operators never set them.
    """

    public: ClassVar[frozenset[str]] = frozenset({"heartbeat_interval", "heartbeat_timeout"})

    token: str = Field(
        default="",
        description="The run's token, with which `interloper launch` loads the run from the API and reports "
        "its events back.",
    )
    heartbeat_interval: int = Field(
        default=10, description="Seconds between the run's heartbeats: the scheduler's `reaper.heartbeat_interval`."
    )
    heartbeat_timeout: int = Field(
        default=90,
        description="Seconds the run may go without a heartbeat before the reaper fails it, the scheduler's "
        "`reaper.heartbeat_timeout`; a run that cannot record one for half of it stops itself first.",
    )
