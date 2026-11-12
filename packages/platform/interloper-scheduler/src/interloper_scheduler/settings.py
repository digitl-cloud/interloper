"""The scheduler's settings sections, and the bundle of every section its services read.

`SchedulerSettings` carries the scheduler's own sections, the deployment's
``server`` and the ``runner`` its launcher hands to runs, from the command
that reads them to the services it builds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar

from interloper.settings import RunnerSettings, ServerSettings, Settings
from pydantic import Field


class CronSettings(Settings):
    """Cron controller settings."""

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "reconcile_interval", "max_execution_delay", "batch_size"})

    enabled: bool = Field(default=True, description="Run the cron controller.")
    reconcile_interval: int = Field(default=10, description="Seconds between reconciliations.")
    max_execution_delay: int = Field(
        default=3600,
        description="Seconds a due job may still fire after its slot, so a scheduler restart doesn't drop it; "
        "at least the reconcile interval.",
    )
    batch_size: int = Field(default=50, description="Due jobs dispatched per reconciliation.")


class RenewalSettings(Settings):
    """Renewal controller settings: connection credential renewal (singleton)."""

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "reconcile_interval", "batch_size"})

    enabled: bool = Field(default=True, description="Run the renewal controller.")
    reconcile_interval: int = Field(default=60, description="Seconds between reconciliations.")
    batch_size: int = Field(default=50, description="Connections renewed per reconciliation.")


class WorkerSettings(Settings):
    """Queue worker settings."""

    public: ClassVar[frozenset[str]] = frozenset({"enabled", "poll_interval"})

    enabled: bool = Field(default=True, description="Run the queue worker.")
    poll_interval: int = Field(default=5, description="Seconds between queue polls.")


class ReaperSettings(Settings):
    """The reaper: it fails silent or overdue runs and deletes credentials that ended long ago.

    The reaper is a singleton. The launcher hands the heartbeat fields to every
    run it starts, as that run's `RunSettings`.
    """

    public: ClassVar[frozenset[str]] = frozenset(
        {
            "enabled",
            "poll_interval",
            "startup_timeout",
            "heartbeat_interval",
            "heartbeat_timeout",
            "run_timeout",
            "credential_retention_days",
        }
    )

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
    credential_retention_days: int = Field(
        default=30,
        description="Days a session, personal or OAuth access token, authorization code or refresh token is kept "
        "once it expired or was revoked, before the reaper deletes it; a refresh token goes with its whole chain, "
        "once the chain has ended.",
    )

    @property
    def credential_retention(self) -> timedelta:
        """How long a credential is kept once it expired or was revoked.

        Returns:
            ``credential_retention_days`` as a duration.
        """
        return timedelta(days=self.credential_retention_days)


class LauncherSettings(Settings):
    """Launcher settings: the launcher type and its own configuration."""

    public: ClassVar[frozenset[str]] = frozenset(
        {
            "type",
            "config.image",
            "config.namespace",
            "config.service_account_name",
            "config.image_pull_policy",
            "config.ttl_seconds_after_finished",
            "config.node_selector",
            "config.resources",
            "config.volumes",
            "config.runner_type",
            "config.runner_config.max_workers",
            "config.runner_config.image",
            "config.runner_config.namespace",
            "config.runner_config.service_account_name",
            "config.runner_config.image_pull_policy",
            "config.runner_config.ttl_seconds_after_finished",
            "config.runner_config.node_selector",
            "config.runner_config.resources",
        }
    )

    type: str = Field(
        default="in_process", description="Registry key of the launcher: `in_process`, `docker` or `kubernetes`."
    )
    config: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the launcher class.")


@dataclass(frozen=True)
class SchedulerSettings:
    """Every section the scheduler's services read, as the command running them read them."""

    server: ServerSettings
    runner: RunnerSettings
    cron: CronSettings
    renewal: RenewalSettings
    worker: WorkerSettings
    reaper: ReaperSettings
    launcher: LauncherSettings

    @classmethod
    def get(cls) -> SchedulerSettings:
        """Read each section the way this process runs with it.

        Returns:
            The sections.
        """
        return cls(
            server=ServerSettings.get(),
            runner=RunnerSettings.get(),
            cron=CronSettings.get(),
            renewal=RenewalSettings.get(),
            worker=WorkerSettings.get(),
            reaper=ReaperSettings.get(),
            launcher=LauncherSettings.get(),
        )
