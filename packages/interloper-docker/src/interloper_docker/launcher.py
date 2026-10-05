"""Docker launcher: runs each job in its own container."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any
from uuid import UUID

import docker
from interloper.catalog.base import Catalog
from interloper.errors import ConfigError
from interloper.telemetry.propagation import child_process_env

if TYPE_CHECKING:
    from interloper.settings import LauncherSettings, PostgresSettings, ReaperSettings, RunnerSettings
from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)

# TODO: Implement a grace period to check if the container is running before returning in order to avoid
# stale run statuses due to container startup errors?

# TODO: `launch` command should supoort --catalog option to pass the catalog as a serialized string
# Then use this instead of INTERLOPER_CATALOG


class DockerLauncher(Launcher):
    """Launches each run in its own Docker container.

    The container executes the ``interloper launch <run_id>`` CLI command,
    which hydrates the DAG from the database and runs it to completion.

    Postgres connection parameters are passed as plain values. The caller
    (``Launcher.from_settings``) injects the app-level defaults; any
    overrides from the launcher YAML config take precedence.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> DockerLauncher:
        """Construct from scheduler settings (registry construction hook).

        Postgres credentials and the catalog are forwarded into the spawned
        environment; ``settings.config`` supplies the launcher-specific
        keyword arguments.

        Args:
            settings: The launcher settings block (image, network, volumes).
            postgres: Postgres credentials forwarded into the spawned container.
            runner: Runner settings the spawned process builds its runner from.
            reaper: Run liveness settings whose heartbeat fields the spawned
                process follows.
            catalog: Catalog forwarded as import paths; required, since a container
                cannot see the caller's.
            store: Accepted for the registry's uniform hook and unused — a
                container opens its own connection, not the caller's.

        Returns:
            The configured launcher.

        Raises:
            ConfigError: If no catalog is provided — this launcher spawns
                isolated processes and must reproduce the catalog there.
        """
        if catalog is None:
            raise ConfigError("The 'docker' launcher requires a catalog.")
        return cls(
            catalog=catalog,
            postgres_host=postgres.host,
            postgres_port=postgres.port,
            postgres_user=postgres.user,
            postgres_password=postgres.password,
            postgres_database=postgres.database,
            runner_type=runner.type,
            runner_config=runner.config,
            heartbeat_interval=reaper.heartbeat_interval,
            heartbeat_timeout=reaper.heartbeat_timeout,
            **settings.config,
        )

    def __init__(
        self,
        catalog: Catalog,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_database: str,
        image: str = "interloper:latest-scheduler",
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
        volumes: dict[str, dict[str, str]] | None = None,
        forward_env: list[str] | None = None,
        heartbeat_interval: int = 10,
        heartbeat_timeout: int = 90,
    ) -> None:
        """Initialize the Docker launcher.

        Args:
            catalog: Catalog to inject into the container so it builds an identical catalog.
            postgres_host: Postgres host to inject into the container.
            postgres_port: Postgres port to inject into the container.
            postgres_user: Postgres user to inject into the container.
            postgres_password: Postgres password to inject into the container.
            postgres_database: Postgres database to inject into the container.
            image: Docker image to use.
            runner_type: Runner type name forwarded to the container.
            runner_config: Runner-specific kwargs forwarded to the container.
            volumes: Volume mounts for the container.  When
                ``runner_type`` is ``"docker"``, the Docker socket is
                mounted automatically if not already included.
            forward_env: Names of variables copied from the launcher's own
                environment into the container (unset ones are skipped).
                Runs hydrate connections, so they need the same
                runtime-resolved credentials (e.g. the in-house OAuth
                provider trio) the API resolves from its environment.
            heartbeat_interval: Seconds between a run's heartbeats, forwarded
                to the container.
            heartbeat_timeout: The reaper's heartbeat timeout in seconds,
                forwarded to the container.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._client = docker.from_env()
        self._catalog = catalog
        self._image = image
        self._postgres_host = postgres_host
        self._postgres_port = postgres_port
        self._postgres_user = postgres_user
        self._postgres_password = postgres_password
        self._postgres_database = postgres_database
        self._volumes = dict(volumes or {})
        self._forward_env = forward_env or []
        if runner_type == "docker" and "/var/run/docker.sock" not in self._volumes:
            self._volumes["/var/run/docker.sock"] = {"bind": "/var/run/docker.sock", "mode": "rw"}

    def launch(self, run_id: UUID) -> None:
        """Start a container that executes a single run.

        Args:
            run_id: The run UUID to execute.
        """
        environment = self._build_environment()
        container_name = f"interloper_run_{str(run_id)[:8]}"

        try:
            container = self._client.containers.run(
                image=self._image,
                name=container_name,
                command=["interloper", "launch", str(run_id)],
                environment=environment,
                volumes=self._volumes or None,
                user="root" if self._runner_type == "docker" else None,
                detach=True,
                auto_remove=False,
                labels={"interloper.run_id": str(run_id)},
            )
            logger.info("Started container %s for run %s", container.short_id, run_id)
        except Exception:
            logger.exception("Failed to start container for run %s", run_id)
            raise

    def diagnose(self, run_id: UUID) -> str | None:
        """Say why a run's container stopped, from its exit state.

        Args:
            run_id: The run UUID.

        Returns:
            The container's status, exit code, OOM kill and error, or ``None``
            when it is still running, gone, or unreadable.
        """
        try:
            container = self._client.containers.get(f"interloper_run_{str(run_id)[:8]}")
            container.reload()
        except Exception:  # noqa: BLE001 - a diagnosis is best-effort detail on a run already found dead
            return None

        state = container.attrs.get("State", {}) if container.attrs else {}
        docker_status = (state.get("Status") or container.status or "").lower()
        if docker_status in ("running", "created", "restarting", "paused"):
            return None

        parts = [f"container status={docker_status}"]
        if state.get("ExitCode") is not None:
            parts.append(f"exit_code={state['ExitCode']}")
        if state.get("OOMKilled"):
            parts.append("OOMKilled")
        if state.get("Error"):
            parts.append(f"error={state['Error']}")
        return " ".join(parts)

    def _build_environment(self) -> dict[str, str]:
        """Build environment variables for the container.

        Returns:
            The environment the run container is started with.

        """
        environment: dict[str, str] = {
            "INTERLOPER_POSTGRES_HOST": self._postgres_host,
            "INTERLOPER_POSTGRES_PORT": str(self._postgres_port),
            "INTERLOPER_POSTGRES_USER": self._postgres_user,
            "INTERLOPER_POSTGRES_PASSWORD": self._postgres_password,
            "INTERLOPER_POSTGRES_DATABASE": self._postgres_database,
            "INTERLOPER_CATALOG": json.dumps(self._catalog.to_paths()),
            "INTERLOPER_RUNNER_TYPE": self._runner_type,
            "INTERLOPER_RUNNER_CONFIG": json.dumps(self._runner_config),
            "INTERLOPER_REAPER_HEARTBEAT_INTERVAL": str(self._heartbeat_interval),
            "INTERLOPER_REAPER_HEARTBEAT_TIMEOUT": str(self._heartbeat_timeout),
        }
        encryption_key = os.environ.get("INTERLOPER_ENCRYPTION_KEY")
        if encryption_key:
            environment["INTERLOPER_ENCRYPTION_KEY"] = encryption_key
        for name in self._forward_env:
            value = os.environ.get(name)
            if value:
                environment[name] = value
        environment.update(child_process_env())
        return environment
