"""Docker launcher: runs each job in its own container."""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from uuid import UUID

import docker
from interloper.catalog.base import Catalog
from interloper_scheduler.launcher import Launcher, RunState, RunStatus

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
    (``_build_launcher``) injects the app-level defaults; any overrides
    from the launcher YAML config take precedence.
    """

    def __init__(
        self,
        catalog: Catalog,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_database: str,
        image: str = "interloper:latest-scheduler",
        runner_type: str = "multi_thread",
        runner_config: dict[str, Any] | None = None,
        volumes: dict[str, dict[str, str]] | None = None,
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
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._client = docker.from_env()
        self._catalog = catalog
        self._image = image
        self._postgres_host = postgres_host
        self._postgres_port = postgres_port
        self._postgres_user = postgres_user
        self._postgres_password = postgres_password
        self._postgres_database = postgres_database
        self._volumes = dict(volumes or {})
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
                volumes=self._volumes if self._volumes else None,
                user="root" if self._runner_type == "docker" else None,
                detach=True,
                auto_remove=False,
                labels={"interloper.run_id": str(run_id)},
            )
            logger.info("Started container %s for run %s", container.short_id, run_id)
        except Exception:
            logger.exception("Failed to start container for run %s", run_id)
            raise

    def describe_run(self, run_id: UUID) -> RunState:
        """Return the authoritative state of a run's container.

        Used by the reaper to catch failed runs as soon as the
        container terminates, without waiting for the fallback timeout.

        Args:
            run_id: The run UUID to describe.

        Returns:
            A :class:`RunState` indicating whether the container is
            still running, has succeeded, has failed, or is gone.
        """
        container_name = f"interloper_run_{str(run_id)[:8]}"
        try:
            container = self._client.containers.get(container_name)
        except Exception:
            return RunState(status=RunStatus.NOT_FOUND)

        try:
            container.reload()
        except Exception:
            return RunState(status=RunStatus.NOT_FOUND)

        state = container.attrs.get("State", {}) if container.attrs else {}
        docker_status = (state.get("Status") or container.status or "").lower()

        # "running", "created", "restarting", "paused" — still alive
        if docker_status in ("running", "created", "restarting", "paused"):
            return RunState(status=RunStatus.RUNNING)

        # Terminal states: "exited", "dead", "removing"
        exit_code = state.get("ExitCode")
        if exit_code == 0:
            return RunState(status=RunStatus.SUCCEEDED)

        # Anything else is a failure
        parts = [f"Container {container.short_id} status={docker_status}"]
        if exit_code is not None:
            parts.append(f"exit_code={exit_code}")
        if state.get("OOMKilled"):
            parts.append("OOMKilled")
        error = state.get("Error") or ""
        if error:
            parts.append(f"error={error}")
        return RunState(status=RunStatus.FAILED, error=" ".join(parts))

    def _build_environment(self) -> dict[str, str]:
        """Build environment variables for the container."""
        environment: dict[str, str] = {
            "INTERLOPER_POSTGRES_HOST": self._postgres_host,
            "INTERLOPER_POSTGRES_PORT": str(self._postgres_port),
            "INTERLOPER_POSTGRES_USER": self._postgres_user,
            "INTERLOPER_POSTGRES_PASSWORD": self._postgres_password,
            "INTERLOPER_POSTGRES_DATABASE": self._postgres_database,
            "INTERLOPER_CATALOG": json.dumps(self._catalog.to_paths()),
            "INTERLOPER_RUNNER_TYPE": self._runner_type,
            "INTERLOPER_RUNNER_CONFIG": json.dumps(self._runner_config),
        }
        encryption_key = os.environ.get("INTERLOPER_ENCRYPTION_KEY")
        if encryption_key:
            environment["INTERLOPER_ENCRYPTION_KEY"] = encryption_key
        return environment
