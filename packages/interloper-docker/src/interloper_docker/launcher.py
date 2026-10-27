"""Docker launcher: runs each run in its own container."""

from __future__ import annotations

import io
import json
import logging
import os
import tarfile
from typing import TYPE_CHECKING, Any
from uuid import UUID

import docker
from interloper.catalog import Catalog
from interloper.errors import ConfigError
from interloper.telemetry.propagation import child_process_env

if TYPE_CHECKING:
    from interloper.dag import DAGSpec
    from interloper.settings import LauncherSettings, ReaperSettings, RunnerSettings, ServerSettings
    from interloper_db.models import Run
from interloper_scheduler.launcher import Launcher

logger = logging.getLogger(__name__)

_SPEC_DIRECTORY = "/interloper/run"


class DockerLauncher(Launcher):
    """Launches each run in its own Docker container.

    The container runs ``interloper run`` on the run's DAG spec, copied into
    it before it starts: a bind mount would need the scheduler and the Docker
    host to share a filesystem, which a containerised scheduler does not. The
    container holds no database credentials: it reports its events to the
    API's ``internal_url`` with the run's token.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        server: ServerSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> DockerLauncher:
        """Construct from scheduler settings (registry construction hook).

        The API's internal URL and the catalog are forwarded into the spawned
        environment; ``settings.config`` supplies the launcher-specific
        keyword arguments.

        Args:
            settings: The launcher settings block (image, network, volumes).
            server: Server settings whose ``internal_url`` the container reports to.
            runner: Runner settings the spawned process builds its runner from.
            reaper: Run liveness settings whose heartbeat fields the spawned
                process follows.
            catalog: Catalog forwarded as import paths; required, since a container
                cannot see the caller's.
            store: Accepted for the registry's uniform hook and unused: a
                container reports through the API.

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
            internal_url=server.internal_url,
            runner_type=runner.type,
            runner_config=runner.config,
            heartbeat_interval=reaper.heartbeat_interval,
            heartbeat_timeout=reaper.heartbeat_timeout,
            **settings.config,
        )

    def __init__(
        self,
        catalog: Catalog,
        internal_url: str = "http://localhost:3000",
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
            internal_url: The API's base URL as reached from the container,
                which the run reports its events to.
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
        self._internal_url = internal_url
        self._volumes = dict(volumes or {})
        self._forward_env = forward_env or []
        if runner_type == "docker" and "/var/run/docker.sock" not in self._volumes:
            self._volumes["/var/run/docker.sock"] = {"bind": "/var/run/docker.sock", "mode": "rw"}

    def launch(self, run: Run, spec: DAGSpec, token: str) -> None:
        """Create a container for the run, copy its spec in, and start it.

        A container that cannot be started is removed: it holds the spec,
        and the spec holds the run's connection secrets.

        Args:
            run: The dispatched run.
            spec: The DAG it executes, copied into the container.
            token: The run's token, in the container's environment.
        """
        container = self._client.containers.create(
            image=self._image,
            name=f"interloper_run_{str(run.id)[:8]}",
            command=self.command(run, f"{_SPEC_DIRECTORY}/spec.json"),
            environment=self._build_environment(token),
            volumes=self._volumes or None,
            user="root" if self._runner_type == "docker" else None,
            labels={"interloper.run_id": str(run.id)},
        )
        try:
            container.put_archive("/", self._archive(spec))
            container.start()
        except Exception:
            container.remove(force=True)
            raise
        logger.info("Started container %s for run %s", container.short_id, run.id)

    @staticmethod
    def _archive(spec: DAGSpec) -> bytes:
        """Pack the spec, and the directory it lives in, as the tar archive Docker copies into a container's root.

        Docker extracts an archive only into a directory that already exists,
        and no image carries the spec's, so the archive brings it along.

        Args:
            spec: The run's DAG spec.

        Returns:
            The archive's bytes.
        """
        directory = _SPEC_DIRECTORY.lstrip("/")
        document = spec.model_dump_json().encode()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            folder = tarfile.TarInfo(directory)
            folder.type = tarfile.DIRTYPE
            folder.mode = 0o755
            archive.addfile(folder)
            member = tarfile.TarInfo(f"{directory}/spec.json")
            member.size = len(document)
            member.mode = 0o444
            archive.addfile(member, io.BytesIO(document))
        return buffer.getvalue()

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

    def _build_environment(self, token: str) -> dict[str, str]:
        """Build environment variables for the container.

        Args:
            token: The run's token.

        Returns:
            The environment the run container is started with.
        """
        environment: dict[str, str] = {
            "INTERLOPER_CATALOG": json.dumps(self._catalog.to_paths()),
            "INTERLOPER_RUNNER_TYPE": self._runner_type,
            "INTERLOPER_RUNNER_CONFIG": json.dumps(self._runner_config),
            "INTERLOPER_REAPER_HEARTBEAT_INTERVAL": str(self._heartbeat_interval),
            "INTERLOPER_REAPER_HEARTBEAT_TIMEOUT": str(self._heartbeat_timeout),
            "INTERLOPER_SERVER_INTERNAL_URL": self._internal_url,
            "INTERLOPER_RUN_TOKEN": token,
        }
        for name in self._forward_env:
            value = os.environ.get(name)
            if value:
                environment[name] = value
        environment.update(child_process_env())
        return environment
