"""Launcher interface and in-process implementation."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper_db import Store

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.settings import LauncherSettings, PostgresSettings, RunnerSettings

logger = logging.getLogger(__name__)


class RunStatus(str, Enum):
    """Authoritative status of a launched run, reported by the launcher."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NOT_FOUND = "not_found"


@dataclass
class RunState:
    """Authoritative state of a launched run, as reported by its launcher."""

    status: RunStatus
    error: str | None = None


def build_launcher(
    launcher: LauncherSettings,
    *,
    postgres: PostgresSettings,
    runner: RunnerSettings,
    catalog: Catalog,
    store: Any | None = None,
) -> Any:
    """Build a launcher instance from settings.

    The runner configuration is always forwarded so every launcher type
    respects ``RunnerSettings`` uniformly.

    Args:
        launcher: Launcher settings (type + type-specific config).
        postgres: Postgres settings forwarded to launchers that spawn
            isolated processes (e.g. Docker).
        runner: Runner settings forwarded to every launcher.
        catalog: Catalog forwarded to launchers that spawn isolated
            processes so they can reproduce an identical catalog.
        store: Optional Store instance shared with in-process launchers.

    Returns:
        A scheduler ``Launcher`` instance.

    Raises:
        ValueError: If the launcher type is unknown.
    """
    match launcher.type:
        case "in_process":
            from interloper_scheduler import InProcessLauncher

            return InProcessLauncher(
                store=store,
                runner_type=runner.type,
                runner_config=runner.config,
            )
        case "docker":
            from interloper_docker import DockerLauncher

            postgres_kwargs = {
                "postgres_host": postgres.host,
                "postgres_port": postgres.port,
                "postgres_user": postgres.user,
                "postgres_password": postgres.password,
                "postgres_database": postgres.database,
            }
            kwargs = {**postgres_kwargs, **launcher.config}
            return DockerLauncher(
                catalog=catalog,
                runner_type=runner.type,
                runner_config=runner.config,
                **kwargs,  # ty: ignore[invalid-argument-type]
            )
        case "kubernetes":
            try:
                from interloper_k8s import KubernetesLauncher
            except ImportError as exc:
                raise ValueError(
                    "Launcher 'kubernetes' requires the 'interloper-k8s' package to be installed."
                ) from exc

            postgres_kwargs = {
                "postgres_host": postgres.host,
                "postgres_port": postgres.port,
                "postgres_user": postgres.user,
                "postgres_password": postgres.password,
                "postgres_database": postgres.database,
            }
            kwargs = {**postgres_kwargs, **launcher.config}
            return KubernetesLauncher(
                catalog=catalog,
                runner_type=runner.type,
                runner_config=runner.config,
                **kwargs,  # ty: ignore[invalid-argument-type]
            )
        case _:
            raise ValueError(f"Unknown launcher: {launcher.type!r}. Available: in_process, docker, kubernetes")


class Launcher(ABC):
    """Abstract base for run launchers.

    A launcher decides *where* a run executes: in-process, Docker, Kubernetes, etc.
    Every launcher carries a runner configuration that determines *how* the
    DAG is executed once it reaches the execution environment.
    """

    def __init__(
        self,
        runner_type: str = "multi_thread",
        runner_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``serial``, ``multi_thread``, ``multi_process``).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
        """
        self._runner_type = runner_type
        self._runner_config = runner_config or {}

    @abstractmethod
    def launch(self, run_id: UUID) -> None:
        """Launch a run for execution.

        Args:
            run_id: The run UUID to execute.
        """

    def describe_run(self, run_id: UUID) -> RunState | None:
        """Return the authoritative state of a launched run.

        Args:
            run_id: The run UUID to describe.

        Returns:
            The run's authoritative state, or ``None`` if the launcher
            cannot introspect its runs.
        """
        return None


class InProcessLauncher(Launcher):
    """Launches runs in a detached thread using ``RunExecutor``.

    Accepts an optional ``store`` so all runs share the same persistence
    layer (encryption keys, etc.) rather than creating a fresh default.
    """

    def __init__(
        self,
        runner_type: str = "multi_thread",
        runner_config: dict[str, Any] | None = None,
        store: Store | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``serial``, ``multi_thread``, ``multi_process``).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
            store: Optional Store instance to share with executors.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._store = store

    def launch(self, run_id: UUID) -> None:
        """Launch a run in a background thread.

        Args:
            run_id: The run UUID to execute.
        """
        from interloper.runner import build_runner

        from interloper_scheduler.executor import RunExecutor

        runner_cls, runner_kwargs = build_runner(self._runner_type, self._runner_config)
        executor = RunExecutor(
            store=self._store,
            runner_type=runner_cls,
            runner_kwargs=runner_kwargs,
        )
        thread = threading.Thread(target=executor.execute, args=(run_id,), daemon=True)
        thread.start()
        logger.info("Launched run %s in background thread", run_id)
