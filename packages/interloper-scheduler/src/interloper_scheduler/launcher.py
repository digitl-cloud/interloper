"""Launcher interface, in-process implementation, and the launcher registry."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.errors import ConfigError
from interloper.registry import Registry
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


#: Launcher classes by type key (``LauncherSettings.type``), fed by the
#: ``interloper.launchers`` entry-point group — every launcher, the
#: built-in in-process one included, registers through it. Installed means
#: discovered: a new launcher is one new package with one entry point.
LAUNCHERS: Registry[type[Launcher]] = Registry("interloper.launchers")


class Launcher(ABC):
    """Abstract base for run launchers.

    A launcher decides *where* a run executes: in-process, Docker, Kubernetes, etc.
    Every launcher carries a runner configuration that determines *how* the
    DAG is executed once it reaches the execution environment.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> Launcher:
        """Construct the launcher the settings describe.

        Called on ``Launcher``, resolves ``settings.type`` in ``LAUNCHERS``
        and delegates to that class's own ``from_settings`` — each launcher
        owns its recipe (which settings it consumes, how ``settings.config``
        maps to its constructor). Concrete launchers must override this
        with theirs.

        Args:
            settings: Launcher settings (type + type-specific config).
            postgres: Postgres settings forwarded to launchers that spawn
                isolated processes (e.g. Docker).
            runner: Runner settings forwarded to every launcher.
            catalog: Catalog forwarded to launchers that spawn isolated
                processes so they can reproduce an identical catalog
                (``None`` is accepted by launchers that don't consume it).
            store: Optional Store instance shared with in-process launchers.

        Returns:
            The configured launcher instance.

        Raises:
            ConfigError: If no launcher is registered under ``settings.type``.
            NotImplementedError: If the resolved launcher class does not
                implement its own ``from_settings``.
        """
        if cls is not Launcher:
            raise NotImplementedError(f"{cls.__name__} must implement from_settings")
        launcher_cls = LAUNCHERS.get(settings.type)
        if launcher_cls is None:
            raise ConfigError(
                f"Unknown launcher: {settings.type!r} (available: {list(LAUNCHERS.keys())}). "
                f"Is the matching interloper package installed?"
            )
        return launcher_cls.from_settings(settings, postgres=postgres, runner=runner, catalog=catalog, store=store)

    def __init__(
        self,
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``async``, ``serial``, ``multi_process``).
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
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
        store: Store | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``async``, ``serial``, ``multi_process``).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
            store: Optional Store instance to share with executors.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._store = store

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> InProcessLauncher:
        """Construct from settings; uses only the runner config and the shared store.

        Returns:
            The configured in-process launcher.
        """
        return cls(runner_type=runner.type, runner_config=runner.config, store=store)

    def launch(self, run_id: UUID) -> None:
        """Launch a run in a background thread.

        Args:
            run_id: The run UUID to execute.
        """
        from interloper.runner import Runner
        from interloper.settings import RunnerSettings

        from interloper_scheduler.executor import RunExecutor

        runner = Runner.from_settings(RunnerSettings(type=self._runner_type, config=self._runner_config))
        executor = RunExecutor(store=self._store, runner=runner)
        thread = threading.Thread(target=executor.execute, args=(run_id,), daemon=True)
        thread.start()
        logger.info("Launched run %s in background thread", run_id)
