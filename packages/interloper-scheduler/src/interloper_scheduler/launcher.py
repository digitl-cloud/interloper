"""Launcher interface, in-process implementation, and the launcher registry."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import cache
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.errors import ConfigError
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


_ENTRY_POINT_GROUP = "interloper.launchers"


@cache
def launchers() -> dict[str, type[Launcher]]:
    """Load the launcher registry from installed entry points.

    Every launcher — including the built-in in-process one — registers
    through the ``interloper.launchers`` entry-point group; the entry name
    is the launcher type key used in ``LauncherSettings.type``. Installed
    means discovered: no import-order dependence, and a new launcher is one
    new package with one entry point.

    Returns:
        Mapping of launcher type key to launcher class.
    """
    return {entry_point.name: entry_point.load() for entry_point in entry_points(group=_ENTRY_POINT_GROUP)}


def build_launcher(
    launcher: LauncherSettings,
    *,
    postgres: PostgresSettings,
    runner: RunnerSettings,
    catalog: Catalog | None,
    store: Any | None = None,
) -> Launcher:
    """Build a launcher instance from settings.

    Resolves the launcher class from the registry by ``launcher.type`` and
    delegates construction to the class's own :meth:`Launcher.from_settings`
    — each integration package owns its construction recipe.

    Args:
        launcher: Launcher settings (type + type-specific config).
        postgres: Postgres settings forwarded to launchers that spawn
            isolated processes (e.g. Docker).
        runner: Runner settings forwarded to every launcher.
        catalog: Catalog forwarded to launchers that spawn isolated
            processes so they can reproduce an identical catalog
            (``None`` is accepted by launchers that don't consume it).
        store: Optional Store instance shared with in-process launchers.

    Returns:
        A scheduler ``Launcher`` instance.

    Raises:
        ConfigError: If no launcher is registered under ``launcher.type``.
    """
    registry = launchers()
    if launcher.type not in registry:
        raise ConfigError(
            f"Unknown launcher: {launcher.type!r} (available: {sorted(registry)}). "
            f"Is the matching interloper package installed?"
        )
    return registry[launcher.type].from_settings(
        launcher, postgres=postgres, runner=runner, catalog=catalog, store=store
    )


class Launcher(ABC):
    """Abstract base for run launchers.

    A launcher decides *where* a run executes: in-process, Docker, Kubernetes, etc.
    Every launcher carries a runner configuration that determines *how* the
    DAG is executed once it reaches the execution environment.
    """

    @classmethod
    @abstractmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> Launcher:
        """Construct this launcher from scheduler settings.

        The registry construction hook: each launcher owns its recipe
        (which settings it consumes, how ``settings.config`` maps to its
        constructor).
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
