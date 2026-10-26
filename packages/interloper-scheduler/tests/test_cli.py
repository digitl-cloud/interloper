"""Tests for ``interloper_scheduler.cli``."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.settings import AppSettings, CronSettings, ReaperSettings, WorkerSettings

from interloper_scheduler import cli

RUN_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


@pytest.fixture(autouse=True)
def _no_active_settings() -> Iterator[None]:
    """Start and leave every test with no activated settings.

    Yields:
        ``None``; the teardown clears the active settings.
    """
    AppSettings.clear_active()
    yield
    AppSettings.clear_active()


def _parse(*argv: str) -> argparse.Namespace:
    """Parse argv through the real registrations.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    subparsers = parser.add_subparsers(dest="command")
    cli.register_scheduler(subparsers)
    cli.register_launch(subparsers)
    return parser.parse_args(argv)


# -- scheduler -------------------------------------------------------------------


@pytest.fixture
def services(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub the bootstrap, catalog, store and supervisor the command wires.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        What the command handed each collaborator.
    """
    recorded: dict[str, Any] = {}

    def scheduler_services(settings: AppSettings, **kwargs: Any) -> dict[str, object]:
        recorded["services"] = kwargs
        return {"stub": object()}

    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", kwargs))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(cli, "scheduler_services", scheduler_services)
    monkeypatch.setattr(cli.Supervisor, "run", lambda self: recorded.setdefault("ran", list(self.services)))
    return recorded


class TestScheduler:
    """``interloper scheduler``."""

    def test_flags_override_the_settings(self, services: dict[str, Any]) -> None:
        cli._cmd_scheduler(_parse("scheduler", "--no-worker"))

        assert services["services"]["cron"] is True
        assert services["services"]["worker"] is False
        assert services["ran"] == ["stub"]

    def test_no_enabled_service_is_an_error(self, services: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
        settings = AppSettings(
            cron=CronSettings(enabled=False), worker=WorkerSettings(enabled=False), reaper=ReaperSettings(enabled=False)
        )
        monkeypatch.setattr(AppSettings, "_active", settings)

        with pytest.raises(SystemExit, match="No scheduler service enabled"):
            cli._cmd_scheduler(_parse("scheduler"))

        assert "ran" not in services

    def test_no_create_tables_reaches_the_bootstrap(self, services: dict[str, Any]) -> None:
        cli._cmd_scheduler(_parse("scheduler", "--no-create-tables"))

        assert services["bootstrap"]["create_tables"] is False


# -- launch ----------------------------------------------------------------------


class FakeRuns:
    """Records the run failures the launch command's failure path reports."""

    def __init__(self, fail: bool = False) -> None:
        """Set up the recorder.

        Args:
            fail: Whether ``fail`` itself should raise, mimicking a store
                that is unreachable while reporting the original failure.
        """
        self.failed: list[tuple[UUID, str]] = []
        self.unreachable = fail

    def fail(self, run_id: UUID, error: str) -> None:
        """Record (or refuse) a failure.

        Args:
            run_id: The run being failed.
            error: Why.

        Raises:
            RuntimeError: When this fake is configured to fail.
        """
        if self.unreachable:
            raise RuntimeError("store unreachable")
        self.failed.append((run_id, error))


class FakeStore:
    """Minimal store exposing only the ``runs`` repository the command uses."""

    def __init__(self, fail_complete: bool = False) -> None:
        """Set up the fake store.

        Args:
            fail_complete: Propagated to the ``runs`` recorder.
        """
        self.runs = FakeRuns(fail=fail_complete)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub out the catalog, store and executor the launch command builds.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        The stubbed store plus the recorded executor calls.
    """
    store = FakeStore()
    executed: list[UUID] = []
    built: list[Any] = []
    outcome = {"success": True}

    class FakeExecutor:
        def __init__(self, store: Any, runner: Any, *, reaper: Any, on_lost: Any) -> None:
            self.store = store
            self.runner = runner
            built.append(on_lost)

        def execute(self, run_id: UUID) -> bool:
            executed.append(run_id)
            return outcome["success"]

    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: store))
    monkeypatch.setattr(cli, "RunExecutor", FakeExecutor)
    return {"store": store, "executed": executed, "outcome": outcome, "on_lost": built}


class TestLaunch:
    """``interloper launch <run_id>``."""

    def test_parses_the_run_id_as_a_uuid(self) -> None:
        args = _parse("launch", str(RUN_ID))

        assert args.run_id == RUN_ID
        assert args.handler is cli._cmd_launch

    def test_rejects_a_malformed_run_id(self) -> None:
        with pytest.raises(SystemExit):
            _parse("launch", "not-a-uuid")

    def test_successful_run_returns_cleanly(self, wiring: dict[str, Any]) -> None:
        cli._cmd_launch(_parse("launch", str(RUN_ID)))

        assert wiring["executed"] == [RUN_ID]
        assert wiring["store"].runs.failed == []

    def test_a_lost_run_exits_the_process(self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
        exits: list[int] = []
        monkeypatch.setattr(cli.os, "_exit", exits.append)
        cli._cmd_launch(_parse("launch", str(RUN_ID)))

        [on_lost] = wiring["on_lost"]
        on_lost()

        assert exits == [1]

    def test_unsuccessful_run_exits_nonzero(self, wiring: dict[str, Any]) -> None:
        wiring["outcome"]["success"] = False

        with pytest.raises(SystemExit) as excinfo:
            cli._cmd_launch(_parse("launch", str(RUN_ID)))

        assert excinfo.value.code == 1
        # A run the executor reported on is already recorded; the command
        # must not overwrite its outcome.
        assert wiring["store"].runs.failed == []

    def test_a_crash_before_the_executor_marks_the_run_failed(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Otherwise the run stays stuck in `dispatched` forever.
        monkeypatch.setattr(
            il.Runner,
            "from_settings",
            classmethod(lambda cls, settings: (_ for _ in ()).throw(RuntimeError("no runner"))),
        )

        with pytest.raises(SystemExit) as excinfo:
            cli._cmd_launch(_parse("launch", str(RUN_ID)))

        assert excinfo.value.code == 1
        assert [run_id for run_id, _ in wiring["store"].runs.failed] == [RUN_ID]

    def test_a_catalog_failure_marks_the_launched_run_failed(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def failing_catalog(cls: Any) -> None:
            raise ValueError("bad catalog setting")

        def store_from_settings(cls: Any, catalog: Any = None) -> FakeStore:
            # The real Store.from_settings loads the catalog itself when given none.
            if catalog is None:
                cli.Catalog.from_settings()
            return wiring["store"]

        monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(failing_catalog))
        monkeypatch.setattr(cli.Store, "from_settings", classmethod(store_from_settings))

        with pytest.raises(SystemExit):
            cli._cmd_launch(_parse("launch", str(RUN_ID)))

        assert [run_id for run_id, _ in wiring["store"].runs.failed] == [RUN_ID]

    def test_a_failing_store_does_not_mask_the_launch_failure(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FakeStore(fail_complete=True)
        monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: store))
        monkeypatch.setattr(
            il.Runner,
            "from_settings",
            classmethod(lambda cls, settings: (_ for _ in ()).throw(RuntimeError("no runner"))),
        )

        with pytest.raises(SystemExit) as excinfo:
            cli._cmd_launch(_parse("launch", str(uuid4())))

        assert excinfo.value.code == 1
