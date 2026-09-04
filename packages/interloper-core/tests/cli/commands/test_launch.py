"""Tests for ``interloper.cli.commands.launch``."""

from __future__ import annotations

import argparse
from typing import Any
from uuid import UUID, uuid4

import interloper_db
import interloper_scheduler
import pytest

import interloper as il
from interloper.catalog import Catalog
from interloper.cli.commands import launch as launch_command

RUN_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class FakeRuns:
    """Records the run-completion calls the failure path makes."""

    def __init__(self, fail: bool = False) -> None:
        """Set up the recorder.

        Args:
            fail: Whether ``complete`` itself should raise, mimicking a store
                that is unreachable while reporting the original failure.
        """
        self.completed: list[tuple[UUID, bool]] = []
        self.fail = fail

    def complete(self, run_id: UUID, success: bool) -> None:
        """Record (or refuse) a completion.

        Args:
            run_id: The run being completed.
            success: Whether the run succeeded.

        Raises:
            RuntimeError: When this fake is configured to fail.
        """
        if self.fail:
            raise RuntimeError("store unreachable")
        self.completed.append((run_id, success))


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
    """Stub out the catalog, store and executor the command builds.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        The stubbed store plus the recorded executor calls.
    """
    store = FakeStore()
    executed: list[UUID] = []
    outcome = {"success": True}

    class FakeExecutor:
        def __init__(self, store: Any, runner: Any) -> None:
            self.store = store
            self.runner = runner

        def execute(self, run_id: UUID) -> bool:
            executed.append(run_id)
            return outcome["success"]

    monkeypatch.setattr(Catalog, "from_settings", classmethod(lambda cls: Catalog()))
    monkeypatch.setattr(interloper_db.Store, "from_settings", classmethod(lambda cls, catalog: store))
    monkeypatch.setattr(interloper_scheduler, "RunExecutor", FakeExecutor)
    return {"store": store, "executed": executed, "outcome": outcome}


def _args(run_id: UUID = RUN_ID) -> argparse.Namespace:
    return argparse.Namespace(run_id=run_id)


class TestRegister:
    """Parser wiring for the ``launch`` command."""

    def test_parses_the_run_id_as_a_uuid(self) -> None:
        parser = argparse.ArgumentParser(prog="interloper")
        launch_command.register(parser.add_subparsers(dest="command"))

        args = parser.parse_args(["launch", str(RUN_ID)])

        assert args.run_id == RUN_ID
        assert args.handler is launch_command._cmd_launch
        assert args.requires == ["interloper_db", "interloper_scheduler"]

    def test_rejects_a_malformed_run_id(self) -> None:
        parser = argparse.ArgumentParser(prog="interloper")
        launch_command.register(parser.add_subparsers(dest="command"))

        with pytest.raises(SystemExit):
            parser.parse_args(["launch", "not-a-uuid"])


class TestLaunch:
    """``interloper launch <run_id>``."""

    def test_successful_run_returns_cleanly(self, wiring: dict[str, Any]) -> None:
        launch_command._cmd_launch(_args())

        assert wiring["executed"] == [RUN_ID]
        assert wiring["store"].runs.completed == []

    def test_unsuccessful_run_exits_nonzero(self, wiring: dict[str, Any]) -> None:
        wiring["outcome"]["success"] = False

        with pytest.raises(SystemExit) as excinfo:
            launch_command._cmd_launch(_args())

        assert excinfo.value.code == 1
        # A run the executor reported on is already recorded; the command
        # must not overwrite its outcome.
        assert wiring["store"].runs.completed == []

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
            launch_command._cmd_launch(_args())

        assert excinfo.value.code == 1
        assert wiring["store"].runs.completed == [(RUN_ID, False)]

    def test_a_failing_store_does_not_mask_the_launch_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FakeStore(fail_complete=True)
        monkeypatch.setattr(Catalog, "from_settings", classmethod(lambda cls: Catalog()))
        monkeypatch.setattr(interloper_db.Store, "from_settings", classmethod(lambda cls, catalog: store))
        monkeypatch.setattr(
            il.Runner,
            "from_settings",
            classmethod(lambda cls, settings: (_ for _ in ()).throw(RuntimeError("no runner"))),
        )

        with pytest.raises(SystemExit) as excinfo:
            launch_command._cmd_launch(_args(uuid4()))

        assert excinfo.value.code == 1
