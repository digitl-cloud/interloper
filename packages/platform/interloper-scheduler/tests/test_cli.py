"""Tests for ``interloper_scheduler.cli``."""

from __future__ import annotations

import argparse
from typing import Any
from uuid import UUID

import pytest

from interloper_scheduler import cli
from interloper_scheduler.settings import CronSettings, ReaperSettings, SchedulerSettings, WorkerSettings

RUN_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _parse(*argv: str) -> argparse.Namespace:
    """Parse argv through the real registrations.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    subparsers = parser.add_subparsers(dest="command")
    cli.SchedulerCommand().attach(subparsers, "scheduler")
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

    def scheduler_services(settings: SchedulerSettings, **kwargs: Any) -> dict[str, object]:
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
        cli.SchedulerCommand().run(_parse("scheduler", "--no-worker"))

        assert services["services"]["cron"] is True
        assert services["services"]["worker"] is False
        assert services["ran"] == ["stub"]

    def test_no_enabled_service_is_an_error(self, services: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
        CronSettings.activate(CronSettings(enabled=False))
        WorkerSettings.activate(WorkerSettings(enabled=False))
        ReaperSettings.activate(ReaperSettings(enabled=False))

        with pytest.raises(SystemExit, match="No scheduler service enabled"):
            cli.SchedulerCommand().run(_parse("scheduler"))

        assert "ran" not in services

    def test_no_create_tables_reaches_the_bootstrap(self, services: dict[str, Any]) -> None:
        cli.SchedulerCommand().run(_parse("scheduler", "--no-create-tables"))

        assert services["bootstrap"]["create_tables"] is False


# -- launch ----------------------------------------------------------------------
