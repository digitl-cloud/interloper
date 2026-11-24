"""Tests for ``interloper_scheduler.cli``."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

import pytest
from interloper_db import Store

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

    monkeypatch.setattr("interloper_db.provision.connect", lambda dsn, **kwargs: recorded.setdefault("connect", kwargs))
    monkeypatch.setattr(
        "interloper_db.provision.bootstrap", lambda *args, **kwargs: pytest.fail("a serving command wrote the schema")
    )
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr("interloper_scheduler.services.scheduler_services", scheduler_services)
    monkeypatch.setattr(cli.Supervisor, "run", lambda self: recorded.update(ran=list(self.services), ready=self.ready))
    monkeypatch.setattr(
        cli, "configure_console_logging", lambda level=logging.INFO, **kwargs: recorded.update(level=level)
    )
    return recorded


class TestScheduler:
    """``interloper scheduler``."""

    def test_the_help_loads_neither_the_controllers_nor_the_database(self, help_modules: Callable[..., set[str]]):
        assert {"sqlalchemy", "interloper_db", "interloper_scheduler.services"}.isdisjoint(
            help_modules("scheduler", "--help")
        )

    def test_flags_override_the_settings(self, services: dict[str, Any]) -> None:
        cli.SchedulerCommand().run(_parse("scheduler", "--no-worker"))

        assert services["services"]["cron"] is True
        assert services["services"]["worker"] is False
        assert services["ran"] == ["stub"]

    def test_a_start_ends_in_one_ready_line_without_a_url(self, services: dict[str, Any]) -> None:
        cli.SchedulerCommand().run(_parse("scheduler"))

        ready = services["ready"]
        assert (ready.url, list(ready.services), ready.definitions) == (None, ["stub"], 0)
        assert ready.logger.name == "interloper_scheduler.cli"

    def test_verbose_prints_debug_lines(self, services: dict[str, Any]) -> None:
        cli.SchedulerCommand().run(_parse("scheduler", "-v"))

        assert services["level"] == logging.DEBUG

    def test_no_enabled_service_is_an_error(self, services: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
        CronSettings.activate(CronSettings(enabled=False))
        WorkerSettings.activate(WorkerSettings(enabled=False))
        ReaperSettings.activate(ReaperSettings(enabled=False))

        with pytest.raises(SystemExit, match="No scheduler service enabled"):
            cli.SchedulerCommand().run(_parse("scheduler"))

        assert "ran" not in services

    def test_the_scheduler_checks_the_schema_and_writes_none(self, services: dict[str, Any]) -> None:
        cli.SchedulerCommand().run(_parse("scheduler"))

        assert services["connect"] == {"statement_timeout": None}

    def test_there_is_no_flag_to_skip_a_bootstrap_that_no_longer_runs(self) -> None:
        with pytest.raises(SystemExit):
            _parse("scheduler", "--no-create-tables")


# -- launch ----------------------------------------------------------------------
