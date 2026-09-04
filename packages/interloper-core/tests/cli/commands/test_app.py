"""Tests for ``interloper.cli.commands.app``."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any

import interloper_db
import pytest

from interloper.catalog import Catalog
from interloper.cli.commands import app as app_command
from interloper.cli.runtime import apply_cli_overrides
from interloper.settings import AppSettings


def _parse(*argv: str) -> argparse.Namespace:
    """Parse ``app`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    app_command.register(parser.add_subparsers(dest="command"))
    return parser.parse_args(argv)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub the database bootstrap and the service runtime.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        Recorded bootstrap calls and the kwargs ``Services`` was built with.
    """
    recorded: dict[str, Any] = {"ensure_database": [], "init_engine": [], "create_all": [], "services": []}

    class FakeServices:
        def __init__(self, **kwargs: Any) -> None:
            recorded["services"].append(kwargs)

        def run(self) -> None:
            recorded["ran"] = True

    monkeypatch.setattr(interloper_db, "ensure_database", lambda dsn: recorded["ensure_database"].append(dsn))
    monkeypatch.setattr(interloper_db, "init_engine", lambda dsn: recorded["init_engine"].append(dsn))
    monkeypatch.setattr(interloper_db, "create_all", lambda *a: recorded["create_all"].append(a))
    monkeypatch.setattr(Catalog, "from_settings", classmethod(lambda cls: Catalog()))
    monkeypatch.setattr(interloper_db.Store, "from_settings", classmethod(lambda cls, catalog: object()))
    monkeypatch.setattr("interloper.cli.runtime.Services", FakeServices)
    return recorded


@pytest.fixture(autouse=True)
def _clear_active_settings() -> Iterator[None]:
    """Keep the activated settings each test installs out of the next one.

    Yields:
        ``None``; the teardown clears the active settings.
    """
    yield
    AppSettings.clear_active()


def _activate(**toggles: bool) -> AppSettings:
    """Activate settings with the given service toggles.

    Args:
        toggles: ``api``, ``cron``, ``worker`` and/or ``reaper`` flags.

    Returns:
        The activated settings.
    """
    args = argparse.Namespace(api=False, cron=False, worker=False, reaper=False)
    for name, value in toggles.items():
        setattr(args, name, value)
    settings = apply_cli_overrides(args, AppSettings())
    AppSettings.activate(settings)
    return settings


class TestRegister:
    """Parser wiring for the ``app`` command."""

    def test_service_toggles_default_to_unset(self) -> None:
        args = _parse("app")

        # None (not False) so settings, not the parser, decide the default.
        assert (args.api, args.cron, args.worker, args.reaper) == (None, None, None, None)
        assert (args.host, args.port) == (None, None)
        assert args.dev is False
        assert args.no_create_tables is False

    def test_negated_toggles_are_supported(self) -> None:
        args = _parse("app", "--api", "--no-cron", "--worker", "--no-reaper")

        assert (args.api, args.cron, args.worker, args.reaper) == (True, False, True, False)

    def test_declares_its_conditional_requirements(self) -> None:
        args = _parse("app")

        assert args.requires == ["interloper_db"]
        assert args.requires_when == {
            "api": ["interloper_api"],
            "cron": ["interloper_scheduler"],
            "worker": ["interloper_scheduler"],
            "reaper": ["interloper_scheduler"],
        }
        assert args.handler is app_command._cmd_app


class TestCmdApp:
    """``interloper app``."""

    def test_no_enabled_service_is_a_clean_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        _activate(api=False, cron=False, worker=False, reaper=False)

        with pytest.raises(SystemExit) as excinfo:
            app_command._cmd_app(_parse("app"))

        assert excinfo.value.code == 1
        assert "No services enabled" in capsys.readouterr().err

    def test_bootstraps_the_database_and_starts_the_services(self, wiring: dict[str, Any]) -> None:
        settings = _activate(api=True)

        app_command._cmd_app(_parse("app"))

        assert wiring["ensure_database"] == [settings.postgres.dsn]
        assert wiring["init_engine"] == [settings.postgres.dsn]
        assert wiring["create_all"] == [()]
        assert wiring["ran"] is True

        built = wiring["services"][0]
        assert built["run_api"] is True
        assert (built["run_cron"], built["run_worker"], built["run_reaper"]) == (False, False, False)
        assert built["dev_mode"] is False
        assert built["api_port"] == settings.server.port

    def test_no_create_tables_skips_the_create_all_bootstrap(self, wiring: dict[str, Any]) -> None:
        _activate(worker=True)

        app_command._cmd_app(_parse("app", "--no-create-tables"))

        assert wiring["create_all"] == []
        assert wiring["services"][0]["run_worker"] is True

    def test_dev_mode_moves_the_api_off_the_nuxt_port(self, wiring: dict[str, Any]) -> None:
        settings = _activate(api=True)

        app_command._cmd_app(_parse("app", "--dev"))

        built = wiring["services"][0]
        assert built["dev_mode"] is True
        assert built["api_port"] != settings.server.port

    def test_an_explicit_port_wins_over_dev_mode(self, wiring: dict[str, Any]) -> None:
        args = _parse("app", "--dev", "--port", "9876", "--api")
        AppSettings.activate(apply_cli_overrides(args, AppSettings()))

        app_command._cmd_app(args)

        assert wiring["services"][0]["api_port"] == 9876

    def test_scheduler_only_run_needs_no_api(self, wiring: dict[str, Any]) -> None:
        _activate(api=False, cron=True, reaper=True)

        app_command._cmd_app(_parse("app"))

        built = wiring["services"][0]
        assert (built["run_api"], built["run_cron"], built["run_reaper"]) == (False, True, True)
