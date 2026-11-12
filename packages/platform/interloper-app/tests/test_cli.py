"""Tests for ``interloper_app.cli``."""

from __future__ import annotations

import argparse
from datetime import timedelta
from typing import Any

import pytest
from interloper.settings import ServerSettings
from interloper_api.settings import AuthSettings
from interloper_db.settings import PostgresSettings
from interloper_scheduler.settings import CronSettings, ReaperSettings, WorkerSettings

from interloper_app import cli


def _parse(*argv: str) -> argparse.Namespace:
    """Parse ``app`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    cli.AppCommand().attach(parser.add_subparsers(dest="command"), "app")
    return parser.parse_args(argv)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub the database bootstrap, the catalog, the store and the composition.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        What the bootstrap, the store and the composition were handed, and the store itself.
    """
    store = object()
    recorded: dict[str, Any] = {"store": store}

    class FakeApp:
        def run(self) -> None:
            recorded["ran"] = True

    def store_from_settings(cls: Any, catalog: object = None, **lifetimes: timedelta) -> object:
        recorded["lifetimes"] = lifetimes
        return store

    def from_settings(cls: Any, api_settings: Any, scheduler_settings: Any, **kwargs: Any) -> FakeApp:
        recorded["settings"] = (api_settings, scheduler_settings)
        recorded["app"] = kwargs
        return FakeApp()

    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", (dsn, kwargs)))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(store_from_settings))
    monkeypatch.setattr(cli.App, "from_settings", classmethod(from_settings))
    return recorded


class TestArguments:
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


class TestActivateFlags:
    """``interloper app`` flags fold into the sections they override, which the command then runs with."""

    def test_omitted_flags_leave_settings_untouched(self) -> None:
        server, cron = ServerSettings.get(), CronSettings.get()

        cli.AppCommand()._activate_flags(_parse("app"))

        assert ServerSettings.get() == server
        assert CronSettings.get() is cron

    def test_host_and_port_override_the_server_block(self) -> None:
        cli.AppCommand()._activate_flags(_parse("app", "--host", "0.0.0.0", "--port", "9999"))

        assert (ServerSettings.get().host, ServerSettings.get().port) == ("0.0.0.0", 9999)

    @pytest.mark.parametrize("enabled", [True, False])
    def test_every_service_toggle_is_applied(self, enabled: bool) -> None:
        flags = [f"--{'' if enabled else 'no-'}{name}" for name in ("api", "cron", "worker", "reaper")]

        cli.AppCommand()._activate_flags(_parse("app", *flags))

        assert ServerSettings.get().enabled is enabled
        assert (CronSettings.get().enabled, WorkerSettings.get().enabled, ReaperSettings.get().enabled) == (
            enabled,
            enabled,
            enabled,
        )

    def test_the_loaded_section_is_not_mutated(self) -> None:
        server = ServerSettings.get()
        original_port = server.port

        cli.AppCommand()._activate_flags(_parse("app", "--port", str(original_port + 1)))

        assert server.port == original_port


class TestRun:
    """``interloper app``."""

    def test_the_configured_statement_timeout_reaches_the_bootstrap(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("INTERLOPER_POSTGRES_STATEMENT_TIMEOUT", "12.5")

        cli.AppCommand().run(_parse("app", "--api"))

        assert wiring["bootstrap"][1]["statement_timeout"] == 12.5

    def test_bootstraps_the_database_and_runs_the_composition(self, wiring: dict[str, Any]) -> None:
        cli.AppCommand().run(_parse("app", "--api", "--no-cron", "--no-worker", "--no-reaper"))

        expected = {"statement_timeout": None, "create_tables": True}
        assert wiring["bootstrap"] == (PostgresSettings.get().dsn, expected)
        assert wiring["ran"] is True
        assert (wiring["app"]["api"], wiring["app"]["cron"], wiring["app"]["worker"], wiring["app"]["reaper"]) == (
            True,
            False,
            False,
            False,
        )
        assert (wiring["app"]["dev"], wiring["app"]["port"]) == (False, None)

    def test_no_create_tables_skips_the_table_bootstrap(self, wiring: dict[str, Any]) -> None:
        cli.AppCommand().run(_parse("app", "--no-create-tables", "--worker"))

        assert wiring["bootstrap"][1]["create_tables"] is False
        assert wiring["app"]["worker"] is True

    def test_the_flags_are_activated_for_the_command(self, wiring: dict[str, Any]) -> None:
        cli.AppCommand().run(_parse("app", "--port", "4321"))

        assert ServerSettings.get().port == 4321
        assert wiring["app"]["port"] == 4321

    def test_dev_and_an_explicit_port_reach_the_composition(self, wiring: dict[str, Any]) -> None:
        cli.AppCommand().run(_parse("app", "--dev", "--port", "9876", "--api"))

        assert (wiring["app"]["dev"], wiring["app"]["port"]) == (True, 9876)

    def test_every_service_shares_a_store_issuing_tokens_for_the_configured_lifetimes(
        self, wiring: dict[str, Any]
    ) -> None:
        auth = AuthSettings(session_expiry_days=2, access_token_expiry_minutes=15, refresh_token_expiry_days=7)
        AuthSettings.activate(auth)

        cli.AppCommand().run(_parse("app"))

        assert wiring["lifetimes"] == {
            "session_expiry": timedelta(days=2),
            "access_token_expiry": timedelta(minutes=15),
            "refresh_token_expiry": timedelta(days=7),
        }
        assert wiring["app"]["store"] is wiring["store"]

    def test_scheduler_only_run_needs_no_api(self, wiring: dict[str, Any]) -> None:
        cli.AppCommand().run(_parse("app", "--no-api", "--cron", "--reaper"))

        assert (wiring["app"]["api"], wiring["app"]["cron"], wiring["app"]["reaper"]) == (False, True, True)
