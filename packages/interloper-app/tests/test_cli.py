"""Tests for ``interloper_app.cli``."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any

import pytest
from interloper.settings import AppSettings

from interloper_app import cli


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
    """Parse ``app`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    cli.register(parser.add_subparsers(dest="command"))
    return parser.parse_args(argv)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub the database bootstrap, the catalog, the store and the composition.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        What the bootstrap and the composition were handed.
    """
    recorded: dict[str, Any] = {}

    class FakeApp:
        def run(self) -> None:
            recorded["ran"] = True

    def from_settings(cls: Any, settings: AppSettings, **kwargs: Any) -> FakeApp:
        recorded["settings"] = settings
        recorded["app"] = kwargs
        return FakeApp()

    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", (dsn, kwargs)))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(cli.App, "from_settings", classmethod(from_settings))
    return recorded


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


class TestWithOverrides:
    """``interloper app`` flags fold into the loaded settings."""

    def test_omitted_flags_leave_settings_untouched(self) -> None:
        settings = AppSettings()

        result = cli._with_overrides(_parse("app"), settings)

        assert (result.server.host, result.server.port) == (settings.server.host, settings.server.port)
        assert result.server.enabled == settings.server.enabled
        assert (result.cron.enabled, result.worker.enabled, result.reaper.enabled) == (
            settings.cron.enabled,
            settings.worker.enabled,
            settings.reaper.enabled,
        )

    def test_host_and_port_override_the_server_block(self) -> None:
        result = cli._with_overrides(_parse("app", "--host", "0.0.0.0", "--port", "9999"), AppSettings())

        assert (result.server.host, result.server.port) == ("0.0.0.0", 9999)

    @pytest.mark.parametrize("enabled", [True, False])
    def test_every_service_toggle_is_applied(self, enabled: bool) -> None:
        flags = [f"--{'' if enabled else 'no-'}{name}" for name in ("api", "cron", "worker", "reaper")]

        result = cli._with_overrides(_parse("app", *flags), AppSettings())

        assert result.server.enabled is enabled
        assert (result.cron.enabled, result.worker.enabled, result.reaper.enabled) == (enabled, enabled, enabled)

    def test_the_original_settings_are_not_mutated(self) -> None:
        settings = AppSettings()
        original_port = settings.server.port

        cli._with_overrides(_parse("app", "--port", str(original_port + 1)), settings)

        assert settings.server.port == original_port


class TestCmdApp:
    """``interloper app``."""

    def test_the_configured_statement_timeout_reaches_the_bootstrap(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("INTERLOPER_POSTGRES_STATEMENT_TIMEOUT", "12.5")

        cli._cmd_app(_parse("app", "--api"))

        assert wiring["bootstrap"][1]["statement_timeout"] == 12.5

    def test_bootstraps_the_database_and_runs_the_composition(self, wiring: dict[str, Any]) -> None:
        cli._cmd_app(_parse("app", "--api", "--no-cron", "--no-worker", "--no-reaper"))

        expected = {"statement_timeout": None, "create_tables": True}
        assert wiring["bootstrap"] == (wiring["settings"].postgres.dsn, expected)
        assert wiring["ran"] is True
        assert (wiring["app"]["api"], wiring["app"]["cron"], wiring["app"]["worker"], wiring["app"]["reaper"]) == (
            True,
            False,
            False,
            False,
        )
        assert (wiring["app"]["dev"], wiring["app"]["port"]) == (False, None)

    def test_no_create_tables_skips_the_table_bootstrap(self, wiring: dict[str, Any]) -> None:
        cli._cmd_app(_parse("app", "--no-create-tables", "--worker"))

        assert wiring["bootstrap"][1]["create_tables"] is False
        assert wiring["app"]["worker"] is True

    def test_the_flags_are_activated_for_the_command(self, wiring: dict[str, Any]) -> None:
        cli._cmd_app(_parse("app", "--port", "4321"))

        assert AppSettings.get().server.port == 4321
        assert wiring["app"]["port"] == 4321

    def test_dev_and_an_explicit_port_reach_the_composition(self, wiring: dict[str, Any]) -> None:
        cli._cmd_app(_parse("app", "--dev", "--port", "9876", "--api"))

        assert (wiring["app"]["dev"], wiring["app"]["port"]) == (True, 9876)

    def test_scheduler_only_run_needs_no_api(self, wiring: dict[str, Any]) -> None:
        cli._cmd_app(_parse("app", "--no-api", "--cron", "--reaper"))

        assert (wiring["app"]["api"], wiring["app"]["cron"], wiring["app"]["reaper"]) == (False, True, True)
