"""Tests for ``interloper_api.cli``."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any

import pytest
from interloper.settings import AppSettings

from interloper_api import cli


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
    """Parse argv through the real registration.

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
    """Stub the bootstrap, catalog, store, server and supervisor the command wires.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        What the command handed each collaborator.
    """
    recorded: dict[str, Any] = {}

    def from_settings(cls: Any, settings: AppSettings, **kwargs: Any) -> object:
        recorded["server"] = (settings, kwargs)
        return object()

    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", kwargs))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(cli.ApiServer, "from_settings", classmethod(from_settings))
    monkeypatch.setattr(cli.Supervisor, "run", lambda self: recorded.setdefault("ran", list(self.services)))
    return recorded


def test_serves_one_api_on_the_port_and_host_flags(wiring: dict[str, Any]) -> None:
    """``--port`` picks the bind port and ``--host`` the bind host."""
    cli._cmd_api(_parse("api", "--port", "3101", "--host", "127.0.0.1"))

    settings, kwargs = wiring["server"]
    assert kwargs["port"] == 3101
    assert settings.server.host == "127.0.0.1"
    assert wiring["ran"] == ["api"]


def test_the_port_defaults_to_the_settings(wiring: dict[str, Any]) -> None:
    """Without ``--port`` the API binds ``server.port``."""
    cli._cmd_api(_parse("api"))

    settings, kwargs = wiring["server"]
    assert kwargs["port"] == settings.server.port


def test_no_create_tables_reaches_the_bootstrap(wiring: dict[str, Any]) -> None:
    """``--no-create-tables`` skips the table bootstrap."""
    cli._cmd_api(_parse("api", "--no-create-tables"))

    assert wiring["bootstrap"]["create_tables"] is False
