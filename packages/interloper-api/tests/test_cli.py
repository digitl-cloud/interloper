"""Tests for ``interloper_api.cli``."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from typing import Any

import pytest
from interloper.settings import AppSettings
from interloper_db import Store

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
    subparsers = parser.add_subparsers(dest="command")
    cli.register(subparsers)
    cli.register_oauth(subparsers)
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


@pytest.fixture
def clients(oauth_store: Store, monkeypatch: pytest.MonkeyPatch) -> Store:
    """Point the ``oauth`` commands at a store over an in-memory database.

    Args:
        oauth_store: The store carrying the OAuth tables.
        monkeypatch: Fixture used to swap the store constructor.

    Returns:
        The store the commands read and write.
    """
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: oauth_store))
    return oauth_store


def _run(*argv: str) -> None:
    """Run a ``oauth`` subcommand through the real parser.

    Args:
        argv: The arguments after ``oauth``.
    """
    args = _parse("oauth", *argv)
    args.handler(args)


def test_clients_create_prints_secret_once(clients: Store, capsys: pytest.CaptureFixture[str]) -> None:
    """``oauth create`` prints the id and the secret, and nothing afterwards can show the secret again."""
    _run("create", "--name", "Slack", "--redirect-uri", "https://slack.example.com/cb", "--redirect-uri", "cursor://cb")
    created = capsys.readouterr().out

    [client] = clients.oauth.list_clients(registered_by="operator")
    assert client.name == "Slack"
    assert client.redirect_uris == ["https://slack.example.com/cb", "cursor://cb"]
    assert client.client_id in created
    secret = next(word for word in created.split() if word.startswith("ilc_"))
    assert client.secret_hash is not None

    _run("list")
    listed = capsys.readouterr().out
    assert client.client_id in listed
    assert "Slack" in listed
    assert secret not in listed


def test_clients_create_refuses_an_unsafe_redirect_uri(clients: Store) -> None:
    """A redirect URI dynamic registration would refuse is refused here and nothing is registered."""
    with pytest.raises(SystemExit, match="javascript:alert"):
        _run("create", "--name", "Evil", "--redirect-uri", "javascript:alert(1)")

    assert clients.oauth.list_clients() == []


def test_clients_create_needs_a_redirect_uri() -> None:
    """A client with no redirect URI could never receive a code."""
    with pytest.raises(SystemExit):
        _parse("oauth", "create", "--name", "Slack")


def test_clients_list_shows_operator_clients_only(clients: Store, capsys: pytest.CaptureFixture[str]) -> None:
    """Dynamically registered clients belong to their users, not to the operator."""
    clients.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")
    clients.oauth.register_client(
        "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
    )

    _run("list")

    listed = capsys.readouterr().out
    assert "Slack" in listed
    assert "Claude Code" not in listed


def test_clients_delete_removes_the_client(clients: Store, capsys: pytest.CaptureFixture[str]) -> None:
    """``oauth delete`` removes an operator client by id."""
    client, _ = clients.oauth.register_client(
        "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
    )

    _run("delete", client.client_id)

    assert clients.oauth.get_client(client.client_id) is None
    assert client.client_id in capsys.readouterr().out


def test_clients_delete_refuses_an_unknown_or_dynamic_client(clients: Store) -> None:
    """Only an operator client can be deleted here; a user's own app is theirs to revoke."""
    dynamic, _ = clients.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")

    with pytest.raises(SystemExit, match="no-such-client"):
        _run("delete", "no-such-client")
    with pytest.raises(SystemExit, match=dynamic.client_id):
        _run("delete", dynamic.client_id)

    assert clients.oauth.get_client(dynamic.client_id) is not None
