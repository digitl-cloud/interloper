"""Regression tests for the SearchConsole connection."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
from types import SimpleNamespace
from typing import Any

import httplib2
import pytest
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from interloper_assets.search_console import constants
from interloper_assets.search_console.connection import SearchConsoleConnection


class FakeSites:
    """Serve a canned ``sites.list`` response."""

    def __init__(self, response: dict[str, Any], refusal: HttpError | None = None):
        """Hold the response to serve, or the refusal to raise."""
        self.response = response
        self.refusal = refusal

    def sites(self) -> FakeSites:
        return self

    def list(self) -> SimpleNamespace:
        return SimpleNamespace(execute=self.execute)

    def execute(self) -> dict[str, Any]:
        if self.refusal is not None:
            raise self.refusal
        return self.response


def _connection(response: dict[str, Any]) -> SearchConsoleConnection:
    connection = SearchConsoleConnection(service_account_key="{}")
    connection.__dict__["client"] = lambda: FakeSites(response)
    return connection


class TestCredentials:
    def test_loads_the_key_with_the_read_only_scopes(self, monkeypatch: Any):
        calls: list[tuple[dict[str, Any], list[str]]] = []
        sentinel = object()

        def from_service_account_info(info: dict[str, Any], scopes: list[str]) -> object:
            calls.append((info, scopes))
            return sentinel

        monkeypatch.setattr(service_account.Credentials, "from_service_account_info", from_service_account_info)
        connection = SearchConsoleConnection(service_account_key='{"client_email": "reader@example.com"}')
        assert connection.credentials is sentinel
        assert connection.credentials is sentinel
        assert calls == [({"client_email": "reader@example.com"}, constants.SCOPES)]


class TestClient:
    def test_builds_a_fresh_discovery_client_per_call(self, monkeypatch: Any):
        calls: list[tuple[str, str, object]] = []

        def build(service: str, version: str, *, credentials: object) -> object:
            calls.append((service, version, credentials))
            return object()

        monkeypatch.setattr(discovery, "build", build)
        connection = SearchConsoleConnection(service_account_key="{}")
        credentials = object()
        connection.__dict__["credentials"] = credentials
        assert connection.client() is not connection.client()
        assert calls == [("searchconsole", "v1", credentials)] * 2


class TestApi:
    def test_every_engine_is_built_on_a_fresh_client(self):
        clients = [object(), object()]
        connection = SearchConsoleConnection(service_account_key="{}")
        connection.__dict__["client"] = iter(clients).__next__

        first, second = connection.api, connection.api

        assert [first.client, second.client] == clients


class TestImport:
    def test_the_connection_and_its_source_load_without_the_sdk(self):
        script = (
            "import sys\n"
            "sys.modules['googleapiclient'] = None\n"
            "sys.modules['httplib2'] = None\n"
            "sys.modules['google.oauth2'] = None\n"
            "import interloper_assets.search_console.connection\n"
            "import interloper_assets.search_console.connector.source\n"
        )

        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)

        assert result.returncode == 0, result.stderr


class TestSites:
    def test_lists_every_property_with_its_permission_level(self):
        connection = _connection(
            {
                "siteEntry": [
                    {"siteUrl": "sc-domain:example.com", "permissionLevel": "SITE_OWNER"},
                    {"siteUrl": "https://example.org/", "permissionLevel": "SITE_FULL_USER"},
                ]
            }
        )
        assert asyncio.run(connection.sites()) == [
            {"site_url": "sc-domain:example.com", "name": "sc-domain:example.com (SITE_OWNER)"},
            {"site_url": "https://example.org/", "name": "https://example.org/ (SITE_FULL_USER)"},
        ]

    def test_the_client_is_built_off_the_event_loop(self):
        built_in: list[int] = []

        def client() -> FakeSites:
            built_in.append(threading.get_ident())
            return FakeSites({})

        async def lookup() -> int:
            await connection.sites()
            return threading.get_ident()

        connection = SearchConsoleConnection(service_account_key="{}")
        connection.__dict__["client"] = client

        loop_thread = asyncio.run(lookup())

        assert len(built_in) == 1
        assert built_in[0] != loop_thread

    def test_no_properties(self):
        assert asyncio.run(_connection({}).sites()) == []

    def test_check_runs_the_lookup(self):
        assert asyncio.run(_connection({}).check()) is True

    def test_a_refusal_raises_out_of_the_check_as_google_sent_it(self):
        refusal = HttpError(httplib2.Response({"status": 403}), b'{"error": {"message": "Permission denied"}}')
        connection = SearchConsoleConnection(service_account_key="{}")
        connection.__dict__["client"] = lambda: FakeSites({}, refusal=refusal)

        with pytest.raises(HttpError) as raised:
            asyncio.run(connection.check())

        assert raised.value is refusal
