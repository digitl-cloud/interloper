"""Regression tests for the SearchConsole connection."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from google.oauth2 import service_account
from googleapiclient import discovery

from interloper_assets.search_console import constants
from interloper_assets.search_console.connection import SearchConsoleConnection


class FakeSites:
    """Serve a canned ``sites.list`` response."""

    def __init__(self, response: dict[str, Any]):
        """Hold the response to serve."""
        self.response = response

    def sites(self) -> FakeSites:
        return self

    def list(self) -> SimpleNamespace:
        return SimpleNamespace(execute=lambda: self.response)


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

    def test_no_properties(self):
        assert asyncio.run(_connection({}).sites()) == []

    def test_check_runs_the_lookup(self):
        assert asyncio.run(_connection({}).check()) is True
