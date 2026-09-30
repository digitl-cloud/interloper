"""Regression tests for the SearchConsole connection."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

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
