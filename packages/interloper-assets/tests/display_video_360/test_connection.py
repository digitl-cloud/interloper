"""Regression tests for the DisplayVideo360 connection.

The discovery services wrap an ``httplib2`` transport that is not thread-safe,
and the runner executes sync assets concurrently in threads, so the connection
caches only the credentials and builds a fresh service on every client call.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from google.oauth2 import service_account
from googleapiclient import discovery

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection


class FakePartners:
    """Serve canned ``partners.list`` pages, keyed by page token."""

    def __init__(self, pages: dict[str | None, dict[str, Any]]):
        """Hold the pages to serve."""
        self.pages = pages

    def partners(self) -> FakePartners:
        return self

    def list(self, pageToken: str | None) -> SimpleNamespace:
        return SimpleNamespace(execute=lambda: self.pages[pageToken])


@pytest.fixture
def builds(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Stub the Google auth and discovery calls, recording every service build.

    Args:
        monkeypatch: The pytest monkeypatch fixture that undoes the stubs after the test.

    Returns:
        The recorded builds, in call order: each one's service name, version and credentials.
    """
    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info, scopes: SimpleNamespace(info=info, scopes=scopes),
    )

    def build(service_name: str, version: str, credentials: Any) -> object:
        recorded.append({"service_name": service_name, "version": version, "credentials": credentials})
        return object()

    monkeypatch.setattr(discovery, "build", build)
    return recorded


def _connection(pages: dict[str | None, dict[str, Any]] | None = None) -> DisplayVideo360Connection:
    connection = DisplayVideo360Connection(service_account_key='{"type": "service_account"}')
    if pages is not None:
        connection.__dict__["dv_client"] = lambda: FakePartners(pages)
    return connection


class TestClients:
    def test_credentials_are_cached_and_cover_both_apis(self, builds: list[dict[str, Any]]):
        connection = _connection()
        credentials = connection.credentials
        assert connection.credentials is credentials
        assert credentials.info == {"type": "service_account"}
        assert credentials.scopes == [*constants.DV_SCOPES, *constants.DBM_SCOPES]

    def test_every_call_builds_its_own_service(self, builds: list[dict[str, Any]]):
        connection = _connection()
        assert connection.dv_client() is not connection.dv_client()
        assert connection.dbm_client() is not connection.dbm_client()
        assert [(build["service_name"], build["version"]) for build in builds] == [
            (constants.DV_API_SERVICE, constants.DV_API_VERSION),
            (constants.DV_API_SERVICE, constants.DV_API_VERSION),
            (constants.DBM_API_SERVICE, constants.DBM_API_VERSION),
            (constants.DBM_API_SERVICE, constants.DBM_API_VERSION),
        ]
        assert all(build["credentials"] is connection.credentials for build in builds)


class TestPartners:
    def test_pages_through_every_partner(self):
        connection = _connection(
            {
                None: {"partners": [{"partnerId": "1", "displayName": "Digitl"}], "nextPageToken": "next"},
                "next": {"partners": [{"partnerId": "2"}]},
            }
        )
        assert asyncio.run(connection.partners()) == [
            {"partner_id": "1", "name": "Digitl (1)"},
            {"partner_id": "2", "name": "2 (2)"},
        ]

    def test_pagination_reuses_one_service(self):
        services: list[FakePartners] = []
        connection = _connection()

        def dv_client() -> FakePartners:
            services.append(FakePartners({None: {"nextPageToken": "next"}, "next": {}}))
            return services[-1]

        connection.__dict__["dv_client"] = dv_client
        assert connection._list_partners() == []
        assert len(services) == 1

    def test_no_partners(self):
        assert asyncio.run(_connection({None: {}}).partners()) == []

    def test_check_runs_the_lookup(self):
        assert asyncio.run(_connection({None: {}}).check()) is True
