"""Regression tests for the CampaignManager360 connection.

The discovery service wraps an ``httplib2`` transport that is not thread-safe,
and the runner executes sync assets concurrently in threads, so the connection
caches only the credentials and builds a fresh service on every ``client()``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from google.oauth2 import service_account
from googleapiclient import discovery

from interloper_assets.campaign_manager_360 import constants
from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection


class FakeUserProfiles:
    """Serve a canned ``userProfiles.list`` response."""

    def __init__(self, response: dict[str, Any]):
        """Hold the response to serve."""
        self.response = response

    def userProfiles(self) -> FakeUserProfiles:
        return self

    def list(self) -> SimpleNamespace:
        return SimpleNamespace(execute=lambda: self.response)


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


def _connection(response: dict[str, Any] | None = None) -> CampaignManager360Connection:
    connection = CampaignManager360Connection(service_account_key='{"type": "service_account"}')
    if response is not None:
        connection.__dict__["client"] = lambda: FakeUserProfiles(response)
    return connection


class TestClient:
    def test_credentials_are_cached_and_scoped(self, builds: list[dict[str, Any]]):
        connection = _connection()
        credentials = connection.credentials
        assert connection.credentials is credentials
        assert credentials.info == {"type": "service_account"}
        assert credentials.scopes == constants.SCOPES

    def test_every_call_builds_its_own_service(self, builds: list[dict[str, Any]]):
        connection = _connection()
        assert connection.client() is not connection.client()
        assert [(build["service_name"], build["version"]) for build in builds] == [
            (constants.API_SERVICE, constants.API_VERSION)
        ] * 2
        assert builds[0]["credentials"] is builds[1]["credentials"] is connection.credentials


class TestProfiles:
    def test_lists_every_profile_with_its_account(self):
        connection = _connection(
            {
                "items": [
                    {"profileId": "111", "accountId": "222", "userName": "reporting", "accountName": "Digitl"},
                    {"profileId": 333, "accountId": 444},
                ]
            }
        )
        assert asyncio.run(connection.profiles()) == [
            {
                "profile_id": "111",
                "account_id": "222",
                "name": "reporting (Digitl)",
                "account_name": "Digitl (222)",
            },
            {"profile_id": "333", "account_id": "444", "name": "333 (444)", "account_name": "444 (444)"},
        ]

    def test_no_profiles(self):
        assert asyncio.run(_connection({}).profiles()) == []

    def test_check_runs_the_lookup(self):
        assert asyncio.run(_connection({}).check()) is True
