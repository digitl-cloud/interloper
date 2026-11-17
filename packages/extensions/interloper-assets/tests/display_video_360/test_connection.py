"""Regression tests for the DisplayVideo360 connection.

The discovery services wrap an ``httplib2`` transport that is not thread-safe,
and the runner executes sync assets concurrently in threads, so the connection
caches only the credentials and builds a fresh service on every client call.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import pytest
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection


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


def _connection() -> DisplayVideo360Connection:
    return DisplayVideo360Connection(service_account_key='{"type": "service_account"}')


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

    def test_the_api_is_cached_and_built_on_both_builders(self):
        connection = _connection()
        assert connection.api is connection.api
        assert connection.api.dv_client == connection.dv_client
        assert connection.api.dbm_client == connection.dbm_client


_WITHOUT_GOOGLE_SDKS = """
import sys


class BlockGoogleSdks:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] == "googleapiclient" or name == "google.oauth2" or name.startswith("google.oauth2."):
            raise ModuleNotFoundError(f"No module named {name!r}")


sys.meta_path.insert(0, BlockGoogleSdks())
from interloper_assets.display_video_360.connection import DisplayVideo360Connection
from interloper_assets.display_video_360.connector.source import DisplayVideo360

DisplayVideo360Connection(service_account_key="{}").api
"""


class TestImports:
    def test_the_connection_its_api_and_the_source_import_without_the_google_sdks(self):
        """The API process installs no vendor SDK, and imports every connection and source."""
        subprocess.run([sys.executable, "-c", _WITHOUT_GOOGLE_SDKS], check=True)


_PARTNERS = "https://displayvideo.googleapis.com/v4/partners"


class TestPartners:
    def test_pages_through_every_partner_on_one_service(self, google, connection):
        google.serve(
            {"partners": [{"partnerId": "1", "displayName": "Digitl"}], "nextPageToken": "next"},
            {"partners": [{"partnerId": "2"}]},
        )

        assert asyncio.run(connection.partners()) == [
            {"partner_id": "1", "name": "Digitl"},
            {"partner_id": "2", "name": "2"},
        ]
        assert google.requests == [
            ("GET", f"{_PARTNERS}?alt=json", None),
            ("GET", f"{_PARTNERS}?pageToken=next&alt=json", None),
        ]
        assert google.builds == [constants.DV_API_SERVICE]

    def test_no_partners(self, connection):
        assert asyncio.run(connection.partners()) == []

    def test_check_runs_the_lookup(self, google, connection):
        assert asyncio.run(connection.check()) is True
        assert google.requests == [("GET", f"{_PARTNERS}?alt=json", None)]

    def test_a_refused_check_raises_the_vendors_error(self, google, connection):
        google.serve((403, {"error": {"code": 403, "message": "The caller does not have permission"}}))

        with pytest.raises(HttpError, match="The caller does not have permission"):
            asyncio.run(connection.check())
