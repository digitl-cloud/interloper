"""Regression tests for the DisplayVideo360 connection.

Its partner lookup and ``check()`` run in the API process, which installs no
vendor SDK extra, so they go over plain HTTP through the engine like the
assets do.
"""

from __future__ import annotations

import json
import sys

import interloper as il
import pytest

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection
from interloper_assets.google.auth import ServiceAccountAuth

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"
_PARTNERS = "https://displayvideo.googleapis.com/v4/partners"


class TestClient:
    def test_authenticates_as_the_service_account_for_both_apis(self):
        connection = DisplayVideo360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL}))
        client = connection.client

        assert str(client.base_url).rstrip("/") == constants.DISPLAY_VIDEO_URL
        assert isinstance(client.auth, ServiceAccountAuth)
        assert client.auth._client_id == CLIENT_EMAIL
        assert client.auth._scope == (
            "https://www.googleapis.com/auth/display-video https://www.googleapis.com/auth/doubleclickbidmanager"
        )
        assert connection.client is client


class TestApi:
    def test_the_api_is_cached_and_built_on_the_client(self, connection):
        assert connection.api is connection.api
        assert connection.api.client is connection.client


class TestPartners:
    async def test_pages_through_every_partner(self, google, connection):
        google.serve(
            {"partners": [{"partnerId": "1", "displayName": "Digitl"}], "nextPageToken": "next"},
            {"partners": [{"partnerId": "2"}]},
        )

        assert await connection.partners() == [
            {"partner_id": "1", "name": "Digitl"},
            {"partner_id": "2", "name": "2"},
        ]
        assert google.requests == [
            ("GET", _PARTNERS, None),
            ("GET", f"{_PARTNERS}?pageToken=next", None),
        ]

    async def test_no_partners(self, connection):
        assert await connection.partners() == []

    async def test_runs_without_the_sdk_extras(self, google, connection, monkeypatch: pytest.MonkeyPatch):
        for module in ("bingads", "facebook_business"):
            monkeypatch.setitem(sys.modules, module, None)
        google.serve({"partners": [{"partnerId": "1"}]})

        assert await connection.partners() == [{"partner_id": "1", "name": "1"}]


class TestCheck:
    async def test_runs_the_lookup(self, google, connection):
        assert await connection.check() is True
        assert google.requests == [("GET", _PARTNERS, None)]

    async def test_a_refusal_reads_as_rejected_credentials(self, google, connection):
        google.serve((403, {"error": {"code": 403, "message": "The caller does not have permission"}}))

        result = await connection.verify(timeout=5)

        assert (result.ok, result.category) == (False, "auth")


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert (
            DisplayVideo360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL})).client.retry
            == il.HttpRetryPolicy()
        )
