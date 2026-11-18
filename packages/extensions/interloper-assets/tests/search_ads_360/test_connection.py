"""Regression tests for the Search Ads 360 connection."""

from __future__ import annotations

import json

from interloper_assets.google.auth import ServiceAccountAuth
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.constants import BASE_URL, SCOPES

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"


def _connection() -> SearchAds360Connection:
    return SearchAds360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL}))


class TestClient:
    def test_targets_the_reporting_api(self):
        assert str(_connection().client.base_url).rstrip("/") == BASE_URL

    def test_authenticates_as_the_service_account_for_the_reporting_scope(self):
        auth = _connection().client.auth

        assert isinstance(auth, ServiceAccountAuth)
        assert (auth._client_id, auth._scope) == (CLIENT_EMAIL, " ".join(SCOPES))


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client
