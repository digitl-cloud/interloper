"""Tests for the Facebook Ads connection.

The connection exposes the Graph API client as ``client`` and the Marketing
API engine every asset reads through as ``api``, built on that client.
"""

from __future__ import annotations

from interloper_assets.facebook_ads.api import FacebookGraphApi, FacebookMarketingApi
from interloper_assets.facebook_ads.connection import FacebookAdsConnection


def _connection() -> FacebookAdsConnection:
    return FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")


class TestApi:
    def test_every_request_goes_through_the_graph_client(self):
        assert isinstance(_connection().client, FacebookGraphApi)

    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert isinstance(connection.api, FacebookMarketingApi)
        assert connection.api.client is connection.client
