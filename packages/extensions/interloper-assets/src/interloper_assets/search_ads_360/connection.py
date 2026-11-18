import json
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.google.auth import ServiceAccountAuth
from interloper_assets.search_ads_360.api import SearchAds360Api
from interloper_assets.search_ads_360.constants import BASE_URL, SCOPES


@il.connection(
    name="Search Ads 360",
    icon="devicon:google",
    tags=["Advertising"],
    maturity="beta",
)
class SearchAds360Connection(il.Connection):
    """Search Ads 360 Reporting API connection using Google service account credentials.

    Uses ``google-auth`` for lightweight service account authentication.
    The heavy ``google-ads`` SDK is not required.
    """

    model_config = SettingsConfigDict(env_prefix="search_ads_360_")
    key = "search_ads_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Search Ads 360 Reporting API client every caller shares.

        Returns:
            The client authenticated as the service account, cached per connection instance.
        """
        auth = ServiceAccountAuth(json.loads(self.service_account_key), SCOPES)
        return il.AsyncRESTClient(BASE_URL, auth=auth, timeout=60)

    @cached_property
    def api(self) -> SearchAds360Api:
        """The Search Ads 360 Reporting API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return SearchAds360Api(self.client)
