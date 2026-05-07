from enum import Enum
from functools import cached_property
from typing import ClassVar

import interloper as il
from pydantic_settings import SettingsConfigDict


class AmazonAdsAPILocation(Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://advertising-api-eu.amazon.com",
            AmazonAdsAPILocation.FAR_EAST: "https://advertising-api-fe.amazon.com",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://advertising-api.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://api.amazon.co.uk",
            AmazonAdsAPILocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


@il.connection(
    name="Amazon Ads",
    icon="icon:amazon",
    tags=["Advertising"],
)
class AmazonAdsConnection(il.Connection):
    """Amazon Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="amazon_ads_")

    oauth: ClassVar[il.OAuthConfig] = il.OAuthConfig(
        provider="amazon",
        auth_url="https://www.amazon.com/ap/oa",
        scope="advertising::campaign_management",
        label="Amazon",
        icon="icon:amazon",
    )
    location: str = il.SelectField(
        options=[
            {"label": "Europe", "value": "EU"},
            {"label": "North America", "value": "NA"},
            {"label": "Far East", "value": "FE"},
        ],
        description="API region",
    )
    client_id: str = il.InputField(description="OAuth2 client ID")
    client_secret: str = il.SecretField(description="OAuth2 client secret")
    refresh_token: str = il.SecretField(description="OAuth2 refresh token")

    @cached_property
    def api_location(self) -> AmazonAdsAPILocation:
        return AmazonAdsAPILocation(self.location)

    @cached_property
    def client(self) -> il.RESTClient:
        location = self.api_location
        client = il.RESTClient(
            location.api_url,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=location.auth_url,
                token_endpoint="/auth/o2/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
        client.base_url = location.api_url
        client.headers.update({"Amazon-Advertising-API-ClientId": self.client_id})
        return client
