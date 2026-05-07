from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="TikTok Ads",
    icon="logos:tiktok-icon",
    tags=["Advertising"],
)
class TiktokAdsConnection(il.Connection):
    """TikTok Ads API connection with bearer token auth."""

    model_config = SettingsConfigDict(env_prefix="tiktok_ads_")

    access_token: str = il.SecretField(description="TikTok Ads API access token")

    @cached_property
    def client(self) -> il.RESTClient:
        client = il.RESTClient(
            "https://business-api.tiktok.com/open_api/v1.3",
            auth=il.HTTPBearerAuth(token=self.access_token),
        )
        return client
