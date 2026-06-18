from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.tiktok_ads.constants import BASE_URL


@il.connection(
    name="TikTok Ads",
    icon="logos:tiktok-icon",
    tags=["Advertising"],
    oauth=il.OAuthConfig("tiktok", fields={"access_token": "access_token"}),
)
class TiktokAdsConnection(il.Connection):
    """TikTok Ads (Business API) connection authenticated with a long-lived access token.

    TikTok's token exchange returns a single long-lived ``access_token`` (no
    refresh token), so this stays a plain ``Connection`` with a custom
    ``OAuthConfig.fields`` mapping rather than subclassing ``OAuthConnection``.
    """

    model_config = SettingsConfigDict(env_prefix="tiktok_ads_")

    access_token: str = il.SecretField(description="TikTok Ads API access token")

    @cached_property
    def client(self) -> il.RESTClient:
        # The TikTok Business API authenticates via the "Access-Token" header, not Bearer.
        return il.RESTClient(BASE_URL, headers={"Access-Token": self.access_token})
