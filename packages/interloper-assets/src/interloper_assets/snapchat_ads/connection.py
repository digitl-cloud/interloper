from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Snapchat Ads",
    icon="mdi:snapchat",
    tags=["Advertising"],
    oauth=il.OAuthConfig("snapchat", scope="snapchat-marketing-api"),
)
class SnapchatAdsConnection(il.OAuthConnection):
    """Snapchat Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="snapchat_ads_")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            "https://adsapi.snapchat.com",
            auth=il.OAuth2RefreshTokenAuth(
                base_url="https://accounts.snapchat.com",
                token_endpoint="/login/oauth2/access_token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
