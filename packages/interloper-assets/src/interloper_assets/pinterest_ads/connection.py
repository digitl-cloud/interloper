from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.pinterest_ads import constants


@il.connection(
    name="Pinterest Ads",
    icon="logos:pinterest",
    tags=["Advertising"],
    oauth=il.OAuthConfig("pinterest", scope="ads:read"),
)
class PinterestAdsConnection(il.OAuthConnection):
    """Pinterest Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="pinterest_ads_")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                token_endpoint="/oauth/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
