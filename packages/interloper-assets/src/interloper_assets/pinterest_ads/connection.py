from functools import cached_property
from typing import ClassVar

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.pinterest_ads import constants


@il.connection(
    name="Pinterest Ads",
    icon="logos:pinterest",
    tags=["Advertising"],
)
class PinterestAdsConnection(il.Connection):
    """Pinterest Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="pinterest_ads_")

    oauth: ClassVar[il.OAuthConfig] = il.OAuthConfig(
        provider="pinterest",
        auth_url="https://www.pinterest.com/oauth",
        scope="ads:read",
        label="Pinterest",
        icon="logos:pinterest",
    )
    client_id: str = il.InputField(description="OAuth2 client ID")
    client_secret: str = il.SecretField(description="OAuth2 client secret")
    refresh_token: str = il.SecretField(description="OAuth2 refresh token")

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
