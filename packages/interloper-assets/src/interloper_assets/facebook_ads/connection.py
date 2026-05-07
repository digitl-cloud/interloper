from functools import cached_property
from typing import ClassVar

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_ads.constants import API_VERSION, BASE_URL


@il.connection(
    name="Facebook Ads",
    icon="logos:facebook",
    tags=["Advertising"],
)
class FacebookAdsConnection(il.Connection):
    """Facebook Ads (Meta Marketing) API connection with OAuth2 auth."""

    model_config = SettingsConfigDict(env_prefix="facebook_ads_")

    oauth: ClassVar[il.OAuthConfig] = il.OAuthConfig(
        provider="facebook",
        auth_url="https://www.facebook.com/v19.0/dialog/oauth",
        scope="ads_read,ads_management",
        label="Facebook",
        icon="logos:facebook",
        fields={
            "access_token": "access_token",
            "client_id": "app_id",
            "client_secret": "app_secret",
        },
    )

    access_token: str = il.SecretField(description="Facebook access token")
    app_id: str = il.InputField(description="Facebook App ID")
    app_secret: str = il.SecretField(description="Facebook App secret")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            f"{BASE_URL}/v{API_VERSION}",
            params={"access_token": self.access_token},
        )
