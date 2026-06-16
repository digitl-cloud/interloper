from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_ads import constants


@il.connection(
    name="Facebook Ads",
    icon="logos:facebook",
    tags=["Advertising"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="ads_read,ads_management",
        fields={
            "access_token": "access_token",
            "client_id": "app_id",
            "client_secret": "app_secret",
        },
    ),
)
class FacebookAdsConnection(il.Connection):
    """Facebook Ads (Meta Marketing) API connection."""

    model_config = SettingsConfigDict(env_prefix="facebook_ads_")

    access_token: str = il.SecretField(description="Facebook access token")
    app_id: str = il.InputField(description="Facebook App ID")
    app_secret: str = il.SecretField(description="Facebook App secret")

    @cached_property
    def api(self) -> Any:
        """Initialise and return the Facebook Ads SDK API client."""
        from facebook_business.api import FacebookAdsApi

        return FacebookAdsApi.init(
            app_id=self.app_id,
            app_secret=self.app_secret,
            access_token=self.access_token,
            api_version=f"v{constants.API_VERSION}",
        )
