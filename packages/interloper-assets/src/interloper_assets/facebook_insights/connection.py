from functools import cached_property
from typing import ClassVar

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_insights import constants


@il.connection(
    name="Facebook Insights",
    icon="logos:facebook",
    tags=["Social"],
)
class FacebookInsightsConnection(il.Connection):
    """Facebook Insights API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="facebook_insights_")

    oauth: ClassVar[il.OAuthConfig] = il.OAuthConfig(
        provider="facebook",
        auth_url="https://www.facebook.com/v19.0/dialog/oauth",
        scope="pages_show_list,pages_read_engagement,pages_read_user_content,read_insights",
        label="Facebook",
        icon="logos:facebook",
    )

    client_id: str = il.InputField(description="Facebook App ID")
    client_secret: str = il.SecretField(description="Facebook App Secret")
    refresh_token: str = il.SecretField(description="OAuth2 long-lived access token")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                token_endpoint="/v21.0/oauth/access_token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
