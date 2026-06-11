from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.instagram_insights import constants


@il.connection(
    name="Instagram Insights",
    icon="skill-icons:instagram",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement",
    ),
)
class InstagramInsightsConnection(il.Connection):
    """Instagram Insights API connection with OAuth2 refresh token auth via Facebook Graph API."""

    model_config = SettingsConfigDict(env_prefix="instagram_insights_")

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
