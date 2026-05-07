from functools import cached_property
from typing import ClassVar

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin_organic import constants


@il.connection(
    name="LinkedIn Organic",
    icon="devicon:linkedin",
    tags=["Social"],
)
class LinkedinOrganicConnection(il.Connection):
    """LinkedIn Organic API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_organic_")

    oauth: ClassVar[il.OAuthConfig] = il.OAuthConfig(
        provider="linkedin",
        auth_url="https://www.linkedin.com/oauth/v2/authorization",
        scope="r_organization_social,rw_organization_admin,r_organization_social_feed",
        label="LinkedIn",
        icon="devicon:linkedin",
    )

    client_id: str = il.InputField(description="LinkedIn OAuth2 client ID")
    client_secret: str = il.SecretField(description="LinkedIn OAuth2 client secret")
    refresh_token: str = il.SecretField(description="LinkedIn OAuth2 refresh token")

    @cached_property
    def client(self) -> il.RESTClient:
        client = il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.AUTH_BASE_URL,
                token_endpoint=constants.TOKEN_ENDPOINT,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
        client.headers.update(
            {
                "LinkedIn-Version": constants.LINKEDIN_VERSION,
                "X-Restli-Protocol-Version": constants.RESTLI_PROTOCOL_VERSION,
                "Accept-Encoding": "gzip, deflate, br",
            }
        )
        return client
