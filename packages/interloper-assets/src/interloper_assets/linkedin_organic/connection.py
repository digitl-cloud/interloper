from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin_organic import constants


@il.connection(
    name="LinkedIn Organic",
    icon="devicon:linkedin",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "linkedin",
        scope="r_organization_social,rw_organization_admin,r_organization_social_feed",
    ),
)
class LinkedinOrganicConnection(il.OAuthConnection):
    """LinkedIn Organic API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_organic_")

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
