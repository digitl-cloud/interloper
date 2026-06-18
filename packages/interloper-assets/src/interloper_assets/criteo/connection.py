from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.criteo import constants


@il.connection(
    name="Criteo",
    icon="icon:criteo",
    tags=["Advertising"],
    oauth=il.OAuthConfig("criteo"),
)
class CriteoConnection(il.OAuthConnection):
    """Criteo API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="criteo_")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                token_endpoint="/oauth2/token",
            ),
        )
