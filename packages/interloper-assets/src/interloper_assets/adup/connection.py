from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.adup import constants


@il.connection(
    name="Adup",
    icon="icon:adup",
    tags=["Advertising"],
)
class AdupConnection(il.Connection):
    """Adup API connection with OAuth2 client credentials."""

    model_config = SettingsConfigDict(env_prefix="adup_")

    client_id: str = il.InputField(label="Client ID", description="OAuth2 client ID")
    client_secret: str = il.SecretField(description="OAuth2 client secret")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client with OAuth2 client-credentials auth (async-native token exchange)."""
        auth = il.OAuth2ClientCredentialsAuth(constants.BASE_URL, self.client_id, self.client_secret)
        return il.AsyncRESTClient(constants.BASE_URL, auth=auth)
