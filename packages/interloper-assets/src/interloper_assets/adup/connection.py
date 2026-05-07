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

    client_id: str = il.InputField(description="OAuth2 client ID")
    client_secret: str = il.SecretField(description="OAuth2 client secret")

    @cached_property
    def client(self) -> il.RESTClient:
        auth = il.OAuth2ClientCredentialsAuth(constants.BASE_URL, self.client_id, self.client_secret)
        return il.RESTClient(constants.BASE_URL, auth)
