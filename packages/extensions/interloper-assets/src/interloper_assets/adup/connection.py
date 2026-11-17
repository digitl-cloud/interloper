from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.adup import constants
from interloper_assets.adup.api import AdupApi


@il.connection(
    name="Adup",
    icon="icon:adup",
    tags=["Advertising"],
    maturity="beta",
)
class AdupConnection(il.Connection):
    """Adup API connection with OAuth2 client credentials."""

    model_config = SettingsConfigDict(env_prefix="adup_")

    client_id: str = il.InputField(label="Client ID", description="OAuth2 client ID")
    client_secret: str = il.SecretField(description="OAuth2 client secret")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The AdUp API client every caller shares, with OAuth2 client-credentials auth (async-native token exchange).

        Returns:
            The authenticated client, cached per connection instance.
        """
        auth = il.OAuth2ClientCredentialsAuth(constants.BASE_URL, self.client_id, self.client_secret)
        return il.AsyncRESTClient(constants.BASE_URL, auth=auth)

    @cached_property
    def api(self) -> AdupApi:
        """The AdUp API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return AdupApi(self.client)
