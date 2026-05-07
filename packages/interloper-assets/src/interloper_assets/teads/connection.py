from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.teads import constants


@il.connection(
    name="Teads",
    icon="icon:teads",
    tags=["Advertising"],
)
class TeadsConnection(il.Connection):
    """Teads API connection with API key auth."""

    model_config = SettingsConfigDict(env_prefix="teads_")

    api_key: str = il.SecretField(description="Teads API key")

    @cached_property
    def client(self) -> il.RESTClient:
        client = il.RESTClient(constants.BASE_URL)
        client.headers.update(
            {
                "Authorization": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        return client
