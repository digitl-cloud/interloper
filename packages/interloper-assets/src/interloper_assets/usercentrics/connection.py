from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.usercentrics import constants


@il.connection(
    name="Usercentrics",
    icon="fluent:connector-24-filled",
    tags=["Analytics"],
)
class UsercentricsConnection(il.Connection):
    """Usercentrics API connection with API key auth."""

    model_config = SettingsConfigDict(env_prefix="usercentrics_")

    api_key: str = il.SecretField(description="Usercentrics API key")
    analytics_id: str = il.InputField(description="Usercentrics analytics ID")

    @cached_property
    def client(self) -> il.RESTClient:
        client = il.RESTClient(constants.BASE_URL)
        client.headers.update({"X-API-Key": self.api_key})
        return client
