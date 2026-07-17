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

    api_key: str = il.SecretField(label="API Key", description="Usercentrics API key")
    analytics_id: str = il.InputField(label="Analytics ID", description="Usercentrics analytics ID", discriminator=True)

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        # Static API-key header (no token refresh) drops straight into the async client.
        return il.AsyncRESTClient(constants.BASE_URL, headers={"X-API-Key": self.api_key})
