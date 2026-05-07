from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.thetradedesk.constants import BASE_URL


@il.connection(
    name="The Trade Desk",
    icon="icon:thetradedesk",
    tags=["Advertising"],
)
class TheTradeDeskConnection(il.Connection):
    """The Trade Desk API connection with custom TTD-Auth header authentication."""

    model_config = SettingsConfigDict(env_prefix="thetradedesk_")

    api_key: str = il.SecretField(description="The Trade Desk API key")
    partner_id: str = il.InputField(description="The Trade Desk partner ID")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            BASE_URL,
            headers={"TTD-Auth": self.api_key},
        )
