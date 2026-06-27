from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.awin.constants import BASE_URL


@il.connection(
    name="Awin",
    icon="icon:awin",
    tags=["Affiliate"],
)
class AwinConnection(il.Connection):
    """Awin affiliate network API connection with bearer token auth."""

    model_config = SettingsConfigDict(env_prefix="awin_")

    access_token: str = il.SecretField(description="Awin API access token")
    publisher_id: str = il.InputField(description="Awin publisher or advertiser ID")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        return il.AsyncRESTClient(BASE_URL, auth=il.HTTPBearerAuth(self.access_token))
