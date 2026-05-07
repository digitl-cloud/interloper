from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Adservice",
    icon="carbon:analytics",
    tags=["Advertising"],
)
class AdserviceConnection(il.Connection):
    """Adservice API connection with API key auth."""

    model_config = SettingsConfigDict(env_prefix="adservice_")

    api_key: str = il.SecretField(description="Adservice API key")

    @cached_property
    def client(self) -> il.RESTClient:
        base_url = "https://api.adservice.com/v2/client"
        auth = httpx.BasicAuth(username="api", password=self.api_key)
        return il.RESTClient(base_url, auth)
