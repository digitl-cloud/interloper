from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.impact.constants import BASE_URL


@il.connection(
    name="Impact",
    icon="fluent:connector-24-filled",
    tags=["Affiliate"],
)
class ImpactConnection(il.Connection):
    """Impact.com API connection with HTTP Basic Auth (account SID + auth token)."""

    model_config = SettingsConfigDict(env_prefix="impact_")

    account_sid: str = il.InputField(description="Impact account SID")
    auth_token: str = il.SecretField(description="Impact auth token")

    @cached_property
    def client(self) -> il.RESTClient:
        auth = httpx.BasicAuth(username=self.account_sid, password=self.auth_token)
        return il.RESTClient(
            BASE_URL,
            auth,
            headers={"Accept": "application/json"},
        )
