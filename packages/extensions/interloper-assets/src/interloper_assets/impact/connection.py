from functools import cached_property

import httpx2
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.impact.api import ImpactApi
from interloper_assets.impact.constants import BASE_URL


@il.connection(
    name="Impact",
    icon="fluent:connector-24-filled",
    tags=["Affiliate"],
)
class ImpactConnection(il.Connection):
    """Impact.com API connection with HTTP Basic Auth (account SID + auth token)."""

    model_config = SettingsConfigDict(env_prefix="impact_")

    account_sid: str = il.InputField(label="Account SID", description="Impact account SID", discriminator=True)
    auth_token: str = il.SecretField(description="Impact auth token")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Impact API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
        return il.AsyncRESTClient(
            f"{BASE_URL}/Advertisers/{self.account_sid}",
            auth=httpx2.BasicAuth(username=self.account_sid, password=self.auth_token),
            headers={"Accept": "application/json"},
            retry=il.HttpRetryPolicy(),
        )

    @cached_property
    def api(self) -> ImpactApi:
        """The Impact Advertiser API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return ImpactApi(self.client)

    @il.fetch_field_provider
    async def programs(self) -> list[dict[str, str]]:
        """Fetch Impact programs (campaigns) accessible by the connection.

        Returns:
            The options for the field's dropdown.

        """
        programs = await self.api.rows("/Campaigns", "Campaigns")
        return [{"Id": program["Id"], "Name": program.get("Name") or program["Id"]} for program in programs]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``programs`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.programs()
        return True
