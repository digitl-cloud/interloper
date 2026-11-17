from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.thetradedesk.api import TheTradeDeskApi
from interloper_assets.thetradedesk.constants import BASE_URL


@il.connection(
    name="The Trade Desk",
    icon="icon:thetradedesk",
    tags=["Advertising"],
)
class TheTradeDeskConnection(il.Connection):
    """The Trade Desk API connection with custom TTD-Auth header authentication."""

    model_config = SettingsConfigDict(env_prefix="thetradedesk_")

    api_key: str = il.SecretField(label="API Key", description="The Trade Desk API key")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Trade Desk API client every caller shares.

        Returns:
            The client authenticated by the ``TTD-Auth`` header, cached per connection instance.
        """
        return il.AsyncRESTClient(
            BASE_URL,
            headers={"TTD-Auth": self.api_key},
            timeout=60,
        )

    @cached_property
    def api(self) -> TheTradeDeskApi:
        """The Trade Desk API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return TheTradeDeskApi(self.client)

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the partners accessible to the API key.

        Returns:
            The options for the field's dropdown.

        """
        partners = await self.api.query("/partner/query")
        return [
            {"partner_id": partner["PartnerId"], "name": partner.get("PartnerName", partner["PartnerId"])}
            for partner in partners
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``partners`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.partners()
        return True
