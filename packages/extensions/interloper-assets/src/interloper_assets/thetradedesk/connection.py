from functools import cached_property

import httpx2
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.thetradedesk.api import TheTradeDeskApi
from interloper_assets.thetradedesk.constants import BASE_URL, DOMAIN


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

        A report's delivery may live on another host, such as a pre-signed
        storage URL, so the key is withheld from every request outside The
        Trade Desk's domain.

        Returns:
            The client authenticated by the ``TTD-Auth`` header, cached per connection instance.
        """
        return il.AsyncRESTClient(
            BASE_URL,
            headers={"TTD-Auth": self.api_key},
            timeout=60,
            event_hooks={"request": [self._withhold_key]},
            retry=il.HttpRetryPolicy(),
        )

    @staticmethod
    async def _withhold_key(request: httpx2.Request) -> None:
        """Remove the API key from a request outside The Trade Desk's domain.

        httpx2 copies every header but ``Authorization`` onto a redirect and
        runs an auth flow on the first hop only, so the key is dropped by a
        request hook, which runs before every hop.

        Args:
            request: The request about to be sent, a redirect's included.
        """
        host = request.url.host
        if host != DOMAIN and not host.endswith(f".{DOMAIN}"):
            request.headers.pop("TTD-Auth", None)

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
            {"partner_id": partner["PartnerId"], "name": partner.get("PartnerName") or partner["PartnerId"]}
            for partner in partners
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``partners`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.partners()
        return True
