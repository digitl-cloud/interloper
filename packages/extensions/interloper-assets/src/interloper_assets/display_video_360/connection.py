import json
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.api import DisplayVideo360Api
from interloper_assets.google.auth import ServiceAccountAuth


@il.connection(
    name="Display & Video 360",
    icon="icon:dv360",
    tags=["Advertising"],
    maturity="beta",
)
class DisplayVideo360Connection(il.Connection):
    """Display & Video 360 API connection using Google service account credentials."""

    model_config = SettingsConfigDict(env_prefix="display_video_360_")
    key = "display_video_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Display & Video API client every caller shares, Bid Manager requests included.

        Returns:
            The client authenticated as the service account for both APIs, cached per connection instance.
        """
        auth = ServiceAccountAuth(json.loads(self.service_account_key), constants.SCOPES)
        return il.AsyncRESTClient(constants.DISPLAY_VIDEO_URL, auth=auth, retry=il.HttpRetryPolicy())

    @cached_property
    def api(self) -> DisplayVideo360Api:
        """The Display & Video 360 API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return DisplayVideo360Api(self.client)

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the DV360 partners the service account has access to.

        Returns:
            The options for the field's dropdown.

        """
        partners = await self.api.rows("/partners")
        return [
            {
                "partner_id": str(p["partnerId"]),
                "name": str(p.get("displayName", p["partnerId"])),
            }
            for p in partners
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``partners`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.partners()
        return True
