import asyncio
import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.api import DisplayVideo360Api


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
    def credentials(self) -> Any:
        """Load the service account credentials, shared by every client built from them.

        Returns:
            The ``google.oauth2`` service account credentials, scoped to both the
            Display & Video API and the Bid Manager API.
        """
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=[*constants.DV_SCOPES, *constants.DBM_SCOPES],
        )

    def dv_client(self) -> Any:
        """Build a fresh Display & Video API service client (entities).

        Not cached: the client's ``httplib2`` transport is not thread-safe and
        the assets run concurrently in threads, so each caller builds its own.

        Returns:
            The ``displayvideo`` discovery client over the shared credentials.
        """
        from googleapiclient.discovery import build

        return build(constants.DV_API_SERVICE, constants.DV_API_VERSION, credentials=self.credentials)

    def dbm_client(self) -> Any:
        """Build a fresh Bid Manager API service client (DV360 reporting).

        Not cached, for the same thread-safety reason as ``dv_client``.

        Returns:
            The ``doubleclickbidmanager`` discovery client over the shared credentials.
        """
        from googleapiclient.discovery import build

        return build(constants.DBM_API_SERVICE, constants.DBM_API_VERSION, credentials=self.credentials)

    @cached_property
    def api(self) -> DisplayVideo360Api:
        """The Display & Video 360 API engine every caller shares, built on ``dv_client`` and ``dbm_client``.

        Returns:
            The engine, cached per connection instance; each call to it builds a service of its own.
        """
        return DisplayVideo360Api(self.dv_client, self.dbm_client)

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the DV360 partners the service account has access to.

        Returns:
            The options for the field's dropdown.

        """
        partners = await asyncio.to_thread(self.api.rows, "partners")
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
