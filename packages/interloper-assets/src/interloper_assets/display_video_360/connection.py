import asyncio
import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.display_video_360 import constants


@il.connection(
    name="Display & Video 360",
    icon="icon:dv360",
    tags=["Advertising"],
)
class DisplayVideo360Connection(il.Connection):
    """Display & Video 360 API connection using Google service account credentials."""

    model_config = SettingsConfigDict(env_prefix="display_video_360_")
    key = "display_video_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def dv_client(self) -> Any:
        """Build and return the Display & Video API client (entities)."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.DV_SCOPES,
        )

        return build(
            constants.DV_API_SERVICE,
            constants.DV_API_VERSION,
            credentials=credentials,
        )

    @cached_property
    def dbm_client(self) -> Any:
        """Build and return the Bid Manager API client (DV360 reporting)."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.DBM_SCOPES,
        )

        return build(
            constants.DBM_API_SERVICE,
            constants.DBM_API_VERSION,
            credentials=credentials,
        )

    def _list_partners(self) -> list[dict[str, Any]]:
        """Page through the partners accessible to the service account."""
        partners: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self.dv_client.partners().list(pageToken=page_token).execute()
            partners.extend(response.get("partners") or [])
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return partners

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the DV360 partners the service account has access to."""
        partners = await asyncio.to_thread(self._list_partners)
        return [
            {
                "partner_id": str(p["partnerId"]),
                "name": f"{p.get('displayName', p['partnerId'])} ({p['partnerId']})",
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
