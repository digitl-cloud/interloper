import json
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.display_video_360 import schemas
from interloper_assets.display_video_360.connection import DisplayVideo360Connection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]


# -- HELPERS -------------------------------------------------------------------
def _list_audiences(service: Any, scope: dict[str, str]) -> list[_Record]:
    """Page through the first-party/partner audiences of a partner or advertiser."""
    audiences: list[_Record] = []
    page_token: str | None = None
    while True:
        response = service.firstPartyAndPartnerAudiences().list(**scope, pageToken=page_token).execute()
        audiences.extend(response.get("firstPartyAndPartnerAudiences") or [])
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return audiences


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="display_video_360",
    resources={
        "connection": DisplayVideo360Connection,
    },
    tags=["Advertising"],
    icon="icon:dv360",
    # Partner objects nest config two levels deep (dataAccessConfig.sdfConfig.*);
    # digit splitting maps dv360ToCm... -> dv_360_to_cm_... onto the schema fields.
    normalizer=DataFrameNormalizer(snake_case_digits=True, flatten_max_level=2),
)
class DisplayVideo360(il.Source):
    """Display & Video 360 advertising platform integration."""

    partner_id: str = il.FetchField(
        title="Partner ID",
        description="DV360 partner",
        provider="connection.partners",
        label_key="name",
        value_key="partner_id",
        discriminator=True,
    )
    advertiser_id: str = il.InputField(
        default="",
        title="Advertiser ID",
        description="DV360 advertiser ID — set to scope audience assets by advertiser instead of partner",
    )
    audience_id: str = il.InputField(
        default="",
        title="Audience ID",
        description="Audience ID (only required for the custom_audiences asset)",
    )

    @property
    def _audience_scope(self) -> dict[str, str]:
        """Audience calls take a partner XOR advertiser scope; advertiser wins when set."""
        if self.advertiser_id:
            return {"advertiserId": self.advertiser_id}
        return {"partnerId": self.partner_id}

    @il.asset(schema=schemas.Partners, tags=["Entity"])
    def partners(self, connection: DisplayVideo360Connection) -> list[_Record]:
        """DV360 partners and their configuration."""
        items = connection._list_partners()
        for item in items:
            exchange_config = item.get("exchangeConfig")
            # Enabled exchanges are a nested list; JSON-encode onto the string column.
            if isinstance(exchange_config, dict) and isinstance(exchange_config.get("enabledExchanges"), list):
                exchange_config["enabledExchanges"] = json.dumps(exchange_config["enabledExchanges"])
        return items

    @il.asset(schema=schemas.Audiences, tags=["Entity"])
    def audiences(self, connection: DisplayVideo360Connection) -> list[_Record]:
        """First-party and partner audiences of the configured partner or advertiser."""
        return _list_audiences(connection.client, self._audience_scope)

    @il.asset(schema=schemas.CustomAudiences, tags=["Entity"])
    def custom_audiences(self, connection: DisplayVideo360Connection) -> list[_Record]:
        """Detail of the configured audience, including per-surface audience sizes."""
        if not self.audience_id:
            raise ValueError("The custom_audiences asset requires the source's audience_id to be set")
        audience = (
            connection.client.firstPartyAndPartnerAudiences()
            .get(**self._audience_scope, firstPartyAndPartnerAudienceId=self.audience_id)
            .execute()
        )
        return [audience]
