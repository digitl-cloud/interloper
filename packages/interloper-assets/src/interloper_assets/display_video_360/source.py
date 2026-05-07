import logging

import interloper as il
import pandas as pd

from interloper_assets.display_video_360.connection import DisplayVideo360Connection
from interloper_assets.display_video_360.schemas import (
    AudienceComposition,
    Audiences,
    InsertionOrders,
    LineItems,
    Partners,
)
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    key="display_video_360",
    resources={
        "connection": DisplayVideo360Connection,
    },
    tags=["Advertising"],
    icon="icon:dv360",
)
class DisplayVideo360(il.Source):
    """Display & Video 360 advertising platform integration."""

    partner_id: str = il.InputField(description="DV360 partner ID")
    advertiser_id: str = il.InputField(description="DV360 advertiser ID (optional, used for audience queries)")

    @il.asset(schema=Partners, tags=["Entity"])
    def partners(self, connection: DisplayVideo360Connection) -> pd.DataFrame:
        """All DV360 partner accounts accessible by the service account."""
        return fake_data(Partners)

    @il.asset(schema=Audiences, tags=["Entity"])
    def audiences(self, connection: DisplayVideo360Connection) -> pd.DataFrame:
        """First-party and partner audiences for the configured partner."""
        return fake_data(Audiences)

    @il.asset(schema=AudienceComposition, tags=["Entity"])
    def audience_composition(self, connection: DisplayVideo360Connection) -> pd.DataFrame:
        """Audience composition details for the configured advertiser."""
        return fake_data(AudienceComposition)

    @il.asset(schema=InsertionOrders, tags=["Entity"])
    def insertion_orders(self, connection: DisplayVideo360Connection) -> pd.DataFrame:
        """Insertion orders for the configured advertiser."""
        return fake_data(InsertionOrders)

    @il.asset(schema=LineItems, tags=["Entity"])
    def line_items(self, connection: DisplayVideo360Connection) -> pd.DataFrame:
        """Line items for the configured advertiser."""
        return fake_data(LineItems)
