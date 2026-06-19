
import interloper as il

from interloper_assets.double_click_bid_manager.connection import DoubleClickBidManagerConnection


@il.source(
    resources={
        "connection": DoubleClickBidManagerConnection,
    },
    tags=["Advertising"],
    icon="icon:dv360",
)
class DoubleClickBidManager(il.Source):
    """DoubleClick Bid Manager (DBM/DV360) reporting integration."""

    partner_id: str = il.InputField(description="DV360 partner ID for report filtering")
    advertiser_id: str = il.InputField(description="DV360 advertiser ID for report filtering (optional)")
