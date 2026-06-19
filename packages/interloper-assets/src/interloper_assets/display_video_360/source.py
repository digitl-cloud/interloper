
import interloper as il

from interloper_assets.display_video_360.connection import DisplayVideo360Connection

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
