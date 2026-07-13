
import interloper as il

from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection


@il.source(
    key="campaign_manager_360",
    resources={
        "connection": CampaignManager360Connection,
    },
    tags=["Advertising"],
    icon="icon:cm360",
)
class CampaignManager360(il.Source):
    """Campaign Manager 360 advertising platform integration."""

    profile_id: str = il.InputField(description="CM360 user profile ID")
    account_id: str = il.InputField(description="CM360 account ID", discriminator=True)
