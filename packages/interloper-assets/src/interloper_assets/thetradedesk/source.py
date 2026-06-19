
import interloper as il

from interloper_assets.thetradedesk.connection import TheTradeDeskConnection

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": TheTradeDeskConnection},
    tags=["Advertising"],
    icon="icon:thetradedesk",
)
class TheTradeDesk(il.Source):
    """The Trade Desk programmatic advertising platform integration."""

    report_template_id: str = il.InputField(
        description="Report template ID for the MyReports schedule",
    )
