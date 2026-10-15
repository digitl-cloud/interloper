import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStats(Schema):
    """Pinterest ad performance per ad and day: delivery, engagement, spend and conversion totals."""

    date: dt.date | None = Field(default=None, description="The report day.")
    ad_account_id: str | None = Field(default=None, description="The ad account ID.")
    campaign_name: str | None = Field(default=None, description="The campaign name.")
    campaign_status: str | None = Field(default=None, description="The campaign status.")
    campaign_id: str | None = Field(default=None, description="The campaign ID.")
    campaign_entity_status: str | None = Field(default=None, description="The campaign entity status.")
    campaign_managed_status: str | None = Field(default=None, description="The campaign managed status.")
    campaign_objective_type: str | None = Field(default=None, description="The campaign objective type.")
    ad_group_id: str | None = Field(default=None, description="The ad group ID.")
    ad_group_name: str | None = Field(default=None, description="The ad group name.")
    ad_group_status: str | None = Field(default=None, description="The ad group status.")
    ad_group_entity_status: str | None = Field(default=None, description="The ad group entity status.")
    ad_status: str | None = Field(default=None, description="The ad status.")
    ad_name: str | None = Field(default=None, description="The ad name.")
    pin_id: str | None = Field(default=None, description="The ID of the organic Pin behind the ad.")
    pin_promotion_status: str | None = Field(default=None, description="The pin promotion (ad) status.")
    clickthrough_1: int | None = Field(default=None, description="Paid Pin clicks.")
    clickthrough_1_gross: int | None = Field(
        default=None, description="Gross paid Pin clicks (before invalid-traffic filtering)."
    )
    clickthrough_2: int | None = Field(default=None, description="Earned Pin clicks.")
    cpc_in_micro_dollar: float | None = Field(default=None, description="Cost per click in micro dollars.")
    cpm_in_dollar: float | None = Field(default=None, description="Cost per mille in dollars.")
    cpm_in_micro_dollar: float | None = Field(default=None, description="Cost per mille in micro dollars.")
    ctr: float | None = Field(default=None, description="Click-through rate.")
    ctr_2: float | None = Field(default=None, description="Earned click-through rate.")
    ecpc_in_dollar: float | None = Field(default=None, description="Effective cost per click in dollars.")
    ecpc_in_micro_dollar: float | None = Field(default=None, description="Effective cost per click in micro dollars.")
    ecpe_in_dollar: float | None = Field(default=None, description="Effective cost per engagement in dollars.")
    ecpm_in_micro_dollar: float | None = Field(default=None, description="Effective cost per mille in micro dollars.")
    ectr: float | None = Field(default=None, description="Effective click-through rate.")
    eengagement_rate: float | None = Field(default=None, description="Effective engagement rate.")
    engagement_1: int | None = Field(default=None, description="Paid engagements.")
    engagement_2: int | None = Field(default=None, description="Earned engagements.")
    engagement_rate: float | None = Field(default=None, description="Overall engagement rate.")
    impression_1_gross: int | None = Field(
        default=None, description="Gross paid impressions (before invalid-traffic filtering)."
    )
    impression_2: int | None = Field(default=None, description="Earned impressions.")
    order_line_id: str | None = Field(default=None, description="The order line ID.")
    order_line_name: str | None = Field(default=None, description="The order line name.")
    outbound_click_1: int | None = Field(default=None, description="Paid outbound clicks.")
    outbound_click_2: int | None = Field(default=None, description="Earned outbound clicks.")
    paid_impression: int | None = Field(default=None, description="Paid impressions.")
    product_group_id: str | None = Field(default=None, description="The product group ID.")
    repin_1: int | None = Field(default=None, description="Paid saves (repins).")
    repin_2: int | None = Field(default=None, description="Earned saves (repins).")
    repin_rate: float | None = Field(default=None, description="Overall repin rate.")
    spend_in_dollar: float | None = Field(default=None, description="Total spend in dollars.")
    spend_in_micro_dollar: float | None = Field(default=None, description="Total spend in micro dollars.")
    total_click_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin click."
    )
    total_click_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin click."
    )
    total_click_checkout: int | None = Field(
        default=None, description="Checkout conversions attributed to a Pin click."
    )
    total_click_custom: int | None = Field(default=None, description="Custom conversions attributed to a Pin click.")
    total_click_lead: int | None = Field(default=None, description="Lead conversions attributed to a Pin click.")
    total_click_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin click."
    )
    total_click_search: int | None = Field(default=None, description="Search conversions attributed to a Pin click.")
    total_click_signup: int | None = Field(default=None, description="Signup conversions attributed to a Pin click.")
    total_click_unknown: int | None = Field(default=None, description="Unknown conversions attributed to a Pin click.")
    total_click_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin click."
    )
    total_click_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin click."
    )
    total_clickthrough: int | None = Field(default=None, description="Total Pin clicks (paid and earned).")
    total_conversions: int | None = Field(default=None, description="Total conversions.")
    total_engagement: int | None = Field(default=None, description="Total engagements (paid and earned).")
    total_engagement_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin engagement."
    )
    total_engagement_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin engagement."
    )
    total_engagement_checkout: int | None = Field(
        default=None, description="Checkout conversions attributed to a Pin engagement."
    )
    total_engagement_custom: int | None = Field(
        default=None, description="Custom conversions attributed to a Pin engagement."
    )
    total_engagement_lead: int | None = Field(
        default=None, description="Lead conversions attributed to a Pin engagement."
    )
    total_engagement_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin engagement."
    )
    total_engagement_search: int | None = Field(
        default=None, description="Search conversions attributed to a Pin engagement."
    )
    total_engagement_signup: int | None = Field(
        default=None, description="Signup conversions attributed to a Pin engagement."
    )
    total_engagement_unknown: int | None = Field(
        default=None, description="Unknown conversions attributed to a Pin engagement."
    )
    total_engagement_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin engagement."
    )
    total_engagement_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin engagement."
    )
    total_impression_frequency: float | None = Field(
        default=None, description="Average number of impressions per reached user."
    )
    total_impression_user: int | None = Field(default=None, description="Total unique users reached.")
    total_impression: int | None = Field(default=None, description="Total impressions (paid and earned).")
    total_lead_conversion_rate: float | None = Field(default=None, description="Lead conversion rate.")
    total_lead: int | None = Field(default=None, description="Total lead conversions.")
    total_view_add_to_cart: int | None = Field(
        default=None, description="Add-to-cart conversions attributed to a Pin view."
    )
    total_view_app_install: int | None = Field(
        default=None, description="App-install conversions attributed to a Pin view."
    )
    total_view_checkout: int | None = Field(default=None, description="Checkout conversions attributed to a Pin view.")
    total_view_custom: int | None = Field(default=None, description="Custom conversions attributed to a Pin view.")
    total_view_lead: int | None = Field(default=None, description="Lead conversions attributed to a Pin view.")
    total_view_page_visit: int | None = Field(
        default=None, description="Page-visit conversions attributed to a Pin view."
    )
    total_view_search: int | None = Field(default=None, description="Search conversions attributed to a Pin view.")
    total_view_signup: int | None = Field(default=None, description="Signup conversions attributed to a Pin view.")
    total_view_unknown: int | None = Field(default=None, description="Unknown conversions attributed to a Pin view.")
    total_view_view_category: int | None = Field(
        default=None, description="View-category conversions attributed to a Pin view."
    )
    total_view_watch_video: int | None = Field(
        default=None, description="Watch-video conversions attributed to a Pin view."
    )
    ad_id: str | None = Field(default=None, description="The ad ID.")
