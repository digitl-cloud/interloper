import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Pinterest campaign-level performance report with engagement, cost, and conversion metrics."""

    date: dt.date = Field(description="Date of the record")
    # Identifiers
    ad_group_entity_status: int = Field(description="Status of the ad group entity")
    ad_group_id: int = Field(description="Identifier for the ad group")
    ad_group_status: int = Field(description="Status of the ad group")
    ad_status: int = Field(description="Status of the ad")
    advertiser: str = Field(description="Name of the advertiser")
    campaign_entity_status: str = Field(description="Status of the campaign entity")
    campaign_id: str = Field(description="Identifier for the campaign")
    campaign_managed_status: int = Field(description="Managed status of the campaign")
    campaign_name: str = Field(description="Name of the campaign")
    campaign_objective: str = Field(description="Objective of the campaign")
    campaign_status: str = Field(description="Status of the campaign")
    # Cost metrics
    cpc: float = Field(description="Cost per click")
    cpc_in_micro_dollar: float = Field(description="Cost per click in micro dollars")
    cpe: float = Field(description="Cost per engagement")
    cpm: float = Field(description="Cost per thousand impressions")
    cpm_in_micro_dollar: float = Field(description="Cost per thousand impressions in micro dollars")
    ctr: float = Field(description="Click-through rate")
    earned_ctr: float = Field(description="Earned click-through rate")
    ecpc_in_micro_dollar: float = Field(description="Effective cost per click in micro dollars")
    ecpm_in_micro_dollar: float = Field(description="Effective cost per thousand impressions in micro dollars")
    # Engagement metrics
    earned_engagements: int = Field(description="Total number of earned engagements")
    earned_impressions: int = Field(description="Total number of earned impressions")
    earned_outbound_clicks: int = Field(description="Total number of earned outbound clicks")
    earned_pin_clicks: int = Field(description="Total number of earned pin clicks")
    earned_saves: int = Field(description="Total number of earned saves")
    engagement_rate: float = Field(description="Engagement rate")
    engagement_rate_1: float = Field(description="Engagement rate 1")
    engagements: int = Field(description="Total number of engagements")
    frequency: float = Field(description="Frequency of the ad")
    gross_impressions: int = Field(description="Total number of gross impressions")
    gross_pin_clicks: int = Field(description="Total number of gross pin clicks")
    impressions: int = Field(description="Total number of impressions")
    # Click-through conversions
    click_through_conversions_add_to_cart: int = Field(
        description="Conversions from click-through actions leading to adding items to cart"
    )
    click_through_conversions_app_install: int = Field(
        description="Conversions from click-through actions leading to app installations"
    )
    click_through_conversions_category_view: int = Field(
        description="Conversions from click-through actions leading to category views"
    )
    click_through_conversions_checkout: int = Field(
        description="Conversions from click-through actions leading to checkouts"
    )
    click_through_conversions_custom: int = Field(
        description="Conversions from custom click-through actions"
    )
    click_through_conversions_lead: int = Field(
        description="Conversions from click-through actions leading to generating leads"
    )
    click_through_conversions_page_visit: int = Field(
        description="Conversions from click-through actions leading to page visits"
    )
    click_through_conversions_signup: int = Field(
        description="Conversions from click-through actions leading to signups"
    )
    click_through_conversions_unknown: int = Field(
        description="Conversions from unknown click-through actions"
    )
    click_through_conversions_watch_video: int = Field(
        description="Conversions from click-through actions leading to watching videos"
    )
    click_through_conversions_website_search: int = Field(
        description="Conversions from click-through actions leading to website searches"
    )
    # Engagement conversions
    engagement_conversions_add_to_cart: int = Field(
        description="Conversions from engagements leading to adding items to cart"
    )
    engagement_conversions_app_install: int = Field(
        description="Conversions from engagements leading to app installations"
    )
    engagement_conversions_category_view: int = Field(
        description="Conversions from engagements leading to category views"
    )
    engagement_conversions_checkout: int = Field(
        description="Conversions from engagements leading to checkouts"
    )
    engagement_conversions_custom: int = Field(
        description="Conversions from custom engagements"
    )
    engagement_conversions_lead: int = Field(
        description="Conversions from engagements leading to generating leads"
    )
    engagement_conversions_page_visit: int = Field(
        description="Conversions from engagements leading to page visits"
    )
    engagement_conversions_signup: int = Field(
        description="Conversions from engagements leading to signups"
    )
    engagement_conversions_unknown: int = Field(
        description="Conversions from unknown engagements"
    )
    engagement_conversions_watch_video: int = Field(
        description="Conversions from engagements leading to watching videos"
    )
    engagement_conversions_website_search: int = Field(
        description="Conversions from engagements leading to website searches"
    )
    # View-through conversions
    view_through_conversions_add_to_cart: int = Field(
        description="View-through conversions resulting in adding items to cart"
    )
    view_through_conversions_app_install: int = Field(
        description="View-through conversions resulting in app installations"
    )
    view_through_conversions_category_view: int = Field(
        description="View-through conversions resulting in category views"
    )
    view_through_conversions_checkout: int = Field(
        description="View-through conversions resulting in checkouts"
    )
    view_through_conversions_custom: int = Field(
        description="View-through conversions from custom actions"
    )
    view_through_conversions_lead: int = Field(
        description="View-through conversions resulting in generating leads"
    )
    view_through_conversions_page_visit: int = Field(
        description="View-through conversions resulting in page visits"
    )
    view_through_conversions_signup: int = Field(
        description="View-through conversions resulting in signups"
    )
    view_through_conversions_unknown: int = Field(
        description="View-through conversions from unknown actions"
    )
    view_through_conversions_watch_video: int = Field(
        description="View-through conversions resulting in watching videos"
    )
    view_through_conversions_website_search: int = Field(
        description="View-through conversions resulting in website searches"
    )
    # Totals
    conversions: int = Field(description="Total number of conversions")
    conversions_lead: int = Field(description="Total number of lead conversions")
    total_conversion_rate_lead: float = Field(description="Total conversion rate for generating leads")
    # Order & spend
    order_line_id: int = Field(description="Identifier for the order line")
    order_line_name: str = Field(description="Name of the order line")
    organic_pin_id: int = Field(description="Identifier for the organic pin")
    pin_clicks: int = Field(description="Total number of pin clicks")
    pin_promotion_status: int = Field(description="Status of pin promotion")
    product_group_id: int = Field(description="Identifier for the product group")
    reach: int = Field(description="Total reach")
    spend: float = Field(description="Total spend")
    spend_in_account_currency: float = Field(description="Total spend in account currency")
    # Paid metrics
    paid_ctr: float = Field(description="Paid click-through rate")
    paid_engagements: int = Field(description="Total number of paid engagements")
    paid_impressions: int = Field(description="Total number of paid impressions")
    paid_outbound_clicks: int = Field(description="Total number of paid outbound clicks")
    paid_pin_clicks: int = Field(description="Total number of paid pin clicks")
    paid_save_rate: float = Field(description="Paid save rate")
    paid_saves: int = Field(description="Total number of paid saves")
