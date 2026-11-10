import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Facebook Ads entity snapshot. One row per ad (not time-series). Captures ad configuration, status, bidding and targeting. Join to the ads insights table on id=ad_id. bid_amount is an integer in the currency's minor units; creation and update times are UTC instants. Nested objects (creative, targeting, bid_info...) are kept whole as JSON."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Unique ad identifier. Join key to ads insights table (ad_id).")
    account_id: str | None = Field(default=None, description="Ad account ID.")
    adlabels: str | None = Field(default=None, description="JSON array of the ad labels attached to the ad.")
    adset_id: str | None = Field(default=None, description="Ad set this ad belongs to.")
    bid_amount: int | None = Field(
        default=None,
        description="Manual bid for this ad, in the currency's minor units (cents for EUR and USD). NULL when using automatic bidding.",
    )
    bid_info: str | None = Field(default=None, description="JSON map of the bid amounts set per optimization event.")
    bid_type: str | None = Field(default=None, description="Bid strategy of the ad: CPC, CPM, MULTI_PREMIUM, ...")
    campaign_id: str | None = Field(default=None, description="Campaign this ad belongs to.")
    conversion_specs: str | None = Field(
        default=None, description="JSON array of the conversion specifications the ad is optimized for."
    )
    created_time: dt.datetime | None = Field(default=None, description="When the ad was created, in UTC.")
    creative: str | None = Field(default=None, description="JSON of the ad's creative, its id only.")
    effective_status: str | None = Field(
        default=None,
        description="Effective delivery status accounting for parent campaign/adset: ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED.",
    )
    last_updated_by_app_id: str | None = Field(default=None, description="App ID that last modified this ad.")
    name: str | None = Field(default=None, description="Ad name as set in Ads Manager.")
    recommendations: str | None = Field(
        default=None, description="JSON array of Facebook optimization recommendations for this ad. NULL if none."
    )
    source_ad_id: str | None = Field(
        default=None, description="If this ad was copied, the ID of the original source ad. NULL otherwise."
    )
    status: str | None = Field(default=None, description="Ad status as set by the advertiser.")
    targeting: str | None = Field(default=None, description="JSON of the targeting the ad delivers to.")
    tracking_specs: str | None = Field(
        default=None,
        description="JSON array of tracking specification objects defining what events to track for this ad.",
    )
    updated_time: dt.datetime | None = Field(default=None, description="When the ad was last changed, in UTC.")
