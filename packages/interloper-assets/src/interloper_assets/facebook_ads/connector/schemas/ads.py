import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Facebook Ads entity snapshot. One row per ad (not time-series). Captures ad configuration, status, and creative details. Join to the ads insights table on id=ad_id. Join to facebook_insights.posts on creative_effective_object_story_id=post_id to resolve paid vs organic split. bid_amount is an integer in the currency's minor units; creation and update times are UTC instants; ad_schedule_start_time and ad_schedule_end_time keep the vendor's ISO 8601 string with the account's offset."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Unique ad identifier. Join key to ads insights table (ad_id).")
    account_id: str | None = Field(default=None, description="Ad account ID.")
    ad_active_time: float | None = Field(
        default=None, description="Time in seconds the ad has been in an active/running state."
    )
    ad_review_feedback_global: str | None = Field(
        default=None,
        description="Global ad review rejection reason (flattened from ad_review_feedback dict). NULL if no review feedback.",
    )
    ad_review_feedback_placement_specific: str | None = Field(
        default=None,
        description="Placement-specific ad review rejection reason (flattened from ad_review_feedback dict). NULL if no placement feedback.",
    )
    ad_schedule_end_time: str | None = Field(
        default=None,
        description=(
            "Scheduled end of the ad as the vendor sends it, ISO 8601 with the account's UTC offset; NULL when the ad "
            "follows its ad set's schedule. A string because a timestamp would store it in UTC and lose the offset; its date part is the local day."
        ),
    )
    ad_schedule_start_time: str | None = Field(
        default=None,
        description=(
            "Scheduled start of the ad as the vendor sends it, ISO 8601 with the account's UTC offset; NULL when the "
            "ad follows its ad set's schedule. A string because a timestamp would store it in UTC and lose the offset; its date part is the local day."
        ),
    )
    adlabels: str | None = Field(default=None, description="JSON array of the ad labels attached to the ad.")
    adset_id: str | None = Field(default=None, description="Ad set this ad belongs to.")
    bid_amount: int | None = Field(
        default=None,
        description="Manual bid for this ad, in the currency's minor units (cents for EUR and USD). NULL when using automatic bidding.",
    )
    campaign_id: str | None = Field(default=None, description="Campaign this ad belongs to.")
    configured_status: str | None = Field(
        default=None, description="Status set by the advertiser: ACTIVE, PAUSED, DELETED, ARCHIVED."
    )
    conversion_domain: str | None = Field(
        default=None, description="Domain where conversions are tracked (e.g. example.com). NULL if not set."
    )
    created_time: dt.datetime | None = Field(default=None, description="When the ad was created, in UTC.")
    creative_id: str | None = Field(
        default=None, description="ID of the ad creative (flattened from creative sub-object)."
    )
    creative_name: str | None = Field(
        default=None, description="Name of the ad creative (flattened from creative sub-object)."
    )
    creative_body: str | None = Field(
        default=None, description="Body of the ad creative (flattened from creative sub-object)."
    )
    creative_title: str | None = Field(
        default=None, description="Title of the ad creative (flattened from creative sub-object)."
    )
    creative_thumbnail_url: str | None = Field(
        default=None, description="Thumbnail image URL for the ad creative (flattened from creative sub-object)."
    )
    creative_effective_object_story_id: str | None = Field(
        default=None,
        description="Page post ID backing this ad creative, in {page_id}_{post_id} format (flattened from creative sub-object). Populated for boosted posts and promoted page posts. NULL for dark/canvas ads with no underlying post. Join key to facebook_insights.posts (post_id) to compute organic = post_total - paid.",
    )
    creative_effective_instagram_story_id: str | None = Field(
        default=None,
        description="Instagram Story ID backing this ad creative. Populated only for Instagram Story-type ads. NULL for feed posts, Reels, and Facebook-only ads. Use creative_instagram_permalink_url as the join key for feed/Reel boosts.",
    )
    creative_instagram_permalink_url: str | None = Field(
        default=None,
        description="Permalink URL of the Instagram post being promoted (e.g. https://www.instagram.com/p/ABC123/). Populated for boosted Instagram feed posts and Reels. Join key to instagram_insights.organic_media (permalink) to identify boosted posts. More reliable than effective_instagram_story_id for non-Story placements.",
    )
    effective_status: str | None = Field(
        default=None,
        description="Effective delivery status accounting for parent campaign/adset: ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED.",
    )
    issues_info: str | None = Field(
        default=None,
        description="JSON array of delivery issue objects ({error_code, error_message, error_summary, error_type, level}). NULL if no issues.",
    )
    last_updated_by_app_id: str | None = Field(default=None, description="App ID that last modified this ad.")
    name: str | None = Field(default=None, description="Ad name as set in Ads Manager.")
    preview_shareable_link: str | None = Field(default=None, description="Shareable link to preview this ad.")
    recommendations: str | None = Field(
        default=None, description="JSON array of Facebook optimization recommendations for this ad. NULL if none."
    )
    source_ad_id: str | None = Field(
        default=None, description="If this ad was copied, the ID of the original source ad. NULL otherwise."
    )
    status: str | None = Field(
        default=None, description="Ad status as set by the advertiser (mirrors configured_status in most cases)."
    )
    tracking_specs: str | None = Field(
        default=None,
        description="JSON array of tracking specification objects defining what events to track for this ad.",
    )
    updated_time: dt.datetime | None = Field(default=None, description="When the ad was last changed, in UTC.")
