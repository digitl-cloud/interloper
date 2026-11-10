import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class MediaStats(Schema):
    """Instagram media insights. One row per feed post or reel published in the last 180 days, and per story live at fetch time, per snapshot day, carrying the media's attributes and its cumulative lifetime metrics as of that day. Metrics that do not apply to a media type are null."""

    id: str | None = Field(default=None, description="Unique Instagram media identifier.")
    caption: str | None = Field(default=None, description="Post caption / text content.")
    media_type: str | None = Field(default=None, description="Media format: IMAGE, VIDEO, or CAROUSEL_ALBUM.")
    media_product_type: str | None = Field(default=None, description="Distribution surface: FEED, REELS, STORY, or AD.")
    timestamp: dt.datetime | None = Field(default=None, description="When the media was published.")
    permalink: str | None = Field(default=None, description="Public URL to the Instagram post.")
    boost_ads_list: str | None = Field(
        default=None,
        description="Ads with ACTIVE delivery status currently boosting the media (list of ad_id and ad_delivery_status), stored as JSON. Clears when the campaign ends; null when the media is not boosted.",
    )
    boost_eligibility_info: str | None = Field(
        default=None,
        description="Whether the media is eligible to be boosted as an ad, and the reason if not, stored as JSON.",
    )
    legacy_instagram_media_id: str | None = Field(
        default=None, description="Legacy Instagram media ID for Marketing API v21.0 and older."
    )
    media_url: str | None = Field(default=None, description="URL of the media asset (image or video).")
    shortcode: str | None = Field(default=None, description="Instagram shortcode for the media.")
    thumbnail_url: str | None = Field(default=None, description="The URL of the thumbnail of the media.")
    reach: int | None = Field(default=None, description="Number of unique accounts that saw the media at least once.")
    replies: int | None = Field(
        default=None,
        description="Number of replies received on the Story (text replies and quick reaction replies). STORY only - absent for FEED and REELS.",
    )
    saved: int | None = Field(default=None, description="Number of times the media was saved.")
    likes: int | None = Field(default=None, description="Total likes on the media.")
    comments: int | None = Field(default=None, description="Total comments on the media. FEED and REELS only.")
    shares: int | None = Field(default=None, description="Total shares of the media.")
    total_interactions: int | None = Field(
        default=None, description="Sum of all interactions (likes + comments + shares + saves)."
    )
    follows: int | None = Field(
        default=None,
        description="Number of follows attributed to the media. Availability depends on media type and API surface.",
    )
    profile_visits: int | None = Field(default=None, description="Number of profile visits attributed to the media.")
    profile_activity: int | None = Field(
        default=None, description="Number of actions people took on the profile after engaging with the media."
    )
    navigation: int | None = Field(
        default=None,
        description="Number of navigation actions taken on the story (taps forward and back, exits, swipes).",
    )
    ig_reels_video_view_total_time: int | None = Field(
        default=None, description="Total cumulative watch time for a Reel in milliseconds."
    )
    ig_reels_avg_watch_time: int | None = Field(
        default=None,
        description="Average watch time per play for a Reel in milliseconds. Reels only. Not deprecated as of v22.0.",
    )
    views: int | None = Field(
        default=None,
        description="Number of times the post was played or displayed. Unified metric replacing deprecated impressions, plays, and video_views (deprecated v22.0+, errors for media after July 1 2024). Works for FEED, STORY, and REELS. All data is organic - the API excludes ad interactions by design.",
    )
    reels_skip_rate: float | None = Field(
        default=None,
        description="Rate of skips for Reels playback. Fraction/percentage semantics depend on API response format.",
    )
    reposts: int | None = Field(
        default=None, description="Number of repost actions. Availability depends on media type and API surface."
    )
    facebook_views: int | None = Field(
        default=None, description="Views generated on Facebook surfaces for eligible cross-posted media."
    )
    crossposted_views: int | None = Field(
        default=None,
        description="Views from cross-posted surfaces. Interpret with care; can overlap with `views` depending on API semantics.",
    )
    total_views: int | None = Field(
        default=None,
        description="Total view metric returned by some endpoints/media types. Prefer as secondary fallback after `views` unless validated otherwise.",
    )
    total_likes: int | None = Field(
        default=None, description="Total likes metric variant. Use as fallback when `likes` is absent."
    )
    total_comments: int | None = Field(
        default=None, description="Total comments metric variant. Use as fallback when `comments` is absent."
    )
    link_clicks: int | None = Field(
        default=None,
        description="Number of clicks on links in the media (e.g. swipe-up links in Stories). Availability depends on media type and API surface.",
    )
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
