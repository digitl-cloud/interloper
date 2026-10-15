import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class InstagramStats(Schema):
    """Instagram channel performance metrics per day (Brandwatch/Falcon.io Measure)."""

    date: dt.date | None = Field(default=None, description="The date the metrics correspond to.")
    channel_id: str | None = Field(default=None, description="Unique identifier for the Facebook Page.")
    fans: int | None = Field(default=None, description="Total number of account followers (Fans).")
    followers: int | None = Field(default=None, description="Total number of people following the account.")
    net_fans: int | None = Field(default=None, description="Net change in total fans (New - Lost).")
    net_followers: int | None = Field(default=None, description="Net change in total followers.")
    impressions: int | None = Field(default=None, description="Total number of times content was seen (non-unique).")
    reach: int | None = Field(default=None, description="Total number of unique users reached.")
    reach_organic: int | None = Field(default=None, description="Reach from unpaid sources.")
    reach_paid: int | None = Field(default=None, description="Reach from paid/promoted sources.")
    reach_follower: int | None = Field(default=None, description="Reach among current followers.")
    reach_non_follower: int | None = Field(default=None, description="Reach among non-followers.")
    frequency: float | None = Field(
        default=None, description="The average number of times a unique user saw the content (Impressions / Reach)."
    )
    reach_post: int | None = Field(default=None, description="Reach of feed posts.")
    reach_reel: int | None = Field(default=None, description="Reach of Reels.")
    reach_story: int | None = Field(default=None, description="Reach of Stories.")
    reach_igtv: int | None = Field(default=None, description="Reach of IGTV/Long-form videos.")
    reach_carousel: int | None = Field(default=None, description="Reach of carousel posts.")
    reach_ad: int | None = Field(default=None, description="Reach of paid advertisements.")
    reach_profile_pic: int | None = Field(default=None, description="Reach of profile picture views.")
    engagements: int | None = Field(
        default=None, description="Total engagement actions (Likes, Comments, Saves, Shares, and other interactions)."
    )
    engaged_users: int | None = Field(default=None, description="Total number of unique users who engaged.")
    interactions: int | None = Field(default=None, description="A general count of interactions.")
    engagement_rate: float | None = Field(default=None, description="Overall engagement rate.")
    engagement_rate_reach: float | None = Field(
        default=None, description="Overall engagement rate calculated based on reach."
    )
    engaged_users_rate: float | None = Field(
        default=None, description="Rate of unique engaged users (Engaged Users / Reach)."
    )
    interaction_rate: float | None = Field(default=None, description="Overall interaction rate.")
    interaction_rate_reach: float | None = Field(
        default=None, description="Interaction rate calculated based on reach."
    )
    engagement_rate_reach_post: float | None = Field(
        default=None, description="Engagement rate of posts based on reach."
    )
    engagement_rate_reach_reel: float | None = Field(
        default=None, description="Engagement rate of Reels based on reach."
    )
    engagement_rate_reach_story: float | None = Field(
        default=None, description="Engagement rate of Stories based on reach."
    )
    engagement_rate_reach_igtv: float | None = Field(
        default=None, description="Engagement rate of IGTV based on reach."
    )
    engagement_rate_reach_ad: float | None = Field(default=None, description="Engagement rate of Ads based on reach.")
    likes: int | None = Field(default=None, description="Total number of likes across all content.")
    comments: int | None = Field(default=None, description="Total number of comments across all content.")
    shares: int | None = Field(default=None, description="Total number of shares across all content.")
    saves: int | None = Field(default=None, description="Total number of saves across all content.")
    reactions: int | None = Field(
        default=None, description="Total reactions (often includes likes/emojis, but can be a custom metric)."
    )
    engagements_post: int | None = Field(default=None, description="Engagements on posts.")
    engagements_reel: int | None = Field(default=None, description="Engagements on Reels.")
    engagements_story: int | None = Field(default=None, description="Engagements on Stories.")
    engagements_igtv: int | None = Field(default=None, description="Engagements on IGTV.")
    engagements_live: int | None = Field(default=None, description="Engagements on Live videos.")
    engagements_ad: int | None = Field(default=None, description="Engagements on Ads.")
    likes_post: int | None = Field(default=None, description="Likes on posts.")
    likes_reel: int | None = Field(default=None, description="Likes on Reels.")
    likes_story: int | None = Field(default=None, description="Likes on Stories.")
    likes_igtv: int | None = Field(default=None, description="Likes on IGTV.")
    likes_carousel: int | None = Field(default=None, description="Likes on carousel posts.")
    likes_ad: int | None = Field(default=None, description="Likes on Ads.")
    comments_post: int | None = Field(default=None, description="Comments on posts.")
    comments_reel: int | None = Field(default=None, description="Comments on Reels.")
    comments_story: int | None = Field(default=None, description="Comments on Stories.")
    comments_igtv: int | None = Field(default=None, description="Comments on IGTV.")
    comments_live: int | None = Field(default=None, description="Comments on Live videos.")
    comments_ad: int | None = Field(default=None, description="Comments on Ads.")
    shares_post: int | None = Field(default=None, description="Shares of posts.")
    shares_reel: int | None = Field(default=None, description="Shares of Reels.")
    shares_story: int | None = Field(default=None, description="Shares of Stories.")
    shares_igtv: int | None = Field(default=None, description="Shares of IGTV.")
    shares_carousel: int | None = Field(default=None, description="Shares of carousel posts.")
    shares_ad: int | None = Field(default=None, description="Shares of Ads.")
    saves_post: int | None = Field(default=None, description="Saves of posts.")
    saves_reel: int | None = Field(default=None, description="Saves of Reels.")
    saves_igtv: int | None = Field(default=None, description="Saves of IGTV.")
    saves_ad: int | None = Field(default=None, description="Saves of Ads.")
    cta_clicks: int | None = Field(default=None, description="Total clicks on call-to-action buttons.")
    website_clicks: int | None = Field(default=None, description="Clicks to the external website link.")
    directions_clicks: int | None = Field(default=None, description="Clicks on the 'Get Directions' button.")
    phone_call_clicks: int | None = Field(default=None, description="Clicks on the 'Call' button.")
    text_message_clicks: int | None = Field(default=None, description="Clicks on the 'Text' button.")
    profile_links_taps_book_now: int | None = Field(default=None, description="Taps on the 'Book Now' profile link.")
    profile_links_taps_call: int | None = Field(default=None, description="Taps on the 'Call' profile link.")
    profile_links_taps_direction: int | None = Field(default=None, description="Taps on the 'Direction' profile link.")
    profile_links_taps_email: int | None = Field(default=None, description="Taps on the 'Email' profile link.")
    profile_links_taps_instant_experience: int | None = Field(
        default=None, description="Taps on 'Instant Experience' links (from ads)."
    )
    profile_links_taps_text: int | None = Field(default=None, description="Taps on the 'Text' profile link.")
    direct_messages: int | None = Field(default=None, description="Total number of direct messages received/sent.")
    incoming_private_messages: int | None = Field(default=None, description="Number of direct messages received.")
    stories: int | None = Field(default=None, description="Total number of stories published.")
    story_replies: int | None = Field(default=None, description="Number of replies received on stories.")
    story_mentions: int | None = Field(
        default=None, description="Number of times the account was mentioned in stories."
    )
    views: int | None = Field(
        default=None, description="General views count (often associated with Stories or Profile views)."
    )
