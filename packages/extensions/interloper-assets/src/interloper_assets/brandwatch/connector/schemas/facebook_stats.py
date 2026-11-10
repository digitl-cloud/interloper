import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FacebookStats(Schema):
    """Facebook channel performance metrics per day (Brandwatch/Falcon.io Measure)."""

    date: dt.date | None = Field(default=None, description="The date the metrics correspond to.")
    channel_id: str | None = Field(default=None, description="Unique identifier for the Facebook Page.")
    engagements: int | None = Field(
        default=None, description="Total number of post engagements (Reactions, Comments, Shares, Clicks)."
    )
    engaged_users: int | None = Field(default=None, description="The number of unique people who engaged with content.")
    engagement_rate: float | None = Field(
        default=None, description="Overall engagement rate (e.g., Engagements / Impressions or Reach)."
    )
    engagement_rate_reach: float | None = Field(default=None, description="Engagement rate calculated using reach.")
    interaction_rate: float | None = Field(
        default=None, description="Rate of interactions (engagement divided by impressions or reach)."
    )
    likes: int | None = Field(default=None, description="Total number of reactions of type 'Like' on posts.")
    comments: int | None = Field(default=None, description="Total number of comments on posts.")
    shares: int | None = Field(default=None, description="Total number of shares of posts.")
    reactions: int | None = Field(
        default=None, description="Total number of Reactions (sum of Like, Love, Wow, Haha, Sad, Angry) on posts."
    )
    clicks: int | None = Field(default=None, description="Total clicks on a post (Link, Photo, Other).")
    fans: int | None = Field(default=None, description="Total number of Page Likes/Fans.")
    followers: int | None = Field(default=None, description="Total number of people who follow the account/Page.")
    net_fans: int | None = Field(default=None, description="Net change in Fans (New Fans - Unfans).")
    net_followers: int | None = Field(default=None, description="Net change in Followers.")
    negative_fans: int | None = Field(default=None, description="Number of Unfans/Unfollows.")
    likes_total: int | None = Field(default=None, description="Cumulative Likes for the account/page.")
    net_likes_total: int | None = Field(default=None, description="Net change in total likes.")
    impressions: int | None = Field(
        default=None, description="Total number of times content entered a person's screen (non-unique)."
    )
    impressions_organic: int | None = Field(default=None, description="Number of organic impressions.")
    impressions_paid: int | None = Field(default=None, description="Number of paid impressions.")
    reach: int | None = Field(default=None, description="Total number of unique people reached.")
    reach_organic: int | None = Field(default=None, description="Number of unique people reached organically.")
    reach_paid: int | None = Field(default=None, description="Number of unique people reached via paid advertising.")
    reach_viral: int | None = Field(
        default=None, description="Unique people reached due to viral activity (friend interaction)."
    )
    reach_nonviral: int | None = Field(
        default=None, description="Unique people reached not attributed to viral activity."
    )
    viral_amplification: float | None = Field(
        default=None, description="Measure of how much content is spread virally."
    )
    video_views: int | None = Field(
        default=None, description="Total number of video views (e.g., 3-second view count)."
    )
    video_views_10s: int | None = Field(default=None, description="Total number of video views of at least 10 seconds.")
    video_views_30s: int | None = Field(default=None, description="Total number of video views of at least 30 seconds.")
    video_views_organic: int | None = Field(default=None, description="Number of organic video views.")
    video_views_paid: int | None = Field(default=None, description="Number of paid video views.")
    video_plays: int | None = Field(default=None, description="Number of times videos started playing.")
    video_repeat_views: int | None = Field(
        default=None, description="Number of times videos were played again by the same user."
    )
    video_view_time: float | None = Field(default=None, description="Total accumulated video view time in seconds.")
    video_viewers: int | None = Field(default=None, description="Number of unique video viewers.")
    views: int | None = Field(default=None, description="Total number of Page/Profile Views.")
    views_by_logged_in_out_logged_in: int | None = Field(default=None, description="Views from logged-in users.")
    views_by_logged_in_out_logged_out: int | None = Field(default=None, description="Views from logged-out users.")
    new_dm_conversations: int | None = Field(
        default=None, description="Number of new direct message conversations started."
    )
    blocked_dm_conversations: int | None = Field(
        default=None, description="Number of blocked direct message conversations."
    )
