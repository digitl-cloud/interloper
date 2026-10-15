import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PostsStats(Schema):
    """Facebook Page post and story insights. One row per post or story updated in the last 180 days, per snapshot day, carrying the post's attributes and its cumulative lifetime metrics as of that day. Stories have no message or status_type. Dict-valued metrics are flattened one level (post_reactions_by_type_total becomes post_reactions_by_type_total_like, _love, ...)."""

    id: str | None = Field(default=None, description="Unique post identifier in page_id_post_id format.")
    message: str | None = Field(default=None, description="Text content / caption of the post.")
    created_time: dt.datetime | None = Field(default=None, description="When the post was published.")
    updated_time: dt.datetime | None = Field(default=None, description="When the post was last updated.")
    status_type: str | None = Field(
        default=None,
        description="Type of post e.g. added_video, shared_story, mobile_status_update. Absent for stories.",
    )
    picture: str | None = Field(default=None, description="Thumbnail image URL of the post.")
    full_picture: str | None = Field(default=None, description="Full-size image URL of the post.")
    permalink_url: str | None = Field(default=None, description="Public URL/permalink to the post.")
    post_activity_by_action_type_unique_like: int | None = Field(
        default=None,
        description="Lifetime: Unique people who liked the post (from post_activity_by_action_type_unique dict explosion).",
    )
    post_activity_by_action_type_unique_comment: int | None = Field(
        default=None,
        description="Lifetime: Unique people who commented on the post. NULL when count is 0 (key absent from dict).",
    )
    post_activity_by_action_type_unique_share: int | None = Field(
        default=None,
        description="Lifetime: Unique people who shared the post. NULL when count is 0 (key absent from dict).",
    )
    post_activity_by_action_type_like: int | None = Field(
        default=None,
        description="Lifetime: Total like interactions on the post (from post_activity_by_action_type dict explosion).",
    )
    post_activity_by_action_type_comment: int | None = Field(
        default=None, description="Lifetime: Total comments on the post. NULL when count is 0 (key absent from dict)."
    )
    post_activity_by_action_type_share: int | None = Field(
        default=None, description="Lifetime: Total shares of the post. NULL when count is 0 (key absent from dict)."
    )
    post_clicks_by_type_link_clicks: int | None = Field(
        default=None, description="Lifetime: Clicks on links in the post (from post_clicks_by_type dict explosion)."
    )
    post_clicks_by_type_video_play: int | None = Field(
        default=None, description="Lifetime: Clicks that played the video (from post_clicks_by_type dict explosion)."
    )
    post_clicks_by_type_other_clicks: int | None = Field(
        default=None,
        description="Lifetime: Other clicks on the post e.g. see more (from post_clicks_by_type dict explosion).",
    )
    post_clicks_by_type_photo_view: int | None = Field(
        default=None, description="Lifetime: Clicks that viewed the photo (from post_clicks_by_type dict explosion)."
    )
    post_clicks: int | None = Field(default=None, description="Lifetime: Total clicks anywhere on the post.")
    post_media_view: int | None = Field(
        default=None,
        description="Lifetime: Number of times media attached to the post was viewed (total paid + organic).",
    )
    post_media_view_is_from_ads_0: int | None = Field(
        default=None,
        description="Lifetime: post_media_view with breakdown is_from_ads=0, views not sourced from an ad (organic). Feed posts only.",
    )
    post_media_view_is_from_ads_1: int | None = Field(
        default=None,
        description="Lifetime: post_media_view with breakdown is_from_ads=1, views sourced from an ad (paid). Feed posts only.",
    )
    post_media_view_is_from_followers_0: int | None = Field(
        default=None,
        description="Lifetime: post_media_view with breakdown is_from_followers=0, views from people who do not follow the Page. Feed posts only.",
    )
    post_media_view_is_from_followers_1: int | None = Field(
        default=None,
        description="Lifetime: post_media_view with breakdown is_from_followers=1, views from people who follow the Page. Feed posts only.",
    )
    post_total_media_view_unique: int | None = Field(default=None, description="Lifetime: unique viewers of the post.")
    post_reactions_by_type_total_like: int | None = Field(
        default=None, description="Lifetime: Total like reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_reactions_by_type_total_love: int | None = Field(
        default=None, description="Lifetime: Total love reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_reactions_by_type_total_wow: int | None = Field(
        default=None, description="Lifetime: Total wow reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_reactions_by_type_total_haha: int | None = Field(
        default=None, description="Lifetime: Total haha reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_reactions_by_type_total_sorry: int | None = Field(
        default=None, description="Lifetime: Total sorry reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_reactions_by_type_total_anger: int | None = Field(
        default=None, description="Lifetime: Total anger reactions (from post_reactions_by_type_total dict explosion)."
    )
    post_video_avg_time_watched: int | None = Field(
        default=None, description="Lifetime: Average watch time per view in milliseconds."
    )
    post_video_complete_views_30s_autoplayed: int | None = Field(
        default=None, description="Lifetime: Auto-played views with at least 30 seconds watched."
    )
    post_video_complete_views_30s_clicked_to_play: int | None = Field(
        default=None, description="Lifetime: Click-to-play views with at least 30 seconds watched."
    )
    post_video_complete_views_30s_organic: int | None = Field(
        default=None, description="Lifetime: Organic views with at least 30 seconds watched."
    )
    post_video_complete_views_30s_paid: int | None = Field(
        default=None, description="Lifetime: Paid-promoted views with at least 30 seconds watched."
    )
    post_video_complete_views_30s_unique: int | None = Field(
        default=None, description="Lifetime: Unique viewers who watched at least 30 seconds."
    )
    post_video_complete_views_organic_unique: int | None = Field(
        default=None, description="Lifetime: Unique viewers who watched the full video organically."
    )
    post_video_complete_views_organic: int | None = Field(
        default=None, description="Lifetime: Full organic video completions."
    )
    post_video_complete_views_paid_unique: int | None = Field(
        default=None, description="Lifetime: Unique viewers who completed the video via paid promotion."
    )
    post_video_complete_views_paid: int | None = Field(
        default=None, description="Lifetime: Full video completions via paid promotion."
    )
    post_video_length: int | None = Field(default=None, description="Video duration in milliseconds.")
    post_video_view_time_organic: int | None = Field(
        default=None, description="Lifetime: Total watch time (ms) for organic views only."
    )
    post_video_view_time: int | None = Field(
        default=None, description="Lifetime: Total watch time (ms) across all views."
    )
    post_video_views_autoplayed: int | None = Field(
        default=None, description="Lifetime: Views via auto-play (3s+ threshold)."
    )
    post_video_views_clicked_to_play: int | None = Field(
        default=None, description="Lifetime: Views via click-to-play (3s+ threshold)."
    )
    post_video_views_organic_unique: int | None = Field(
        default=None, description="Lifetime: Unique viewers who watched 3s+ organically."
    )
    post_video_views_organic: int | None = Field(
        default=None, description="Lifetime: Total 3s+ views via organic reach."
    )
    post_video_views_paid_unique: int | None = Field(
        default=None, description="Lifetime: Unique viewers who watched 3s+ via paid promotion."
    )
    post_video_views_paid: int | None = Field(default=None, description="Lifetime: Total 3s+ views via paid promotion.")
    post_video_views_sound_on: int | None = Field(default=None, description="Lifetime: Views with sound on.")
    post_video_views: int | None = Field(
        default=None, description="Lifetime: Total 3s+ video views (standard FB view definition)."
    )
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
