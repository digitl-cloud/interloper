BASE_URL = "https://graph.facebook.com"
API_VERSION = "v26.0"

POSTS_LOOKBACK_DAYS = 180
BREAKDOWN_CONCURRENCY = 8
UNSUPPORTED_BREAKDOWN_ERROR_CODE = 100

INSIGHTS_PAGE_METRICS = [
    "page_actions_post_reactions_anger_total",
    "page_actions_post_reactions_haha_total",
    "page_actions_post_reactions_like_total",
    "page_actions_post_reactions_love_total",
    "page_actions_post_reactions_sorry_total",
    "page_actions_post_reactions_wow_total",
    "page_actions_post_reactions_total",
    "page_follows",
    "page_media_view",
    "page_post_engagements",
    "page_total_media_view_unique",
    "page_total_actions",
    "page_video_complete_views_30s_autoplayed",
    "page_video_complete_views_30s_click_to_play",
    "page_video_complete_views_30s_organic",
    "page_video_complete_views_30s_paid",
    "page_video_complete_views_30s_repeat_views",
    "page_video_complete_views_30s_unique",
    "page_video_complete_views_30s",
    "page_video_repeat_views",
    "page_video_view_time",
    "page_video_views_autoplayed",
    "page_video_views_by_paid_non_paid",
    "page_video_views_click_to_play",
    "page_video_views_organic",
    "page_video_views_paid",
    "page_video_views",
    "page_views_total",
]

INSIGHTS_POST_METRICS = [
    "post_activity_by_action_type_unique",
    "post_activity_by_action_type",
    "post_clicks_by_type",
    "post_clicks",
    "post_media_view",
    "post_total_media_view_unique",
    "post_reactions_by_type_total",
    "post_video_avg_time_watched",
    "post_video_complete_views_30s_autoplayed",
    "post_video_complete_views_30s_clicked_to_play",
    "post_video_complete_views_30s_organic",
    "post_video_complete_views_30s_paid",
    "post_video_complete_views_30s_unique",
    "post_video_complete_views_organic_unique",
    "post_video_complete_views_organic",
    "post_video_complete_views_paid_unique",
    "post_video_complete_views_paid",
    "post_video_length",
    "post_video_view_time_organic",
    "post_video_view_time",
    "post_video_views_autoplayed",
    "post_video_views_clicked_to_play",
    "post_video_views_organic_unique",
    "post_video_views_organic",
    "post_video_views_paid_unique",
    "post_video_views_paid",
    "post_video_views_sound_on",
    "post_video_views",
]

MEDIA_VIEW_BREAKDOWNS = [
    "is_from_ads",
    "is_from_followers",
]

POST_FIELDS = [
    "id",
    "message",
    "created_time",
    "updated_time",
    "status_type",
    "picture",
    "full_picture",
    "permalink_url",
]

STORY_FIELDS = [
    "id",
    "created_time",
    "updated_time",
    "picture",
    "full_picture",
    "permalink_url",
]
