BASE_URL = "https://graph.facebook.com"
API_VERSION = "v26.0"

MEDIA_LOOKBACK_DAYS = 180
FOLLOWER_COUNT_MAX_AGE_DAYS = 30
INSIGHTS_CONCURRENCY = 8

ACCOUNT_METRICS = [
    "follower_count",
    "reach",
]

ENGAGEMENT_METRICS = [
    "reach",
    "accounts_engaged",
    "total_interactions",
    "follows_and_unfollows",
    "likes",
    "comments",
    "shares",
    "saves",
    "replies",
    "profile_links_taps",
    "views",
    "reposts",
]

PROFILE_FIELDS = [
    "followers_count",
    "follows_count",
    "media_count",
]

MEDIA_FIELDS = [
    "caption",
    "comments_count",
    "id",
    "ig_id",
    "is_comment_enabled",
    "is_shared_to_feed",
    "like_count",
    "media_product_type",
    "media_type",
    "media_url",
    "permalink",
    "shortcode",
    "thumbnail_url",
    "timestamp",
    "username",
]

MEDIA_STATS_FIELDS = [
    "id",
    "caption",
    "media_type",
    "media_product_type",
    "timestamp",
    "permalink",
    "boost_ads_list",
    "boost_eligibility_info",
    "legacy_instagram_media_id",
    "media_url",
    "shortcode",
    "thumbnail_url",
]

FEED_METRICS = [
    "reach",
    "saved",
    "likes",
    "comments",
    "shares",
    "total_interactions",
    "follows",
    "profile_visits",
    "profile_activity",
    "views",
    "reposts",
    "facebook_views",
    "total_views",
    "total_likes",
    "total_comments",
]

REELS_METRICS = [
    "reach",
    "saved",
    "likes",
    "comments",
    "shares",
    "total_interactions",
    "ig_reels_video_view_total_time",
    "ig_reels_avg_watch_time",
    "views",
    "reels_skip_rate",
    "reposts",
    "facebook_views",
    "crossposted_views",
    "total_views",
    "total_likes",
    "total_comments",
]

STORY_METRICS = [
    "reach",
    "replies",
    "shares",
    "total_interactions",
    "follows",
    "profile_visits",
    "profile_activity",
    "navigation",
    "views",
    "reposts",
    "facebook_views",
    "total_views",
    "link_clicks",
]

MEDIA_METRICS_BY_PRODUCT_TYPE = {
    "FEED": FEED_METRICS,
    "REELS": REELS_METRICS,
    "STORY": STORY_METRICS,
}
