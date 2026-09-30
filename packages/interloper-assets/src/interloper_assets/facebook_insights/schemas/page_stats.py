import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PageStats(Schema):
    """Daily Facebook Page insights. One row per Page per day, with every metric as a column: post reactions, follows, media views (with their paid/organic and follower/non-follower breakdowns), engagement, contact actions and video views."""

    end_time: dt.datetime | None = Field(
        default=None,
        description="End of the daily period the values cover, as returned by the API (midnight Pacific time).",
    )
    page_actions_post_reactions_anger_total: int | None = Field(
        default=None, description="Daily: total post anger reactions of a page."
    )
    page_actions_post_reactions_haha_total: int | None = Field(
        default=None, description="Daily: total post haha reactions of a page."
    )
    page_actions_post_reactions_like_total: int | None = Field(
        default=None, description="Daily: total post like reactions of a page."
    )
    page_actions_post_reactions_love_total: int | None = Field(
        default=None, description="Daily: total post love reactions of a page."
    )
    page_actions_post_reactions_sorry_total: int | None = Field(
        default=None, description="Daily: total post sorry reactions of a page."
    )
    page_actions_post_reactions_wow_total: int | None = Field(
        default=None, description="Daily: total post wow reactions of a page."
    )
    page_actions_post_reactions_total_like: int | None = Field(
        default=None,
        description="Daily: total post like reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_actions_post_reactions_total_love: int | None = Field(
        default=None,
        description="Daily: total post love reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_actions_post_reactions_total_wow: int | None = Field(
        default=None,
        description="Daily: total post wow reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_actions_post_reactions_total_haha: int | None = Field(
        default=None,
        description="Daily: total post haha reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_actions_post_reactions_total_sorry: int | None = Field(
        default=None,
        description="Daily: total post sorry reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_actions_post_reactions_total_anger: int | None = Field(
        default=None,
        description="Daily: total post anger reactions of a page (from the page_actions_post_reactions_total object).",
    )
    page_follows: int | None = Field(
        default=None,
        description="Lifetime: The number of followers of your Facebook Page or profile. This is calculated as the number of follows minus the number of unfollows over the lifetime of your Facebook Page or profile.",
    )
    page_media_view: int | None = Field(
        default=None,
        description="Daily: Total times any content from the Page was displayed on a person's screen (paid + organic).",
    )
    page_media_view_is_from_ads_0: int | None = Field(
        default=None,
        description="Daily: page_media_view with breakdown is_from_ads=0, views not sourced from an ad (organic).",
    )
    page_media_view_is_from_ads_1: int | None = Field(
        default=None,
        description="Daily: page_media_view with breakdown is_from_ads=1, views sourced from an ad (paid).",
    )
    page_media_view_is_from_followers_0: int | None = Field(
        default=None,
        description="Daily: page_media_view with breakdown is_from_followers=0, views from people who do not follow the Page.",
    )
    page_media_view_is_from_followers_1: int | None = Field(
        default=None,
        description="Daily: page_media_view with breakdown is_from_followers=1, views from people who follow the Page.",
    )
    page_post_engagements: int | None = Field(
        default=None,
        description="Daily: The number of times people have engaged with your posts through like, comments and shares and more.",
    )
    page_total_media_view_unique: int | None = Field(
        default=None,
        description="Daily: Unique viewers (people) who saw any content from the Page. Cross-platform replacement for deprecated page reach metrics.",
    )
    page_total_actions: int | None = Field(
        default=None, description="Daily: The number of clicks on your Page's contact info and call-to-action button."
    )
    page_video_complete_views_30s_autoplayed: int | None = Field(
        default=None,
        description="Daily: Number of times your page's videos started automatically playing and people viewed it for 30 seconds or to the end, whichever came first. (Total Count)",
    )
    page_video_complete_views_30s_click_to_play: int | None = Field(
        default=None,
        description="Daily: Number of times a video has been viewed for at least 30s after the user clicked play (Total Count)",
    )
    page_video_complete_views_30s_organic: int | None = Field(
        default=None,
        description="Daily: Number of times the video has been viewed for at least 30s by organic reach (Total Count)",
    )
    page_video_complete_views_30s_paid: int | None = Field(
        default=None,
        description="Daily: Number of times page's videos was viewed for 30 seconds or viewed to the end, whichever came first, after a paid promotion. (Total Count)",
    )
    page_video_complete_views_30s_repeat_views: int | None = Field(
        default=None,
        description="Daily: Number of times a video has been viewed for at least 30s outside the first play (Total Count)",
    )
    page_video_complete_views_30s_unique: int | None = Field(
        default=None,
        description="Daily: Metric showing videos played for unique people for at least 30 seconds aggregated at the page level (Unique Users)",
    )
    page_video_complete_views_30s: int | None = Field(
        default=None,
        description="Daily: Total number of times page's videos was viewed for at least 30 seconds. (Total Count)",
    )
    page_video_repeat_views: int | None = Field(
        default=None, description="Daily: Number of times the video has been seen outside the first play (Total Count)"
    )
    page_video_view_time: int | None = Field(
        default=None,
        description="Daily: The total amount of time (in milliseconds) people spent watching videos on your Page.",
    )
    page_video_views_autoplayed: int | None = Field(
        default=None,
        description="Daily: Number of times an auto-played video has been viewed for more than 3 seconds (Total Count)",
    )
    page_video_views_by_paid_non_paid_paid: int | None = Field(
        default=None,
        description="Daily: paid number of times page's videos have been viewed for more than 3 seconds (from the page_video_views_by_paid_non_paid object).",
    )
    page_video_views_by_paid_non_paid_unpaid: int | None = Field(
        default=None,
        description="Daily: non-paid number of times page's videos have been viewed for more than 3 seconds (from the page_video_views_by_paid_non_paid object).",
    )
    page_video_views_by_paid_non_paid_total: int | None = Field(
        default=None,
        description="Daily: total number of times page's videos have been viewed for more than 3 seconds (from the page_video_views_by_paid_non_paid object).",
    )
    page_video_views_click_to_play: int | None = Field(
        default=None,
        description="Daily: Number of times a video has been viewed after the user clicked play (Total Count)",
    )
    page_video_views_organic: int | None = Field(
        default=None, description="Daily: Number of times a video has been viewed due to organic reach (Total Count)"
    )
    page_video_views_paid: int | None = Field(
        default=None,
        description="Daily: Number of times a promoted video has been viewed for more than 3 seconds (Total Count)",
    )
    page_video_views: int | None = Field(
        default=None,
        description="Daily: Total number of times videos have been viewed for more than 3 seconds. (Total Count)",
    )
    page_views_total: int | None = Field(default=None, description="Daily: Total views count per Page")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
