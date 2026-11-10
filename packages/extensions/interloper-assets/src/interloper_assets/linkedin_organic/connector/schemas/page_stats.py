import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PageStats(Schema):
    """LinkedIn organization page views and clicks by day. One row per organization per day, with views per page tab and device and custom button clicks."""

    date: dt.date | None = Field(
        default=None, description="The day the statistics cover (stamped from the partition, UTC)."
    )
    organization: str | None = Field(default=None, description="The URN of the organization.")
    time_range_start: dt.datetime | None = Field(default=None, description="Start of the covered day (UTC).")
    time_range_end: dt.datetime | None = Field(default=None, description="End of the covered day (UTC).")
    total_page_statistics_views_about_page_views_page_views: int | None = Field(
        default=None, description="Page views of the about page."
    )
    total_page_statistics_views_about_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the about page."
    )
    total_page_statistics_views_all_page_views_page_views: int | None = Field(
        default=None, description="Page views of all pages."
    )
    total_page_statistics_views_all_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of all pages."
    )
    total_page_statistics_views_all_desktop_page_views_page_views: int | None = Field(
        default=None, description="Page views of all pages on desktop."
    )
    total_page_statistics_views_all_desktop_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of all pages on desktop."
    )
    total_page_statistics_views_all_mobile_page_views_page_views: int | None = Field(
        default=None, description="Page views of all pages on mobile."
    )
    total_page_statistics_views_all_mobile_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of all pages on mobile."
    )
    total_page_statistics_views_careers_page_views_page_views: int | None = Field(
        default=None, description="Page views of the careers page."
    )
    total_page_statistics_views_careers_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the careers page."
    )
    total_page_statistics_views_desktop_about_page_views_page_views: int | None = Field(
        default=None, description="Page views of the about page on desktop."
    )
    total_page_statistics_views_desktop_about_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the about page on desktop."
    )
    total_page_statistics_views_desktop_careers_page_views_page_views: int | None = Field(
        default=None, description="Page views of the careers page on desktop."
    )
    total_page_statistics_views_desktop_careers_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the careers page on desktop."
    )
    total_page_statistics_views_desktop_insights_page_views_page_views: int | None = Field(
        default=None, description="Page views of the insights page on desktop."
    )
    total_page_statistics_views_desktop_jobs_page_views_page_views: int | None = Field(
        default=None, description="Page views of the jobs page on desktop."
    )
    total_page_statistics_views_desktop_jobs_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the jobs page on desktop."
    )
    total_page_statistics_views_desktop_life_at_page_views_page_views: int | None = Field(
        default=None, description="Page views of the life at page on desktop."
    )
    total_page_statistics_views_desktop_overview_page_views_page_views: int | None = Field(
        default=None, description="Page views of the overview page on desktop."
    )
    total_page_statistics_views_desktop_overview_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the overview page on desktop."
    )
    total_page_statistics_views_desktop_people_page_views_page_views: int | None = Field(
        default=None, description="Page views of the people page on desktop."
    )
    total_page_statistics_views_desktop_people_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the people page on desktop."
    )
    total_page_statistics_views_desktop_products_page_views_page_views: int | None = Field(
        default=None, description="Page views of the products page on desktop."
    )
    total_page_statistics_views_insights_page_views_page_views: int | None = Field(
        default=None, description="Page views of the insights page."
    )
    total_page_statistics_views_jobs_page_views_page_views: int | None = Field(
        default=None, description="Page views of the jobs page."
    )
    total_page_statistics_views_jobs_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the jobs page."
    )
    total_page_statistics_views_life_at_page_views_page_views: int | None = Field(
        default=None, description="Page views of the life at page."
    )
    total_page_statistics_views_mobile_about_page_views_page_views: int | None = Field(
        default=None, description="Page views of the about page on mobile."
    )
    total_page_statistics_views_mobile_about_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the about page on mobile."
    )
    total_page_statistics_views_mobile_careers_page_views_page_views: int | None = Field(
        default=None, description="Page views of the careers page on mobile."
    )
    total_page_statistics_views_mobile_careers_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the careers page on mobile."
    )
    total_page_statistics_views_mobile_insights_page_views_page_views: int | None = Field(
        default=None, description="Page views of the insights page on mobile."
    )
    total_page_statistics_views_mobile_jobs_page_views_page_views: int | None = Field(
        default=None, description="Page views of the jobs page on mobile."
    )
    total_page_statistics_views_mobile_jobs_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the jobs page on mobile."
    )
    total_page_statistics_views_mobile_life_at_page_views_page_views: int | None = Field(
        default=None, description="Page views of the life at page on mobile."
    )
    total_page_statistics_views_mobile_overview_page_views_page_views: int | None = Field(
        default=None, description="Page views of the overview page on mobile."
    )
    total_page_statistics_views_mobile_overview_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the overview page on mobile."
    )
    total_page_statistics_views_mobile_people_page_views_page_views: int | None = Field(
        default=None, description="Page views of the people page on mobile."
    )
    total_page_statistics_views_mobile_people_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the people page on mobile."
    )
    total_page_statistics_views_mobile_products_page_views_page_views: int | None = Field(
        default=None, description="Page views of the products page on mobile."
    )
    total_page_statistics_views_overview_page_views_page_views: int | None = Field(
        default=None, description="Page views of the overview page."
    )
    total_page_statistics_views_overview_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the overview page."
    )
    total_page_statistics_views_people_page_views_page_views: int | None = Field(
        default=None, description="Page views of the people page."
    )
    total_page_statistics_views_people_page_views_unique_page_views: int | None = Field(
        default=None, description="Unique page views of the people page."
    )
    total_page_statistics_views_products_page_views_page_views: int | None = Field(
        default=None, description="Page views of the products page."
    )
    total_page_statistics_clicks_desktop_custom_button_click_counts: str | None = Field(
        default=None, description="Custom button clicks on desktop (JSON array of button type and clicks)."
    )
    total_page_statistics_clicks_mobile_custom_button_click_counts: str | None = Field(
        default=None, description="Custom button clicks on mobile (JSON array of button type and clicks)."
    )
