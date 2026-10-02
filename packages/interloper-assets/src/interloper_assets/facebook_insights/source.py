import datetime as dt
import logging
from collections.abc import Iterator
from typing import Any

import httpx2
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_insights import constants
from interloper_assets.facebook_insights.connection import FacebookInsightsConnection
from interloper_assets.facebook_insights.schemas import PageStats, PostsStats

logger = logging.getLogger(__name__)


# -- HELPERS -------------------------------------------------------------------
def _insight_values(insight: dict[str, Any]) -> Iterator[tuple[str | None, str, Any]]:
    """Unpack one Graph API insight object into ``(end_time, column, value)`` triples.

    An insight is metric-major (``{name, values: [{value, end_time, <breakdown>...}]}``);
    both ``page_stats`` and ``posts_stats`` pivot it into one column per metric. A
    value entry carrying a breakdown key (``is_from_ads``, ``is_from_followers``)
    lands on ``<name>_<breakdown>_<breakdown value>``, the way the vendor keys it.
    Dict-valued metrics (``post_reactions_by_type_total``) are left for the
    normalizer's flatten pass.

    Args:
        insight: One element of an insights response's ``data`` list.

    Yields:
        The value's ``end_time`` (None for lifetime values), its column and its value.
    """
    name = insight["name"]
    for entry in insight.get("values", []):
        breakdowns = {key: value for key, value in entry.items() if key not in ("value", "end_time")}
        column = "_".join([name, *(f"{key}_{value}" for key, value in breakdowns.items())])
        yield entry.get("end_time"), column, entry.get("value")


async def _paginate(client: il.AsyncRESTClient, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """GET every page of a Graph API edge, following ``paging.next``.

    Used by the page, post and story fetches alike.

    Args:
        client: The client to send the requests with (a Page client for Page edges).
        path: The edge path, relative to the versioned base URL.
        params: The query parameters of the first request; ``next`` links carry them on.

    Returns:
        The concatenated ``data`` of every page.
    """
    paginator = il.JSONLinkPaginator(next_url_path="paging.next")
    pages = client.paginate(path, paginator, params=params, data_selector="data")
    return [row async for page in pages for row in page]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Social Media"],
    icon="logos:facebook",
    normalizer=DataFrameNormalizer(flatten_max_level=1),
    maturity="beta",
)
class FacebookInsights(il.Source):
    """Facebook Page and Post Insights integration."""

    connection: FacebookInsightsConnection

    page_id: str = il.FetchField(
        provider="connection.pages",
        label_key="name",
        value_key="id",
        description="Facebook Page to retrieve insights for",
        discriminator=True,
    )

    @il.asset(schema=PageStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def page_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily Facebook Page insights: reactions, follows, media views, engagement and video views."""
        since = context.partition_date.isoformat()
        until = (context.partition_date + dt.timedelta(days=1)).isoformat()
        async with await self.connection.page_client(self.page_id) as client:
            insights = await _paginate(
                client,
                f"/{self.page_id}/insights",
                {"metric": ",".join(constants.INSIGHTS_PAGE_METRICS), "period": "day", "since": since, "until": until},
            )
            # breakdown (singular) is the parameter Graph honours; one breakdown per request
            for breakdown in constants.MEDIA_VIEW_BREAKDOWNS:
                insights += await _paginate(
                    client,
                    f"/{self.page_id}/insights",
                    {
                        "metric": "page_media_view",
                        "breakdown": breakdown,
                        "period": "day",
                        "since": since,
                        "until": until,
                    },
                )

        rows: dict[str | None, dict[str, Any]] = {}
        for insight in insights:
            for end_time, column, value in _insight_values(insight):
                row = rows.setdefault(end_time, {"end_time": end_time, "date": context.partition_date})
                row[column] = value
        return list(rows.values())

    @il.asset(schema=PostsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def posts_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime insights of every Facebook Page post and story updated in the last 180 days, snapshotted daily.

        Each partition holds the values as of the day it ran, so a backfilled partition carries today's totals.
        """
        since = context.partition_date - dt.timedelta(days=constants.POSTS_LOOKBACK_DAYS)
        until = context.partition_date + dt.timedelta(days=1)
        insights_field = f"insights.metric({','.join(constants.INSIGHTS_POST_METRICS)}){{name,values,id}}"

        async def breakdown_insights(client: il.AsyncRESTClient, post_id: str, breakdown: str) -> list[dict]:
            try:
                return await _paginate(
                    client,
                    f"/{post_id}/insights",
                    {"metric": "post_media_view", "breakdown": breakdown, "period": "lifetime"},
                )
            except httpx2.HTTPStatusError as error:
                try:
                    code = error.response.json().get("error", {}).get("code")
                except ValueError:
                    code = None
                # (#100) is Graph rejecting the breakdown for this kind of post; anything else is real
                if error.response.status_code != 400 or code != constants.UNSUPPORTED_BREAKDOWN_ERROR_CODE:
                    raise
                logger.warning("post_media_view breakdown %s is unsupported for post %s: %s", breakdown, post_id, error)
                return []

        async with await self.connection.page_client(self.page_id) as client:
            posts = await _paginate(
                client,
                f"/{self.page_id}/published_posts",
                {
                    "fields": ",".join([*constants.POST_FIELDS, insights_field]),
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                },
            )
            stories = await _paginate(
                client,
                f"/{self.page_id}/stories",
                {
                    "fields": ",".join([*constants.STORY_FIELDS, insights_field]),
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                },
            )

            since_instant = dt.datetime.combine(since, dt.time.min, tzinfo=dt.timezone.utc)
            items = [
                item
                for item in [*posts, *stories]
                if "updated_time" in item
                and dt.datetime.strptime(item["updated_time"], "%Y-%m-%dT%H:%M:%S%z") >= since_instant
            ]

            # stories carry no status_type and do not take the post_media_view breakdowns
            feed_ids = [item["id"] for item in items if item.get("status_type") is not None]
            requests = [(post_id, breakdown) for post_id in feed_ids for breakdown in constants.MEDIA_VIEW_BREAKDOWNS]
            results = await il.bounded_gather(
                (breakdown_insights(client, post_id, breakdown) for post_id, breakdown in requests),
                limit=constants.BREAKDOWN_CONCURRENCY,
            )
        breakdowns: dict[str, list[dict]] = {}
        for (post_id, _), result in zip(requests, results, strict=True):
            breakdowns.setdefault(post_id, []).extend(result)

        rows = []
        for item in items:
            row = {key: value for key, value in item.items() if key != "insights"}
            # the insights edge answers every metric twice; the /day copy is a rolling window
            lifetime = [
                insight
                for insight in item.get("insights", {}).get("data", [])
                if not insight.get("id", "").endswith("/day")
            ]
            for insight in [*lifetime, *breakdowns.get(item["id"], [])]:
                for _, column, value in _insight_values(insight):
                    row[column] = value
            row["date"] = context.partition_date
            rows.append(row)
        return rows
