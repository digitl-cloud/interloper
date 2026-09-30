import datetime as dt
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.instagram_insights import constants
from interloper_assets.instagram_insights.connection import InstagramInsightsConnection
from interloper_assets.instagram_insights.schemas import (
    AccountStats,
    EngagementStats,
    FollowersStatsByAgeGender,
    FollowersStatsByCity,
    FollowersStatsByCountry,
    Media,
    MediaStats,
    Profiles,
)

logger = logging.getLogger(__name__)


# -- HELPERS -------------------------------------------------------------------
async def _paginate(client: il.AsyncRESTClient, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """GET every page of a Graph API edge, following ``paging.next``.

    Used by every asset that reads an edge: the account insights, ``media``,
    ``media_stats`` and its stories and per-media insights.

    Args:
        client: The client to send the requests with.
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
    icon="skill-icons:instagram",
    normalizer=DataFrameNormalizer(),
)
class InstagramInsights(il.Source):
    """Instagram Business and Creator account insights integration."""

    connection: InstagramInsightsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Instagram Business or Creator account ID",
        discriminator=True,
    )

    # -- INTERNALS -------------------------------------------------------------
    async def _account_insights(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Read ``GET /{account_id}/insights`` and return every insight object.

        Shared by the account-level reports (``account_stats``, ``engagement_stats``
        and the three ``followers_stats_by_*`` demographics), which differ only in *params*.

        Args:
            params: The insights query (``metric``, ``period``, ``metric_type``, ``breakdown``, ...).

        Returns:
            The insight objects (``{name, period, values | total_value, ...}``) of every page.
        """
        return await _paginate(self.connection.client, f"/{self.account_id}/insights", params)

    async def _follower_demographics(self, date: dt.date, breakdown: list[str]) -> list[dict[str, Any]]:
        """Read the ``follower_demographics`` metric broken down by *breakdown*, one row per result.

        Shared by the three ``followers_stats_by_*`` assets. The value sits in
        ``total_value.breakdowns[].results[]`` with its dimension values listed in
        the order of the breakdown's ``dimension_keys``; each result becomes a row
        keyed by those dimension names, the count under the metric's own name.

        Args:
            date: The partition day, sent as ``since`` and stamped as ``date``.
            breakdown: The demographic dimensions (``country``, ``city``, ``gender``, ``age``).

        Returns:
            One row per demographic segment, stamped with *date*.
        """
        insights = await self._account_insights(
            {
                "metric": "follower_demographics",
                "period": "lifetime",
                "metric_type": "total_value",
                "breakdown": ",".join(breakdown),
                "since": date.isoformat(),
                "until": (date + dt.timedelta(days=1)).isoformat(),
            },
        )
        rows = []
        for insight in insights:
            for group in insight.get("total_value", {}).get("breakdowns", []):
                for result in group.get("results", []):
                    row: dict[str, Any] = dict(zip(group["dimension_keys"], result["dimension_values"], strict=True))
                    row[insight["name"]] = result.get("value")
                    row["date"] = date
                    rows.append(row)
        return rows

    # -- REPORTS ---------------------------------------------------------------
    @il.asset(schema=AccountStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def account_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily Instagram account insights: new followers and reach."""
        metrics = list(constants.ACCOUNT_METRICS)
        # follower_count only answers for the last 30 days and fails the whole request beyond
        if context.partition_date < dt.date.today() - dt.timedelta(days=constants.FOLLOWER_COUNT_MAX_AGE_DAYS):
            metrics.remove("follower_count")
        insights = await self._account_insights(
            {
                "metric": ",".join(metrics),
                "period": "day",
                "since": context.partition_date.isoformat(),
                "until": (context.partition_date + dt.timedelta(days=1)).isoformat(),
            },
        )
        rows: dict[str | None, dict[str, Any]] = {}
        for insight in insights:
            for entry in insight.get("values", []):
                end_time = entry.get("end_time")
                row = rows.setdefault(end_time, {"end_time": end_time, "date": context.partition_date})
                row[insight["name"]] = entry.get("value")
        return list(rows.values())

    @il.asset(schema=EngagementStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def engagement_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily Instagram account engagement totals: reach, interactions, follows, likes, shares, saves, views."""
        insights = await self._account_insights(
            {
                "metric": ",".join(constants.ENGAGEMENT_METRICS),
                "period": "day",
                "metric_type": "total_value",
                "since": context.partition_date.isoformat(),
                "until": (context.partition_date + dt.timedelta(days=1)).isoformat(),
            },
        )
        if not insights:
            return []
        row: dict[str, Any] = {insight["name"]: insight.get("total_value", {}).get("value") for insight in insights}
        return [{**row, "date": context.partition_date}]

    @il.asset(schema=FollowersStatsByCountry, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def followers_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime follower counts of the Instagram account by country.

        Each partition holds the counts as of the day it ran, so a backfilled partition carries today's audience.
        """
        return await self._follower_demographics(context.partition_date, ["country"])

    @il.asset(schema=FollowersStatsByCity, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def followers_stats_by_city(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime follower counts of the Instagram account by city.

        Each partition holds the counts as of the day it ran, so a backfilled partition carries today's audience.
        """
        return await self._follower_demographics(context.partition_date, ["city"])

    @il.asset(schema=FollowersStatsByAgeGender, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def followers_stats_by_age_gender(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime follower counts of the Instagram account by gender and age bracket.

        Each partition holds the counts as of the day it ran, so a backfilled partition carries today's audience.
        """
        return await self._follower_demographics(context.partition_date, ["gender", "age"])

    @il.asset(schema=MediaStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def media_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime insights of Instagram posts and reels from the last 180 days and of live stories.

        Snapshotted daily: each partition holds every media item's cumulative metrics as of the day it ran,
        so a backfilled partition carries today's totals and today's live stories.
        """
        client = self.connection.client
        fields = ",".join(constants.MEDIA_STATS_FIELDS)
        since = context.partition_date - dt.timedelta(days=constants.MEDIA_LOOKBACK_DAYS)
        until = context.partition_date + dt.timedelta(days=1)

        def unix(day: dt.date) -> int:
            return int(dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc).timestamp())

        media = await _paginate(
            client,
            f"/{self.account_id}/media",
            {"fields": fields, "since": unix(since), "until": unix(until)},
        )
        # stories live for 24 hours and are only listed by /stories, never by /media
        stories = await _paginate(client, f"/{self.account_id}/stories", {"fields": fields})
        items = [*media, *stories]

        async def insights(item: dict[str, Any]) -> list[dict[str, Any]]:
            # Graph rejects the whole request when one metric does not apply to the product type
            metrics = constants.MEDIA_METRICS_BY_PRODUCT_TYPE.get(item.get("media_product_type") or "")
            if metrics is None:
                logger.warning("No insight metrics for media %s of type %s", item["id"], item.get("media_product_type"))
                return []
            return await _paginate(client, f"/{item['id']}/insights", {"metric": ",".join(metrics)})

        results = await il.bounded_gather((insights(item) for item in items), limit=constants.INSIGHTS_CONCURRENCY)

        rows = []
        for item, item_insights in zip(items, results, strict=True):
            row = dict(item)
            for insight in item_insights:
                values = insight.get("values") or [{}]
                row[insight["name"]] = values[0].get("value")
            row["date"] = context.partition_date
            rows.append(row)
        return rows

    # -- ENTITIES --------------------------------------------------------------
    @il.asset(schema=Profiles, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def profiles(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The Instagram account's profile with its follower, following and media counts."""
        response = await self.connection.client.get(
            f"/{self.account_id}",
            params={"fields": ",".join(constants.PROFILE_FIELDS)},
        )
        response.raise_for_status()
        return [{**response.json(), "date": context.partition_date}]

    @il.asset(schema=Media, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def media(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Every media item of the Instagram account with its attributes, like and comment counts."""
        items = await _paginate(
            self.connection.client,
            f"/{self.account_id}/media",
            {"fields": ",".join(constants.MEDIA_FIELDS)},
        )
        return [{**item, "date": context.partition_date} for item in items]
