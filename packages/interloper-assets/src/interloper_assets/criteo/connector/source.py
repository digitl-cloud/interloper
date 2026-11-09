import datetime as dt
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.connector.schemas import Ads, AdSets, AdsStats, Advertisers, Campaigns, CampaignsStats

logger = logging.getLogger(__name__)


# -- HELPERS -------------------------------------------------------------------
async def _statistics_report(
    connection: CriteoConnection,
    advertiser_id: str,
    start_date: dt.date,
    end_date: dt.date,
    dimensions: list[str],
    metrics: list[str] = constants.STATISTICS_METRICS,
    currency: str = "EUR",
) -> list[dict]:
    """POST a Criteo statistics report and return its rows."""
    response = await connection.client.post(
        f"/{constants.API_VERSION}/statistics/report",
        json={
            "advertiserIds": advertiser_id,
            "dimensions": dimensions,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "currency": currency,
            "metrics": metrics,
            "format": "json",
        },
    )
    response.raise_for_status()
    return response.json()["Rows"]


def _snapshot_rows(resources: list[dict[str, Any]], date: dt.date) -> list[dict[str, Any]]:
    """Unwrap Criteo's resources into rows, each stamped with the snapshot's day.

    Used by every entity asset: Criteo wraps each entity as ``{id, type, attributes}``,
    so a row is the attributes beside the resource's own fields. Campaigns and ads
    repeat the resource's id among their attributes, with the same value.

    Args:
        resources: The ``data`` list of a Criteo response, or the concatenated pages of one.
        date: The snapshot's day, stamped on every row as ``date``.

    Returns:
        One row per resource.
    """
    rows = []
    for resource in resources:
        fields = {key: value for key, value in resource.items() if key != "attributes"}
        rows.append({**(resource.get("attributes") or {}), **fields, "date": date})
    return rows


async def _search(connection: CriteoConnection, entities: str, advertiser_id: str) -> list[dict[str, Any]]:
    """Search one advertiser's campaigns or ad sets; Criteo returns every match in one response.

    Used by the campaigns and ad sets assets, the two entities Criteo lists through a search.

    Args:
        connection: The Criteo connection.
        entities: The entity path segment: ``campaigns`` or ``ad-sets``.
        advertiser_id: The advertiser whose entities are listed.

    Returns:
        The response's resources.
    """
    response = await connection.client.post(
        f"/{constants.API_VERSION}/marketing-solutions/{entities}/search",
        json={"filters": {"advertiserIds": [advertiser_id]}},
    )
    response.raise_for_status()
    return response.json().get("data") or []


# Entity resources nest objects up to four levels deep (an ad set's targeting rules); lists are kept as JSON.
_ENTITY_NORMALIZER = DataFrameNormalizer(flatten_max_level=4, drop_na_columns=True)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:criteo",
    normalizer=DataFrameNormalizer(
        snake_case_digits=True,
        column_overrides={
            "OmnichannelRoasPc30d": "omni_channel_roas_pc_30d",
            "OmnichannelRevenuePc30d": "omni_channel_revenue_pc_30d",
            "OmnichannelSalesPc30d": "omni_channel_sales_pc_30d",
            "OmnichannelsalesClientAttribution": "omnichannel_sales_client_attribution",
        },
    ),
)
class Criteo(il.Source):
    """Criteo advertising platform integration."""

    connection: CriteoConnection

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="id",
        description="Criteo advertiser account",
        discriminator=True,
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance statistics with daily breakdowns."""
        return await _statistics_report(
            self.connection,
            advertiser_id=self.advertiser_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
            dimensions=["Day", "AdId", "AdsetId", "CampaignId"],
        )

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign-level performance statistics with daily breakdowns."""
        return await _statistics_report(
            self.connection,
            advertiser_id=self.advertiser_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
            dimensions=["Day", "CampaignId"],
        )

    @il.asset(schema=Advertisers, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def advertisers(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The source's advertiser with its name, as Criteo lists it among the advertisers the connection sees."""
        response = await self.connection.client.get(f"/{constants.API_VERSION}/advertisers/me")
        response.raise_for_status()
        resources = [item for item in response.json().get("data") or [] if str(item["id"]) == self.advertiser_id]
        return _snapshot_rows(resources, context.partition_date)

    @il.asset(
        schema=Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns of the advertiser with their goal, spend limit and budget automation."""
        resources = await _search(self.connection, "campaigns", self.advertiser_id)
        return _snapshot_rows(resources, context.partition_date)

    @il.asset(
        schema=AdSets,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_sets(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ad sets of the advertiser with their schedule, status, budget, bidding and targeting."""
        resources = await _search(self.connection, "ad-sets", self.advertiser_id)
        return _snapshot_rows(resources, context.partition_date)

    @il.asset(
        schema=Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ads of the advertiser with their ad set, creative and dates."""
        pages = self.connection.client.paginate(
            f"/{constants.API_VERSION}/marketing-solutions/advertisers/{self.advertiser_id}/ads",
            il.OffsetPaginator(limit=50, data_selector="data"),
            data_selector="data",
        )
        resources = [resource async for page in pages for resource in page or []]
        return _snapshot_rows(resources, context.partition_date)
