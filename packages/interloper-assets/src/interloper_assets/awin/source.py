import datetime as dt

import interloper as il
import pandas as pd

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.schemas import Publishers, Transactions

# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------


def get_advertiser_transactions(
    client: il.RESTClient,
    advertiser_id: str,
    start_date: dt.date,
    end_date: dt.date,
) -> list[dict]:
    """Fetch advertiser transactions from the Awin API."""
    response = client.get(
        f"/advertisers/{advertiser_id}/transactions/",
        params={
            "startDate": start_date.isoformat() + "T00:00:00",
            "endDate": end_date.isoformat() + "T23:59:59",
            "timezone": "UTC",
            "showBasketProducts": True,
        },
    )
    response.raise_for_status()
    return response.json()


def get_advertiser_reports_by_publisher(
    client: il.RESTClient,
    advertiser_id: str,
    start_date: dt.date,
    end_date: dt.date,
) -> list[dict]:
    """Fetch advertiser reports aggregated by publisher from the Awin API."""
    response = client.get(
        f"/advertisers/{advertiser_id}/reports/publisher",
        params={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "timezone": "UTC",
        },
    )
    response.raise_for_status()
    return response.json()


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": AwinConnection},
    tags=["Affiliate"],
    icon="icon:awin",
)
class Awin(il.Source):
    """Awin affiliate network integration for transaction and publisher reporting."""

    @il.asset(
        schema=Transactions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def transactions(self, context: il.ExecutionContext, connection: AwinConnection) -> pd.DataFrame:
        """Advertiser transactions including commissions, sales amounts, and click attribution."""
        data = get_advertiser_transactions(
            client=connection.client,
            advertiser_id=connection.publisher_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(
        schema=Publishers,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def publishers(self, context: il.ExecutionContext, connection: AwinConnection) -> pd.DataFrame:
        """Advertiser performance reports aggregated by publisher."""
        data = get_advertiser_reports_by_publisher(
            client=connection.client,
            advertiser_id=connection.publisher_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)
