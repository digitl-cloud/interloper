import datetime as dt
from collections.abc import Sequence
from typing import Any

import httpx
import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer

from interloper_assets.awin.schemas.advertiser_by_publisher import AdvertiserByPublishers
from interloper_assets.awin.schemas.advertiser_transactions import AdvertiserTransactions

# Notes:
# - Even though the API supports start and end date, the data is aggregated over the entire date range,
#   so we don't use DateWindow to partition the data.


class AwinNormalizer(DataframeNormalizer):
    def __init__(self):
        super().__init__(
            max_level=1,
            # convert_object_columns_to_string=True,
        )

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            super()
            .normalize(df)
            .rename(
                columns={
                    "commission_amount_amount": "commission_amount",
                    "old_commission_amount_amount": "old_commission_amount",
                    "commission_amount_currency": "commission_currency",
                    "old_commission_amount_currency": "old_commission_currency",
                    "sale_amount_amount": "sale_amount",
                    "old_sale_amount_amount": "old_sale_amount",
                    "sale_amount_currency": "sale_currency",
                    "old_sale_amount_currency": "old_sale_currency",
                    "click_refs_click_ref": "click_ref",
                }
            )
        )


@itlp.source(normalizer=AwinNormalizer())
def awin(
    access_token: str = itlp.Env("AWIN_ACCESS_TOKEN"),
) -> Sequence[itlp.Asset]:
    client = httpx.Client(
        base_url="https://api.awin.com",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    @itlp.asset(
        schema=AdvertiserByPublishers,
        partition_strategy=itlp.TimePartitionStrategy(column="date"),
    )
    def advertiser_by_publisher(
        advertiser_id: str,
        date: dt.date = itlp.Date(),
    ) -> Any:
        response = client.get(
            f"advertisers/{advertiser_id}/reports/publisher",
            params={
                "startDate": date.isoformat(),
                "endDate": date.isoformat(),  # TODO: should this be `+ "T23:59:59"` ?
                "timezone": "UTC",
            },
        )
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.insert(0, "date", pd.to_datetime(date))
        return df

    @itlp.asset(
        schema=AdvertiserTransactions,
        partition_strategy=itlp.TimePartitionStrategy(column="date"),
    )
    def advertiser_transactions(
        advertiser_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        response = client.get(
            f"advertisers/{advertiser_id}/transactions/",
            params={
                "startDate": date.isoformat() + "T00:00:00",
                "endDate": date.isoformat() + "T23:59:59",
                "timezone": "UTC",
                "showBasketProducts": True,
            },
        )
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.insert(0, "date", pd.to_datetime(date))
        return df

    return (advertiser_by_publisher, advertiser_transactions)
