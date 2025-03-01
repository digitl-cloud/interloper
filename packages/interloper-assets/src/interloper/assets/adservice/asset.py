import datetime as dt
import logging
from base64 import b64encode
from collections.abc import Sequence

import httpx
import pandas as pd

from interloper.assets.adservice.schema import Campaign
from interloper.core.asset import Asset, asset
from interloper.core.param import ActivePartition, Date, DateWindow, Env, UpstreamAsset
from interloper.core.partitioning import TimePartitionStrategy
from interloper.core.source import source

logger = logging.getLogger(__name__)


@source
def adservice(
    api_key: str = Env("ADSERVICE_API_KEY"),
) -> Sequence[Asset]:
    base_url = "https://api.adservice.com/v2/client"
    credentials = b64encode(f"api:{api_key}".encode()).decode()
    client = httpx.Client(headers={"Authorization": f"Basic {credentials}"})

    def get_report(
        start_date: dt.date,
        end_date: dt.date,
        report_type: str,
        group_by: str | None = None,
        end_group: str | None = None,
        sales_amount: int | None = None,
    ) -> dict:
        response = client.get(
            url=f"{base_url}/{report_type}",
            params={
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "sales_amount": sales_amount,
                "group_by": group_by,
                "end_group": end_group,
            },
        )
        response.raise_for_status()
        return response.json()

    @asset(
        schema=Campaign,
        partition_strategy=TimePartitionStrategy("date"),
    )
    def campaigns(
        date: dt.date = Date(),
    ) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics",
            group_by="stamp,camp_id",
            end_group="stamp",
            sales_amount=1,
        )
        data = response["data"]["rows"]
        return pd.DataFrame(data)

    @asset(
        partition_strategy=TimePartitionStrategy("date", allow_window=True),
    )
    def conversions(
        date: tuple[dt.date, dt.date] = DateWindow(),
        campaigns: pd.DataFrame = UpstreamAsset("campaigns", pd.DataFrame),
    ) -> pd.DataFrame:
        start, end = date
        response = get_report(
            start_date=start,
            end_date=end,
            report_type="conversions",
        )
        data = response["data"]
        return pd.DataFrame(data)

    return (campaigns, conversions)
