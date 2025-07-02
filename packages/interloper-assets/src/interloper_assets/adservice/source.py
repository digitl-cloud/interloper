import datetime as dt
import logging
from base64 import b64encode
from collections.abc import Sequence

import httpx
import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer

from interloper_assets.adservice.schemas.campaigns import Campaigns

logger = logging.getLogger(__name__)


@itlp.source(normalizer=DataframeNormalizer())
def adservice(
    api_key: str = itlp.Env("ADSERVICE_API_KEY"),
) -> Sequence[itlp.Asset]:
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

    @itlp.asset(
        schema=Campaigns,
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def campaigns(date: dt.date = itlp.Date()) -> pd.DataFrame:
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

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def conversions(date: dt.date = itlp.Date()) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="conversions",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def conversions_time_of_day(date: dt.date = itlp.Date()) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics/conversions/timeofday",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def campaigns_by_city(date: dt.date = itlp.Date()) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics/devicedetails",
            group_by="city",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def campaigns_by_browser(date: dt.date = itlp.Date()) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics/devicedetails",
            group_by="browser",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig("date"),
    )
    def campaigns_by_device_type(date: dt.date = itlp.Date()) -> pd.DataFrame:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics/devicedetails",
            group_by="device_type",
        )
        data = response["data"]
        return pd.DataFrame(data)

    return (
        campaigns,
        conversions,
        conversions_time_of_day,
        campaigns_by_city,
        campaigns_by_browser,
        campaigns_by_device_type,
    )
