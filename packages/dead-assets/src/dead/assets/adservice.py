import datetime as dt
import logging
from base64 import b64encode
from collections.abc import Sequence
from typing import Any

import httpx

from dead.core.asset import Asset, asset
from dead.core.sentinel import Env
from dead.core.source import source

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
        logger.info(f"Fetching {report_type} report...")

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

        logger.info(f"{report_type} Report fetched")
        return response.json()

    @asset
    def campaigns(
        date: dt.date,
    ) -> Any:
        response = get_report(
            start_date=date,
            end_date=date,
            report_type="statistics",
            group_by="stamp,camp_id",
            end_group="stamp",
            sales_amount=1,
        )
        print(response["data"]["rows"])
        return response["data"]["rows"]

    return (campaigns,)
