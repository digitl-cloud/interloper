import datetime as dt
import logging
from collections.abc import Sequence
from typing import Any

import httpx
import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer

from interloper_assets.adup.schemas.ads import Ads

from . import constants

logger = logging.getLogger(__name__)


@itlp.source(
    normalizer=DataframeNormalizer(),
)
def adup(
    client_id: str = itlp.Env("ADUP_CLIENT_ID"),
    client_secret: str = itlp.Env("ADUP_CLIENT_SECRET"),
) -> Sequence[itlp.Asset]:
    client = httpx.Client()

    def _authenticate() -> None:
        logger.info("Authenticating...")
        response = client.post(
            f"{constants.BASE_URL}/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        response.raise_for_status()
        access_token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {access_token}"
        logger.info("Authenticated")

    def _get_report(report_type: str, start_date: dt.date, end_date: dt.date) -> dict:
        _authenticate()

        logger.info("Fetching report...")
        response = client.post(
            f"{constants.BASE_URL}/reports/v202101/report",
            json={
                "report_name": report_type,
                "report_type": report_type,
                "select": constants.FIELDS[report_type],
                "conditions": [],
                "download_format": "JSON",
                "date_range_type": "CUSTOM_DATE",
                "date_range": {
                    "min": start_date.isoformat(),
                    "max": end_date.isoformat(),
                },
            },
        )
        response.raise_for_status()
        logger.info("Report fetched")
        return response.json()

    @itlp.asset(
        schema=Ads,
        partition_strategy=itlp.TimePartitionStrategy(column="Date", allow_window=True),
    )
    def ads(
        date_window: tuple[dt.date, dt.date] = itlp.DateWindow(),
    ) -> Any:
        response = _get_report("AD_PERFORMANCE_REPORT", date_window[0], date_window[1])
        data = response["rows"]
        return pd.DataFrame(data)

    return (ads,)
