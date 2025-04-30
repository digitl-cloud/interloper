import datetime as dt
import logging

import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer

from interloper_assets.criteo import constants, schemas

logger = logging.getLogger(__name__)


@itlp.source(normalizer=DataframeNormalizer())
def criteo(
    client_id: str = itlp.Env("CRITEO_CLIENT_ID"),
    client_secret: str = itlp.Env("CRITEO_CLIENT_SECRET"),
    refresh_token: str = itlp.Env("CRITEO_REFRESH_TOKEN"),
) -> tuple[itlp.Asset, ...]:
    client = itlp.RESTClient(
        base_url=f"{constants.BASE_URL}/{constants.API_VERSION}",
        auth=itlp.OAuth2RefreshTokenAuth(
            base_url=constants.BASE_URL,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            token_endpoint="/oauth2/token",
        ),
    )

    def get_advertisers_report(advertiser_id: str, start_date: dt.date, end_date: dt.date) -> dict:
        logger.info(f"Fetching advertiser report for advertiser {advertiser_id}...")

        response = client.post(
            url=f"/log-level/advertisers/{advertiser_id}/report",
            json={
                "shouldDisplayProductIds": True,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
        )
        response.raise_for_status()

        logger.info("Advertiser report fetched")
        return response.json()

    def get_placements_report(
        advertiser_id: str,
        start_date: dt.date,
        end_date: dt.date,
        dimensions: list[str] = ["AdsetId"],
        metrics: list[str] = constants.PLACEMENTS_METRICS,
        currency: str = "EUR",
    ) -> dict:
        logger.info(f"Fetching statistics report for advertiser {advertiser_id}...")

        response = client.post(
            url="/placements/report",
            json={
                "data": [
                    {
                        "type": "PlacementReport",
                        "attributes": {
                            "advertiserIds": advertiser_id,
                            "dimensions": dimensions,
                            "startDate": start_date.isoformat(),
                            "endDate": end_date.isoformat(),
                            "currency": currency,
                            "metrics": metrics,
                            "format": "json",
                        },
                    }
                ]
            },
        )
        response.raise_for_status()

        logger.info("Statistics report fetched")
        return response.json()

    def get_statistics_report(
        advertiser_id: str,
        start_date: dt.date,
        end_date: dt.date,
        dimensions: list[str] = ["Day", "AdId"],
        metrics: list[str] = constants.STATISTICS_METRICS,
        currency: str = "EUR",
    ) -> dict:
        logger.info(f"Fetching statistics report for advertiser {advertiser_id}...")

        response = client.post(
            url="/statistics/report",
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

        logger.info("Statistics report fetched")
        return response.json()

    @itlp.asset
    def advertisers() -> pd.DataFrame:
        response = client.get("/advertisers/me")
        response.raise_for_status()
        return pd.DataFrame(response.json()["data"])

    @itlp.asset(
        schema=schemas.Ads,
        partitioning=itlp.TimePartitionConfig(column="day"),
    )
    def ads(
        advertiser_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        response = get_statistics_report(
            advertiser_id=advertiser_id,
            start_date=date,
            end_date=date,
            dimensions=["Day", "AdId", "AdsetId", "CampaignId"],
        )
        return pd.DataFrame(response["Rows"])

    @itlp.asset(
        schema=schemas.Campaigns,
        partitioning=itlp.TimePartitionConfig(column="day"),
    )
    def campaigns(
        advertiser_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        response = get_statistics_report(
            advertiser_id=advertiser_id,
            start_date=date,
            end_date=date,
            dimensions=["Day", "CampaignId"],
        )
        return pd.DataFrame(response["Rows"])

    return (advertisers, ads, campaigns)
