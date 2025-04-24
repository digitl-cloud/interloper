import datetime as dt
import gzip
import json
import logging
from enum import Enum
from functools import partial

import httpx
import interloper as itlp
import pandas as pd
from interloper_pandas.normalizer import DataframeNormalizer
from tenacity import (
    Retrying,
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
    wait_incrementing,
)

from interloper_assets.amazon_selling_partner import constants

logger = logging.getLogger(__name__)


class AmazonSellingPartnerLocation(Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://sellingpartnerapi-eu.amazon.com",
            AmazonSellingPartnerLocation.FAR_EAST: "https://sellingpartnerapi-fe.amazon.com",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://sellingpartnerapi-na.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://api.amazon.co.uk",
            AmazonSellingPartnerLocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


class AmazonSellingPartnerMarketplace(Enum):
    # NORTH AMERICA
    CANADA = "A2EUQ1WTGCTBG2"
    UNITED_STATES = "ATVPDKIKX0DER"
    MEXICO = "A1AM78C64UM0Y8"
    BRAZIL = "A2Q3Y263D00KWC"

    # EUROPE
    BELGIUM = "AMEN7PMS3EDWL"
    CHINA = "AAHKV2X7AFYLW"
    EGYPT = "ARBP9OOSHTCHU"
    FRANCE = "A13V1IB3VIYZZH"
    GERMANY = "A1PA6795UKMFR9"
    INDIA = "A21TJRUUN4KGV"
    ITALY = "APJ6JRA9NG5V4"
    NETHERLANDS = "A1805IZSGTT6HS"
    POLAND = "A1C3SOZRARQ6R3"
    SAUDI_ARABIA = "A17E79C6D8DWNP"
    SOUTH_AFRICA = "AE08WJ6YKNBMC"
    SPAIN = "A1RKKUPIHCS9HS"
    SWEDEN = "A2NODRKZP88ZB9"
    TAIWAN = "A2NODRKZP88ZB9"
    TURKEY = "A33AVAJ2PDY3EV"
    UNITED_ARAB_EMIRATES = "A2VIGQ35RCS4UG"
    UNITED_KINGDOM = "A1F83G8C2ARO7P"

    # FAR EAST
    AUSTRALIA = "A39IBJ37TRP1C6"
    JAPAN = "A1VC38T7YXB528"
    SINGAPORE = "A19VAU5U5O7RUS"

    @property
    def location(self) -> AmazonSellingPartnerLocation:
        return {
            AmazonSellingPartnerMarketplace.CANADA: AmazonSellingPartnerLocation.NORTH_AMERICA,
            AmazonSellingPartnerMarketplace.UNITED_STATES: AmazonSellingPartnerLocation.NORTH_AMERICA,
            AmazonSellingPartnerMarketplace.MEXICO: AmazonSellingPartnerLocation.NORTH_AMERICA,
            AmazonSellingPartnerMarketplace.BRAZIL: AmazonSellingPartnerLocation.NORTH_AMERICA,
            AmazonSellingPartnerMarketplace.BELGIUM: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.CHINA: AmazonSellingPartnerLocation.FAR_EAST,
            AmazonSellingPartnerMarketplace.EGYPT: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.FRANCE: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.GERMANY: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.INDIA: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.ITALY: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.NETHERLANDS: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.POLAND: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.SAUDI_ARABIA: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.SOUTH_AFRICA: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.SPAIN: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.SWEDEN: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.TAIWAN: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.TURKEY: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.UNITED_ARAB_EMIRATES: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.UNITED_KINGDOM: AmazonSellingPartnerLocation.EUROPE,
            AmazonSellingPartnerMarketplace.AUSTRALIA: AmazonSellingPartnerLocation.FAR_EAST,
            AmazonSellingPartnerMarketplace.JAPAN: AmazonSellingPartnerLocation.FAR_EAST,
            AmazonSellingPartnerMarketplace.SINGAPORE: AmazonSellingPartnerLocation.FAR_EAST,
        }[self]


class AmazonSellingPartnerAuth(itlp.OAuth2RefreshTokenAuth):
    def __call__(self, client: httpx.Client):
        super().__call__(client)
        client.headers.update({"x-amz-access-token": self.access_token})


@itlp.source(normalizer=DataframeNormalizer())
def amazon_selling_partner(
    location: AmazonSellingPartnerLocation | str = itlp.Env("AMAZON_SELLING_PARTNER_LOCATION"),
    client_id: str = itlp.Env("AMAZON_SELLING_PARTNER_CLIENT_ID"),
    client_secret: str = itlp.Env("AMAZON_SELLING_PARTNER_CLIENT_SECRET"),
    refresh_token: str = itlp.Env("AMAZON_SELLING_PARTNER_REFRESH_TOKEN"),
) -> tuple[itlp.Asset, ...]:
    location = AmazonSellingPartnerLocation(location) if isinstance(location, str) else location
    auth = AmazonSellingPartnerAuth(
        base_url=location.auth_url,
        token_endpoint="/auth/o2/token",
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    client = itlp.RESTClient(
        base_url=location.api_url,
        auth=auth,
    )

    def request_report(
        type: str,
        marketplaces: list[AmazonSellingPartnerMarketplace] | list[str],
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        options: dict[str, str] | None = None,
    ) -> dict:
        marketplaces = [
            AmazonSellingPartnerMarketplace(marketplace) if isinstance(marketplace, str) else marketplace
            for marketplace in marketplaces
        ]

        if not all(marketplace.location == location for marketplace in marketplaces):
            raise ValueError("Marketplace location must match the connector location")

        params = {
            "reportType": type,
            "marketplaceIds": [marketplace.value for marketplace in marketplaces],
            "dataStartTime": start_date.strftime("%Y-%m-%dT00:00:00.000Z") if start_date else None,
            "dataEndTime": end_date.strftime("%Y-%m-%dT23:59:59.999Z") if end_date else None,
        }

        if options:
            params.update({"reportOptions": options})

        response = client.post(f"/reports/{constants.REPORT_API_VERSION}/reports", json=params)
        response.raise_for_status()
        return response.json()

    def get_report_status(report_id: str) -> dict:
        response = client.get(url=f"/reports/{constants.REPORT_API_VERSION}/reports/{report_id}")
        response.raise_for_status()
        return response.json()

    def get_report_document(document_id: str) -> dict:
        response = client.get(url=f"/reports/{constants.REPORT_API_VERSION}/documents/{document_id}")
        response.raise_for_status()
        return response.json()

    def download_report(url: str) -> dict:
        response = httpx.get(url, timeout=None)
        response.raise_for_status()

        gzip_content = response.content
        json_content = gzip.decompress(gzip_content)

        return json.loads(json_content)

    @retry(stop=stop_after_attempt(3))
    def _authenticate() -> None:
        client.authenticate()

    def request_and_download_report(
        type: str,
        marketplaces: list[AmazonSellingPartnerMarketplace] | list[str],
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        options: dict[str, str] | None = None,
    ) -> dict | None:
        ConfiguredRetrying = partial(
            Retrying,
            wait=wait_fixed(60),
            stop=stop_after_delay(dt.timedelta(hours=1)),
            before=lambda _: _authenticate(),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True,
        )

        marketplaces = [
            AmazonSellingPartnerMarketplace(marketplace) if isinstance(marketplace, str) else marketplace
            for marketplace in marketplaces
        ]

        failed = False

        # Request report
        for attempt in ConfiguredRetrying(
            wait=wait_incrementing(start=60, increment=60, max=dt.timedelta(minutes=10)),
        ):
            with attempt:
                logger.info(
                    f"Requesting {type} report for marketplaces {[id.name for id in marketplaces]} "
                    f"(attempt {attempt.retry_state.attempt_number})"
                )
                response = request_report(type, marketplaces, start_date, end_date, options)

        report_id = response["reportId"]
        logger.info(f"Report id: {report_id}")

        # Wait for report
        for attempt in ConfiguredRetrying(
            retry=retry_if_result(lambda status: status in ["IN_PROGRESS", "IN_QUEUE"])
            | retry_if_exception(lambda e: issubclass(e.__class__, httpx.HTTPError)),
        ):
            with attempt:
                logger.info(f"Waiting for report {report_id}...")
                response = get_report_status(report_id)
                status = response["processingStatus"]

                if status in ["CANCELLED", "FATAL"]:
                    logger.error(f"Report {report_id} failed with status {response['processingStatus']}")
                    failed = True
                    break

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(status)

        # Error without document ID -> fail fast, no error details available
        if failed and "reportDocumentId" not in response.keys():
            raise Exception(f"Report {report_id} failed with status {response['processingStatus']}")

        document_id = response["reportDocumentId"]
        logger.info(f"Document id: {document_id}")

        # Get document of report
        for attempt in ConfiguredRetrying(stop=stop_after_delay(dt.timedelta(minutes=10))):
            with attempt:
                logger.info(f"Getting document {document_id} (attempt {attempt.retry_state.attempt_number})")
                response = get_report_document(document_id)

        report_document_url = response["url"]
        logger.info(f"Report document URL: {report_document_url}")

        # Download document
        for attempt in ConfiguredRetrying(stop=stop_after_delay(dt.timedelta(minutes=10))):
            with attempt:
                logger.info(f"Downloading document {document_id} (attempt {attempt.retry_state.attempt_number})")
                response = download_report(report_document_url)

        # Error with document ID -> fail with detailed error
        if failed:
            # Do not fail if report data is not yet available
            if "The report data for the requested date range is not yet available" in response["errorDetails"]:
                logger.error(f"Report {report_id} failed: {response['errorDetails']}")
                return None

            raise Exception(f"Report {report_id} failed: {response['errorDetails']}")

        return response

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_traffic(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_TRAFFIC_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={"reportPeriod": "DAY"},
        )
        return pd.DataFrame(data["trafficByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_retail_manufacturing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "RETAIL",
                "distributorView": "MANUFACTURING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_retail_sourcing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "RETAIL",
                "distributorView": "SOURCING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_business_manufacturing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "BUSINESS",
                "distributorView": "MANUFACTURING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_business_sourcing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "BUSINESS",
                "distributorView": "SOURCING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_fresh_manufacturing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "FRESH",
                "distributorView": "MANUFACTURING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_sales_fresh_sourcing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_SALES_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "FRESH",
                "distributorView": "SOURCING",
            },
        )
        return pd.DataFrame(data["salesByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_inventory_retail_manufacturing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_INVENTORY_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "RETAIL",
                "distributorView": "MANUFACTURING",
            },
        )
        return pd.DataFrame(data["inventoryByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_inventory_retail_sourcing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_INVENTORY_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "RETAIL",
                "distributorView": "SOURCING",
            },
        )
        return pd.DataFrame(data["inventoryByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_inventory_fresh_manufacturing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_INVENTORY_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "FRESH",
                "distributorView": "MANUFACTURING",
            },
        )
        return pd.DataFrame(data["inventoryByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_inventory_fresh_sourcing(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_INVENTORY_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={
                "reportPeriod": "DAY",
                "sellingProgram": "FRESH",
                "distributorView": "SOURCING",
            },
        )
        return pd.DataFrame(data["inventoryByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_net_pure_product_margin(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
            marketplaces=[marketplace],
            start_date=date,
            end_date=date,
            options={"reportPeriod": "DAY"},
        )
        return pd.DataFrame(data["netPureProductMarginByAsin"] if data else [])

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def vendor_forecasting_retail(
        marketplace: AmazonSellingPartnerMarketplace,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            type="GET_VENDOR_FORECASTING_REPORT",
            marketplaces=[marketplace],
            options={"sellingProgram": "RETAIL"},
        )
        return pd.DataFrame(data["forecastingByAsin"] if data else [])

    return (
        vendor_traffic,
        vendor_sales_retail_manufacturing,
        vendor_sales_retail_sourcing,
        vendor_sales_business_manufacturing,
        vendor_sales_business_sourcing,
        vendor_sales_fresh_manufacturing,
        vendor_sales_fresh_sourcing,
        vendor_inventory_retail_manufacturing,
        vendor_inventory_retail_sourcing,
        vendor_inventory_fresh_manufacturing,
        vendor_inventory_fresh_sourcing,
        vendor_net_pure_product_margin,
        vendor_forecasting_retail,
    )
