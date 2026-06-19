import datetime as dt
import json
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.schemas import PublishersStats, Transactions

# ------------------------------------------------------------------
# NORMALIZER
# ------------------------------------------------------------------

# camelCase money object -> (amount key, currency key) — both snake-case onto the schema.
_MONEY_FIELDS = {
    "commissionAmount": ("commissionAmount", "commissionCurrency"),
    "saleAmount": ("saleAmount", "saleCurrency"),
    "oldCommissionAmount": ("oldCommissionAmount", "oldCommissionCurrency"),
    "oldSaleAmount": ("oldSaleAmount", "oldSaleCurrency"),
}


def _reshape_transaction(row: dict) -> dict:
    """Flatten Awin's nested money/click-ref/parts objects into scalar columns."""
    for src, (amount_key, currency_key) in _MONEY_FIELDS.items():
        obj = row.pop(src, None)
        if isinstance(obj, dict):
            row[amount_key] = obj.get("amount")
            row[currency_key] = obj.get("currency")
    click_refs = row.pop("clickRefs", None)
    if isinstance(click_refs, dict):
        row["clickRef"] = click_refs.get("clickRef")
    parts = row.get("transactionParts")
    if isinstance(parts, (list, dict)):
        row["transactionParts"] = json.dumps(parts)
    return row


class AwinTransactionsNormalizer(DataFrameNormalizer):
    """Reshape Awin transaction rows onto the flat ``Transactions`` schema.

    Awin embeds money as ``{"amount", "currency"}`` objects (``commissionAmount``,
    ``saleAmount``, …), click references as ``{"clickRef": …}``, and transaction
    parts as a list. Split each money object into ``*_amount`` / ``*_currency``
    scalars, lift the click ref, and JSON-encode the parts list so the rows cast
    cleanly; then defer to the generic snake-case pass.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        if isinstance(data, list):
            data = [_reshape_transaction(dict(row)) for row in data]
        return super().normalize(data)


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
    # Awin returns camelCase fields (publisherId, commissionAmount, …); snake-case them.
    normalizer=DataFrameNormalizer(),
)
class Awin(il.Source):
    """Awin affiliate network integration for transaction and publisher reporting."""

    @il.asset(
        schema=Transactions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
        normalizer=AwinTransactionsNormalizer(),
    )
    def transactions(self, context: il.ExecutionContext, connection: AwinConnection) -> list[dict[str, Any]]:
        """Advertiser transactions including commissions, sales amounts, and click attribution."""
        data = get_advertiser_transactions(
            client=connection.client,
            advertiser_id=connection.publisher_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return [{**row, "date": context.partition_date} for row in data]

    @il.asset(
        schema=PublishersStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def publishers_stats(self, context: il.ExecutionContext, connection: AwinConnection) -> list[dict[str, Any]]:
        """Advertiser performance reports aggregated by publisher."""
        data = get_advertiser_reports_by_publisher(
            client=connection.client,
            advertiser_id=connection.publisher_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return [{**row, "date": context.partition_date} for row in data]
