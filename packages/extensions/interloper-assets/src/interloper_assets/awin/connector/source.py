import json
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.connector.schemas import PublishersStats, Transactions

# -- NORMALIZER ----------------------------------------------------------------

# camelCase money object -> (amount key, currency key); both snake-case onto the schema.
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
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        if isinstance(data, list):
            data = [_reshape_transaction(dict(row)) for row in data]
        return super().normalize(data)


# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["Affiliate"],
    icon="icon:awin",
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class Awin(il.Source):
    """Awin affiliate network integration for transaction and publisher reporting."""

    connection: AwinConnection

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="advertiser_id",
        description="Awin advertiser account",
        discriminator=True,
    )

    @il.asset(
        schema=Transactions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
        normalizer=AwinTransactionsNormalizer(),
    )
    async def transactions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Advertiser transactions including commissions, sales amounts, and click attribution."""
        rows = await self.connection.api.rows(
            f"/advertisers/{self.advertiser_id}/transactions/",
            context.partition_date,
            timezone="UTC",
            showBasketProducts=True,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=PublishersStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def publishers_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Advertiser performance reports aggregated by publisher."""
        rows = await self.connection.api.report(
            f"/advertisers/{self.advertiser_id}/reports/publisher",
            context.partition_date,
            timezone="UTC",
        )
        return [{**row, "date": context.partition_date} for row in rows]
