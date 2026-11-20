from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from interloper_pandas import DataFrameNormalizer

if TYPE_CHECKING:
    import pandas as pd

# camelCase money object -> (amount key, currency key); both snake-case onto the schema.
_MONEY_FIELDS = {
    "commissionAmount": ("commissionAmount", "commissionCurrency"),
    "saleAmount": ("saleAmount", "saleCurrency"),
    "oldCommissionAmount": ("oldCommissionAmount", "oldCommissionCurrency"),
    "oldSaleAmount": ("oldSaleAmount", "oldSaleCurrency"),
}


def _reshape_transaction(row: dict) -> dict:
    """Flatten Awin's nested money/click-ref/parts objects into scalar columns.

    Args:
        row: One transaction as the API returned it; reshaped in place.

    Returns:
        The same row, its money objects split into amount and currency, the click
        ref lifted and the transaction parts JSON-encoded.
    """
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
