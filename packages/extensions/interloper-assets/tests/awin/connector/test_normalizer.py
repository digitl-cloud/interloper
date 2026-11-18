"""Tests for the reshape ``AwinTransactionsNormalizer`` runs before the snake-case pass."""

from __future__ import annotations

from interloper_assets.awin.connector.normalizer import _reshape_transaction


class TestReshape:
    """Splitting the nested money/click-ref/parts objects is the crux of the port."""

    def test_money_objects_split_into_amount_and_currency(self):
        row = _reshape_transaction(
            {
                "commissionAmount": {"amount": 120.0, "currency": "EUR"},
                "saleAmount": {"amount": 5842.86, "currency": "EUR"},
            }
        )
        assert row["commissionAmount"] == 120.0
        assert row["commissionCurrency"] == "EUR"
        assert row["saleAmount"] == 5842.86
        assert row["saleCurrency"] == "EUR"

    def test_click_ref_is_lifted_and_parts_json_encoded(self):
        row = _reshape_transaction(
            {
                "clickRefs": {"clickRef": "https://ad4mat.com/native-network"},
                "transactionParts": [{"commissionGroupId": 1, "amount": 5842.86}],
            }
        )
        assert row["clickRef"] == "https://ad4mat.com/native-network"
        assert "clickRefs" not in row
        assert isinstance(row["transactionParts"], str)
