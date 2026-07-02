"""Regression tests for the Awin source configuration.

Awin returns camelCase fields, and transactions embed money as nested
``{"amount", "currency"}`` objects (``commissionAmount``, ``saleAmount``, …),
click references as ``{"clickRef": …}``, and transaction parts as a list. A
plain ``DataFrameNormalizer`` snake-cases the publisher report; transactions
need a custom ``AwinTransactionsNormalizer`` that splits the money objects into
``*_amount`` / ``*_currency`` scalars first — otherwise the dict-valued cell
reaches reconcile and fails to cast to ``float`` (observed live). These tests
pin the reshape, that the normalizer survives the host→child spec round-trip,
and that a raw API-shaped row reconciles against the schema.
"""

from __future__ import annotations

from typing import Any

from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.awin.schemas import Transactions
from interloper_assets.awin.source import Awin, AwinTransactionsNormalizer, _reshape_transaction


def _source() -> Any:
    return Awin(id="src-1", advertiser_id="12345")  # ty: ignore[unknown-argument]


class TestSourceNormalizer:
    def test_transactions_uses_the_custom_normalizer(self):
        asset = next(a for a in _source().assets if type(a).key == "transactions")
        assert isinstance(asset.normalizer, AwinTransactionsNormalizer)

    def test_publishers_uses_the_plain_normalizer(self):
        asset = next(a for a in _source().assets if type(a).key == "publishers_stats")
        assert isinstance(asset.normalizer, DataFrameNormalizer)
        assert not isinstance(asset.normalizer, AwinTransactionsNormalizer)


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


class TestSpecRoundtripAndReconcile:
    """Normalizer survives the host→child spec round-trip and a raw row reconciles."""

    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_transaction_row_reconciles_against_schema(self):
        child = self._child("transactions")
        assert isinstance(child.normalizer, AwinTransactionsNormalizer)

        rows = [
            {
                "id": 123,
                "commissionAmount": {"amount": 120.0, "currency": "EUR"},
                "saleAmount": {"amount": 5842.86, "currency": "EUR"},
                "clickRefs": {"clickRef": "ref-1"},
                "transactionParts": [{"commissionGroupId": 1, "amount": 5842.86}],
                "date": "2026-06-09",
            }
        ]
        normalized = child.normalizer.normalize(rows)
        assert "commission_amount" in normalized.columns
        reconciled = representation_for(normalized).conformer.reconcile(normalized, Transactions)
        assert float(reconciled.loc[0, "commission_amount"]) == 120.0
        assert reconciled.loc[0, "commission_currency"] == "EUR"
