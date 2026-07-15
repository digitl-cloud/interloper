"""Regression tests for the AmazonSellingPartner source configuration.

Vendor reports arrive as nested camelCase objects (money as
``{amount, currencyCode}``, inventory cost nested two levels deep). The
source-level ``DataFrameNormalizer`` must reach every asset instance — and
survive the host→child DAG-spec round-trip — so those rows flatten and
snake-case onto the flat schema fields instead of failing validation.

These tests pin the whole chain: live instance → spec JSON → reconstructed
child asset → normalize + validate a camelCase report row.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_selling_partner import schemas
from interloper_assets.amazon_selling_partner.source import AmazonSellingPartner

_ASSET_KEY = "vendor_inventory_retail_manufacturing_stats"


def _source() -> Any:
    return AmazonSellingPartner(id="src-1", marketplace="A1PA6795UKMFR9")  # ty: ignore[unknown-argument]


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)
        assert src.normalizer.snake_case_digits is True
        assert src.normalizer.flatten_max_level == 3

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 3

    def test_nested_camelcase_report_row_conforms_after_roundtrip(self):
        """A nested API-shaped inventory row must flatten, snake-case, and validate."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        # Nested money objects (cost two levels deep) and a digit-prefixed key,
        # exactly as GET_VENDOR_INVENTORY_REPORT returns them.
        row: dict[str, Any] = {
            "startDate": "2026-06-10",
            "endDate": "2026-06-10",
            "asin": "B0TEST0001",
            "sellableOnHandInventory": {"cost": {"amount": 500.0, "currencyCode": "EUR"}, "units": 40},
            "aged90PlusDaysSellableInventory": {"cost": {"amount": 10.0, "currencyCode": "EUR"}, "units": 1},
            "sellThroughRate": 0.5,
            "vendorConfirmationRate": 0.9,
            "uft": 3,
        }
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        normalized = normalizer.normalize(df)

        # The two-level nesting and aged90Plus digit prefix land on schema fields.
        assert "sellable_on_hand_inventory_cost_amount" in normalized.columns
        assert "aged_90_plus_days_sellable_inventory_cost_amount" in normalized.columns

        conformer = Representation.of(normalized).conformer
        conformer.validate(normalized, schemas.VendorInventoryRetailManufacturingStats)  # must not raise
