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
from interloper.asset import Asset
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_selling_partner import schemas
from interloper_assets.amazon_selling_partner.source import AmazonSellingPartner

_ASSET_KEY = "vendor_inventory_retail_manufacturing_stats"


def _source() -> Any:
    return AmazonSellingPartner(id="src-1", marketplace="A1PA6795UKMFR9")


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
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

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
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

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

        Representation.of(normalized).reconcile(schemas.VendorInventoryRetailManufacturingStats)  # must not raise


class TestForecastingSnapshots:
    """The forecast report is a weekly snapshot: one partition per generation, kept only on its own run."""

    def test_asset_is_enabled_and_partitioned_on_the_generation_date(self):
        asset = next(a for a in _source().assets if type(a).key == "vendor_forecasting_retail_stats")
        assert asset.partitioning is not None and asset.partitioning.column == "forecast_generation_date"
        assert asset.tags == ["Report"]

    def test_run_keeps_only_the_partitions_generation(self, monkeypatch: Any):
        import asyncio
        import datetime as dt

        import interloper as il
        from interloper.asset.context import ExecutionContext

        from interloper_assets.amazon_selling_partner import source as source_module

        rows = [
            {"asin": "A", "forecastGenerationDate": "2026-09-13", "startDate": "2026-09-13"},
            {"asin": "A", "forecastGenerationDate": "2026-09-13", "startDate": "2026-09-20"},
            {"asin": "B", "forecastGenerationDate": "2026-09-06", "startDate": "2026-09-06"},
        ]

        async def fake_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {"forecastByAsin": rows}

        monkeypatch.setattr(source_module, "_get_report", fake_report)
        asset = next(a for a in _source().assets if type(a).key == "vendor_forecasting_retail_stats")

        def run(day: dt.date) -> list[dict[str, Any]]:
            context = ExecutionContext(
                asset_key=asset.key,
                partitioning=asset.partitioning,
                partition_or_window=il.TimePartition(value=day),
            )
            return asyncio.run(asset.data(context=context))

        assert [r["startDate"] for r in run(dt.date(2026, 9, 13))] == ["2026-09-13", "2026-09-20"]
        assert run(dt.date(2026, 9, 14)) == []  # a run between generations writes nothing

    def test_forecast_row_conforms_with_date_typed_columns(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "vendor_forecasting_retail_stats")
        rows = [
            {
                "asin": "B00TEST",
                "startDate": "2026-09-13",
                "endDate": "2026-09-19",
                "forecastGenerationDate": "2026-09-13",
                "meanForecastUnits": 12.5,
                "p70ForecastUnits": 10.0,
                "p80ForecastUnits": 9.0,
                "p90ForecastUnits": 7.0,
            }
        ]
        normalizer = asset.normalizer
        assert normalizer is not None
        out = Representation.of(normalizer.normalize(rows)).reconcile(schemas.VendorForecastingRetailStats)
        assert str(out.loc[0, "forecast_generation_date"])[:10] == "2026-09-13"
        assert set(out.columns) == set(schemas.VendorForecastingRetailStats.model_fields)
