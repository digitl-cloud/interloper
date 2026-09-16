"""Regression tests for the AmazonAds source configuration.

Prod incident (run d0aebe6d, 2026-06-10): the source-level
``DataFrameNormalizer`` never reached asset instances, so raw camelCase
report rows hit schema validation and every report asset failed with
"Field required" errors. Two distinct defects were involved:

1. ``@il.source(normalizer=...)`` field args were silently dropped on
   class-based sources (``build_class`` setattr on a built
   pydantic class).
2. The host→child DAG-spec round-trip dumped the normalizer as a bare
   dict, degrading ``DataFrameNormalizer`` to the base ``Normalizer``.

These tests pin the whole chain: live instance → spec JSON → reconstructed
child asset → normalize+validate a camelCase report row.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.asset import Asset
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper.schema import Schema
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants, schemas
from interloper_assets.amazon_ads.source import AmazonAds


def _source() -> Any:
    return AmazonAds(id="src-1", profile_id="123")


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)
        assert src.normalizer.snake_case_digits is True
        assert src.normalizer.flatten_max_level == 1

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "products_campaigns_stats")
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 1
        assert normalizer.column_overrides == {
            "eCPAddToCart": "ecp_add_to_cart",
            "eCPBrandSearch": "ecp_brand_search",
        }

    def test_camelcase_report_row_conforms_after_roundtrip(self):
        """A raw API-shaped row must normalize and validate against the schema."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "products_campaigns_stats")
        assert isinstance(child_asset, Asset)

        # One row with every requested report column, as Amazon returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = dict.fromkeys(constants.PRODUCTS_CAMPAIGN_METRICS)
        row["date"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        Representation.of(normalized).reconcile(schemas.ProductsCampaignsStats)  # must not raise


# Count metrics: whole numbers only across the 20 M legacy rows of the Swarovski warehouse (checked
# 2026-09-16), so the schemas type them as ``int``. Rates and amounts keep ``float``.
COUNT_FIELDS = frozenset({
    "impressions", "clicks", "purchases", "purchases_clicks", "units_sold", "units_sold_clicks",
    "purchases_1d", "purchases_7d", "purchases_14d", "purchases_30d",
    "units_sold_clicks_1d", "units_sold_clicks_7d", "units_sold_clicks_14d", "units_sold_clicks_30d",
    "units_sold_same_sku_1d", "units_sold_same_sku_7d", "units_sold_same_sku_14d", "units_sold_same_sku_30d",
    "purchases_same_sku_1d", "purchases_same_sku_7d", "purchases_same_sku_14d", "purchases_same_sku_30d",
    "new_to_brand_purchases", "new_to_brand_units_sold", "detail_page_views", "add_to_cart", "branded_searches",
    "viewable_impressions", "video_complete_views", "video_unmutes", "cumulative_reach",
    "gross_impressions", "invalid_impressions", "gross_click_throughs", "invalid_click_throughs",
})
FLOAT_SUFFIXES = (
    "_rate", "_percentage", "cost", "sales", "spend", "bid", "amount", "roas_clicks_7d", "roas_clicks_14d",
)


def _stats_schemas() -> list[type[Schema]]:
    import inspect

    return [
        cls
        for name, cls in inspect.getmembers(schemas, inspect.isclass)
        if issubclass(cls, Schema) and cls is not Schema and name.endswith("Stats")
    ]


class TestCountMetricTypes:
    """Count metrics are integers; rates and amounts stay floats."""

    def test_count_metrics_are_int_everywhere(self):
        wrong = [
            f"{cls.__name__}.{name}"
            for cls in _stats_schemas()
            for name, info in cls.model_fields.items()
            if name in COUNT_FIELDS and info.annotation != (int | None)
        ]
        assert not wrong, wrong

    def test_rates_and_amounts_stay_float(self):
        wrong = [
            f"{cls.__name__}.{name}"
            for cls in _stats_schemas()
            for name, info in cls.model_fields.items()
            if name.endswith(FLOAT_SUFFIXES) and info.annotation == (int | None)
        ]
        assert not wrong, wrong

    def test_budget_rule_name_is_a_string(self):
        field = schemas.ProductsCampaignsStats.model_fields["campaign_applicable_budget_rule_name"]
        assert field.annotation == (str | None)

    def test_float_valued_counts_reconcile_to_int(self):
        """Amazon may serialise a count as ``3.0``; conform must land it as an integer column."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")
        row: dict[str, object] = dict.fromkeys(constants.PRODUCTS_CAMPAIGN_METRICS)
        row.update({"date": "2026-06-10", "impressions": 3.0, "clicks": 2, "cost": 1.5})
        normalizer = asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(pd.DataFrame([row]))
        out = Representation.of(normalized).reconcile(schemas.ProductsCampaignsStats)
        assert pd.api.types.is_integer_dtype(out["impressions"]) and int(out.loc[0, "impressions"]) == 3
        assert pd.api.types.is_float_dtype(out["cost"])
