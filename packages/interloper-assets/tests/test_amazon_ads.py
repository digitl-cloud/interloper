"""Regression tests for the AmazonAds source configuration.

Prod incident (run d0aebe6d, 2026-06-10): the source-level
``DataFrameNormalizer`` never reached asset instances, so raw camelCase
report rows hit schema validation and every report asset failed with
"Field required" errors. Two distinct defects were involved:

1. ``@il.source(normalizer=...)`` field args were silently dropped on
   class-based sources (``build_component_class`` setattr on a built
   pydantic class).
2. The host→child DAG-spec round-trip dumped the normalizer as a bare
   dict, degrading ``DataFrameNormalizer`` to the base ``Normalizer``.

These tests pin the whole chain: live instance → spec JSON → reconstructed
child asset → normalize+validate a camelCase report row.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.conformer import conformer_for
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants, schemas
from interloper_assets.amazon_ads.source import AmazonAds


def _source() -> Any:
    return AmazonAds(id="src-1", profile_id="123")  # ty: ignore[unknown-argument]


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
        asset = next(a for a in src.assets if type(a).key == "products_campaigns")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == "products_campaigns")

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
        asset = next(a for a in src.assets if type(a).key == "products_campaigns")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == "products_campaigns")

        # One row with every requested report column, as Amazon returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = {col: None for col in constants.PRODUCTS_CAMPAIGN_METRICS}
        row["date"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        conformer_for(normalized).validate(normalized, schemas.ProductsCampaigns)  # must not raise
