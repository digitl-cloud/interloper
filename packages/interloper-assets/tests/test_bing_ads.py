"""Regression tests for the BingAds source configuration.

The Bing Ads report comes back with PascalCase CSV headers
(``AccountName``, ``TimePeriod``, ``Ctr``, …) while the ``Ads``
schema is snake_case. A source-level ``DataFrameNormalizer`` bridges the
two — these tests pin that the normalizer reaches every asset instance,
survives the host→child spec round-trip, and that a raw API-shaped row
normalizes and validates against the schema (the same chain that broke
in prod for AmazonAds; see ``test_amazon_ads.py``).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.schemas import Ads
from interloper_assets.bing_ads.source import BingAds


def _source() -> Any:
    return BingAds(id="src-1")


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "ads")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == "ads")

        assert isinstance(child_asset.normalizer, DataFrameNormalizer)

    def test_pascalcase_report_row_conforms_after_roundtrip(self):
        """A raw API-shaped row must normalize and validate against the schema."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "ads")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == "ads")

        # One row with every requested report column, as Bing returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = {col: None for col in constants.AD_PERFORMANCE_FIELDS}
        row["TimePeriod"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        representation_for(normalized).conformer.validate(normalized, Ads)  # must not raise
