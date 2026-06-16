"""Regression tests for the Criteo source configuration.

The Criteo statistics report returns PascalCase columns (``Day``, ``AdId``,
``SalesAllClientAttribution``, …) while the schemas are snake_case. A
source-level ``DataFrameNormalizer`` bridges the two — these tests pin that
the normalizer reaches every asset instance, survives the host→child spec
round-trip, and that a raw API-shaped row normalizes and validates against
the schema (the same chain that broke in prod for AmazonAds; see
``test_amazon_ads.py``).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.schemas import Ads, Campaigns
from interloper_assets.criteo.source import Criteo

# Raw API column names per asset, as Criteo returns them: the requested id
# dimensions, their companion name/currency columns, and the full metric set.
ADS_COLUMNS = [
    "Day", "AdId", "Ad", "AdsetId", "Adset", "CampaignId", "Campaign", "Currency",
    *constants.STATISTICS_METRICS,
]
CAMPAIGNS_COLUMNS = ["Day", "CampaignId", "Campaign", "Currency", *constants.STATISTICS_METRICS]


def _source() -> Any:
    return Criteo(id="src-1")


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
    """The host→child spec round-trip must preserve the normalizer and conform."""

    def _child_asset(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child_asset("ads")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = {col: None for col in ADS_COLUMNS}
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        representation_for(normalized).conformer.validate(normalized, Ads)  # must not raise

    def test_campaigns_row_conforms_after_roundtrip(self):
        child = self._child_asset("campaigns")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = {col: None for col in CAMPAIGNS_COLUMNS}
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        representation_for(normalized).conformer.validate(normalized, Campaigns)  # must not raise
