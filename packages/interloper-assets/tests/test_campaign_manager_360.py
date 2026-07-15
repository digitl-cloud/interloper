"""Regression tests for the CampaignManager360 source.

CM360 report downloads are CSV files with a metadata preamble (ending at a
"Report Fields" line), human-readable headers, and a trailing "Grand Total:"
summary row. The custom normalizer must map those headers onto the schema
columns — notably spelling ``%`` out as ``pct`` so "% Viewable Impressions"
does not collide with "Viewable Impressions" — and survive the host→child
DAG-spec round-trip.
"""

from __future__ import annotations

import math
from io import BytesIO
from typing import Any

from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation

from interloper_assets.campaign_manager_360 import schemas
from interloper_assets.campaign_manager_360.source import (
    CampaignManager360,
    CampaignManager360Normalizer,
    _parse_report_csv,
)

_ASSET_KEY = "campaigns_stats"

_REPORT_FILE = b"""Report Name,report_standard_2026-07-10_2026-07-10
Date Range,2026-07-10 - 2026-07-10

Report Fields
Date,Advertiser ID,Campaign,Active View: Viewable Impressions,Active View: % Viewable Impressions,Impressions,Clicks
2026-07-10,123,Campaign A,90,85.5,100,5
2026-07-10,123,Campaign B,-,-,200,10
Grand Total:,,,90,85.5,300,15
"""


def _source() -> Any:
    return CampaignManager360(id="src-1", profile_id="111", account_id="222")  # ty: ignore[unknown-argument]


def _normalizer() -> CampaignManager360Normalizer:
    return CampaignManager360Normalizer(snake_case_digits=True, flatten_max_level=2)


class TestCsvParsing:
    """The preamble is skipped and the trailing totals row dropped."""

    def test_parse_report_csv(self):
        rows = _parse_report_csv(BytesIO(_REPORT_FILE))
        assert len(rows) == 2
        assert rows[0]["Date"] == "2026-07-10"
        assert rows[0]["Impressions"] == 100
        assert rows[1]["Campaign"] == "Campaign B"

    def test_dash_reads_as_missing(self):
        rows = _parse_report_csv(BytesIO(_REPORT_FILE))
        assert math.isnan(rows[1]["Active View: Viewable Impressions"])

    def test_report_without_totals_keeps_all_rows(self):
        content = _REPORT_FILE.replace(b"Grand Total:,,,90,85.5,300,15\n", b"")
        assert len(_parse_report_csv(BytesIO(content))) == 2


class TestHeaderNormalization:
    """CSV headers land on schema columns; ``%`` becomes ``pct``, digits split."""

    def test_percent_headers_do_not_collide(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Active View: % Viewable Impressions") == (
            "active_view_pct_viewable_impressions"
        )
        assert normalizer.column_name("Active View: Viewable Impressions") == "active_view_viewable_impressions"

    def test_digit_and_symbol_headers(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Site (CM360)") == "site_cm_360"
        assert normalizer.column_name("DV360 Cost (USD)") == "dv_360_cost_usd"
        assert normalizer.column_name("Package/Roadblock ID") == "package_roadblock_id"
        assert normalizer.column_name("TrueView: Views") == "true_view_views"

    def test_parsed_report_conforms_to_schema(self):
        rows = _parse_report_csv(BytesIO(_REPORT_FILE))
        normalized = _normalizer().normalize(rows)
        conformer = Representation.of(normalized).conformer
        df = conformer.reconcile(normalized, schemas.CampaignsStats)
        conformer.validate(df, schemas.CampaignsStats)  # must not raise
        record = df.to_dict("records")[0]
        assert record["active_view_pct_viewable_impressions"] == 85.5
        assert record["active_view_viewable_impressions"] == 90


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 4
        for asset in src.assets:
            assert isinstance(asset.normalizer, CampaignManager360Normalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, CampaignManager360Normalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 2
        # The % -> pct behavior must survive in the child pod.
        assert normalizer.column_name("Active View: % Viewable Impressions") == (
            "active_view_pct_viewable_impressions"
        )
