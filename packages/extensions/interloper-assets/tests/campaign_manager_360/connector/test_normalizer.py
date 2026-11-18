"""Tests for ``CampaignManager360Normalizer``, which maps report headers onto schema columns."""

from __future__ import annotations

from interloper_assets.campaign_manager_360.connector.normalizer import CampaignManager360Normalizer


def _normalizer() -> CampaignManager360Normalizer:
    return CampaignManager360Normalizer(snake_case_digits=True, flatten_max_level=2)


class TestHeaderNormalization:
    """CSV headers land on schema columns; ``%`` becomes ``pct``, digits split."""

    def test_percent_headers_do_not_collide(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Active View: % Viewable Impressions") == ("active_view_pct_viewable_impressions")
        assert normalizer.column_name("Active View: Viewable Impressions") == "active_view_viewable_impressions"

    def test_digit_and_symbol_headers(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Site (CM360)") == "site_cm_360"
        assert normalizer.column_name("DV360 Cost (USD)") == "dv_360_cost_usd"
        assert normalizer.column_name("Package/Roadblock ID") == "package_roadblock_id"
        assert normalizer.column_name("TrueView: Views") == "true_view_views"
