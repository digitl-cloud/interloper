"""Tests for ``DisplayVideo360Normalizer``, which maps report headers onto schema columns."""

from __future__ import annotations

from interloper_assets.display_video_360.connector.normalizer import DisplayVideo360Normalizer


class TestHeaderNormalization:
    """Report headers land on schema columns; ``%`` becomes ``pct``, digits split."""

    def test_percent_headers_do_not_collide(self):
        normalizer = DisplayVideo360Normalizer(snake_case_digits=True, flatten_max_level=2)
        assert normalizer.column_name("Active View: % Viewable Impressions") == ("active_view_pct_viewable_impressions")
        assert normalizer.column_name("Active View: Viewable Impressions") == "active_view_viewable_impressions"
        assert normalizer.column_name("CM360 Placement ID") == "cm_360_placement_id"
        assert normalizer.column_name("YouTube Revenue (eCPV) (Adv Currency)") == (
            "you_tube_revenue_e_cpv_adv_currency"
        )
