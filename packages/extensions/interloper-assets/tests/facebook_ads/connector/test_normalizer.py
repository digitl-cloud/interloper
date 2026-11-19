"""Tests for ``FacebookActionsNormalizer``, which pivots action lists into one column per action type."""

from __future__ import annotations

from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads.connector.normalizer import FacebookActionsNormalizer


class TestActionsNormalizer:
    """The action-list pivot is the crux of the Facebook port."""

    def test_pivots_actions_into_per_action_type_columns(self):
        rows = [
            {
                "date_start": "2026-06-10",
                "account_id": "123",
                "ad_id": "456",
                # link_click listed twice -> summed into one column.
                "actions": [
                    {"action_type": "link_click", "action_device": "mobile", "value": "5"},
                    {"action_type": "link_click", "action_device": "desktop", "value": "2"},
                    {"action_type": "video_view", "value": "3"},
                ],
                "action_values": [
                    {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "12.5"},
                ],
            }
        ]
        df = FacebookActionsNormalizer().normalize(rows)
        assert df.loc[0, "actions_link_click"] == 7.0  # summed across devices
        assert df.loc[0, "actions_video_view"] == 3.0
        # the "." in the action type is sanitized to "_"
        assert df.loc[0, "action_values_offsite_conversion_fb_pixel_purchase"] == 12.5
        assert "actions" not in df.columns  # original list column dropped

    def test_empty_rows_yield_empty_frame(self):
        assert FacebookActionsNormalizer().normalize([]).empty


def test_isinstance_of_dataframe_normalizer():
    # The custom normalizer is a DataFrameNormalizer, so it inherits its config.
    assert isinstance(FacebookActionsNormalizer(), DataFrameNormalizer)
