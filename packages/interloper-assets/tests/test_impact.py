"""Regression tests for the Impact source.

Impact returns PascalCase fields with digit groups (``SubId1`` -> ``sub_id_1``)
that a source-level ``DataFrameNormalizer(snake_case_digits=True)`` maps to the
schema fields; the source stamps a ``date`` partition column. These tests pin
that the normalizer reaches every asset, survives the host→child spec
round-trip, and that a raw API-shaped record conforms to the schema.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.impact import schemas
from interloper_assets.impact.source import Impact, _to_df


def _source() -> Any:
    return Impact(id="src-1", program_id="123")  # ty: ignore[unknown-argument]


class TestSource:
    def test_all_assets_inherit_the_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key

    def test_ten_report_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "actions",
            "action_updates",
            "action_inquiries",
            "actions_by_sku",
            "clicks",
            "performance",
            "performance_by_ad",
            "performance_by_domain",
            "performance_by_shared_id",
            "performance_by_io",
        }


class TestFraming:
    def test_to_df_stamps_date_and_blanks_empty_strings(self):
        df = _to_df([{"Id": "1", "ReferringDomain": ""}], dt.date(2026, 6, 10))
        assert df.loc[0, "date"] == dt.date(2026, 6, 10)
        assert pd.isna(df.loc[0, "ReferringDomain"])  # empty string -> null

    def test_to_df_empty_records(self):
        assert "date" in _to_df([], dt.date(2026, 6, 10)).columns


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_actions_record_conforms_after_roundtrip(self):
        child = self._child("actions")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        # A raw PascalCase Actions record; digit-group field exercises snake_case_digits.
        record = {"Id": "A1", "CampaignId": "123", "SubId1": "x", "Amount": "12.50"}
        df = _to_df([record], dt.date(2026, 6, 10))
        normalized = child.normalizer.normalize(df)
        assert {"id", "campaign_id", "sub_id_1", "amount", "date"} <= set(normalized.columns)
        representation_for(normalized).conformer.reconcile(normalized, schemas.Actions)  # must not raise
