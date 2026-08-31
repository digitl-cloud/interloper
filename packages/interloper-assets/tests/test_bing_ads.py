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

import os
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from interloper.asset import Asset
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer
from suds import WebFault

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.schemas import AdsStats
from interloper_assets.bing_ads.source import BingAds, _translate_soap_fault


def _source() -> Any:
    return BingAds(id="src-1", account_id="123")  # ty: ignore[unknown-argument]


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
        asset = next(a for a in src.assets if type(a).key == "ads_stats")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "ads_stats")
        assert isinstance(child_asset, Asset)

        assert isinstance(child_asset.normalizer, DataFrameNormalizer)

    def test_pascalcase_report_row_conforms_after_roundtrip(self):
        """A raw API-shaped row must normalize and validate against the schema."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "ads_stats")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "ads_stats")
        assert isinstance(child_asset, Asset)

        # One row with every requested report column, as Bing returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = {col: None for col in constants.AD_PERFORMANCE_FIELDS}
        row["TimePeriod"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        Representation.of(normalized).conformer.validate(normalized, AdsStats)  # must not raise


def _web_fault(detail: SimpleNamespace) -> WebFault:
    return WebFault(SimpleNamespace(faultstring="Invalid client data.", detail=detail), document=None)


class TestTranslateSoapFault:
    """The generic 'Invalid client data' fault must be unpacked into its real cause."""

    def test_operation_error_is_surfaced(self):
        fault = _web_fault(
            SimpleNamespace(
                ApiFaultDetail=SimpleNamespace(
                    BatchErrors="",
                    OperationErrors=SimpleNamespace(
                        OperationError=SimpleNamespace(
                            Code="2003",
                            ErrorCode="AccountNotAuthorized",
                            Message="insufficient privileges",
                        )
                    ),
                )
            )
        )
        with pytest.raises(RuntimeError, match="AccountNotAuthorized: insufficient privileges"):
            _translate_soap_fault(fault)

    def test_multiple_batch_errors_are_joined(self):
        fault = _web_fault(
            SimpleNamespace(
                ApiFaultDetail=SimpleNamespace(
                    OperationErrors="",
                    BatchErrors=SimpleNamespace(
                        BatchError=[
                            SimpleNamespace(Code="1", ErrorCode="A", Message="first"),
                            SimpleNamespace(Code="2", ErrorCode="B", Message="second"),
                        ]
                    ),
                )
            )
        )
        with pytest.raises(RuntimeError, match="A: first; B: second"):
            _translate_soap_fault(fault)

    def test_non_webfault_is_left_untouched(self):
        # Returns None (does not raise) so the caller re-raises the original.
        assert _translate_soap_fault(ValueError("boom")) is None


class TestReportingServiceManagerWorkingDirectory:
    """Each manager must get its own working directory.

    The SDK defaults to a shared ``/tmp/BingAdsSDKPython`` created with a racy
    exists-then-makedirs, so concurrent report assets in one pod crash with
    ``FileExistsError`` (prod runs 7c88e1be / 1205e329).
    """

    def test_each_manager_gets_a_fresh_working_directory(self, monkeypatch):
        import bingads.v13.reporting.reporting_service_manager as rsm_module

        captured: list[dict[str, Any]] = []

        class FakeManager:
            def __init__(self, authorization_data: Any, **kwargs: Any):
                captured.append(kwargs)

        monkeypatch.setattr(rsm_module, "ReportingServiceManager", FakeManager)
        monkeypatch.setattr(BingAdsConnection, "authorization_data", lambda self, account_id: SimpleNamespace())

        connection = BingAdsConnection(
            client_id="cid",
            client_secret="secret",
            refresh_token="token",
            developer_token="dev",
        )
        connection.reporting_service_manager("123")
        connection.reporting_service_manager("456")

        dirs = [kwargs["working_directory"] for kwargs in captured]
        assert len(dirs) == 2
        assert dirs[0] != dirs[1]
        for path in dirs:
            assert os.path.isdir(path)
