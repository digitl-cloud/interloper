"""Regression tests for the BingAds source configuration.

The Bing Ads report comes back with PascalCase CSV headers
(``AccountName``, ``TimePeriod``, ``Ctr``, …) while the ``Ads``
schema is snake_case. A source-level ``DataFrameNormalizer`` bridges the
two — these tests pin that the normalizer reaches every asset instance,
survives the host→child spec round-trip, and that a raw API-shaped row
normalizes and validates against the schema (the same chain that broke
in prod for AmazonAds; see ``amazon_ads/test_source.py``).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.asset import Asset
from interloper.asset.context import ExecutionContext
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer
from suds import WebFault

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.connector import schemas
from interloper_assets.bing_ads.connector.schemas import AdsStats
from interloper_assets.bing_ads.connector.source import BingAds, _translate_soap_fault

ENTITY_ASSETS = ("accounts", "campaigns", "ad_groups", "ads")
DAY = dt.date(2026, 11, 5)
AD_GROUPS_FILE = """\
Type,Status,Id,Parent Id,Campaign,Ad Group,Modified Time,Start Date,End Date,Cpc Bid,Use Optimized Targeting,Name,Bid
Format Version,,,,,,,,,,,6.0,
Account,,111,999,,,11/04/2026 08:00:00,,,,,,
Ad Group,Active,9007199254740993,555,Camp,Group A,11/04/2026 09:15:02.123,10/01/2026,12/31/2026,0.45,True,,
Ad Group,Paused,777,555,Camp,Group B,11/03/2026 10:00:00,10/01/2026,,0.3,FALSE,,
"""
ADS_FILE = """\
Type,Status,Id,Parent Id,Headline,Title Part 1
Format Version,,,,,
Account,,111,999,,
Responsive Search Ad,Active,1,9,"[{""text"":""Shop""}]",
Expanded Text Ad,Paused,2,9,,Title
Some Future Ad,Active,3,9,,
"""


def _source() -> Any:
    return BingAds(id="src-1", account_id="123")  # ty: ignore[missing-argument]


def _connected_source() -> Any:
    connection = BingAdsConnection(client_id="cid", client_secret="x", refresh_token="x", developer_token="x")
    return BingAds(id="src-1", account_id="111", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


async def _run(source: Any, key: str) -> Any:
    asset = _asset(source, key)
    return await asset.data(context=ExecutionContext(asset, il.TimePartition(value=DAY).window))


def _reconcile(source: Any, key: str, data: Any) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    reconciled = TableData.from_data(asset.normalizer.normalize(data)).reconcile(asset.schema)
    return reconciled.astype(object).where(reconciled.notna(), None).to_dict("records")


@dataclass
class FakeBulkServiceManager:
    """Writes a canned Bulk file where the SDK would, recording what it was asked for."""

    content: str
    calls: list[tuple[str, str, list[str]]]
    account_id: str
    customer_id: str

    def download_file(self, download_parameters: Any) -> str:
        self.calls.append((self.account_id, self.customer_id, download_parameters.download_entities))
        path = Path(download_parameters.result_file_directory) / download_parameters.result_file_name
        path.write_text(self.content, encoding="utf-8-sig")
        return str(path)


@pytest.fixture
def bulk(monkeypatch):
    calls: list[tuple[str, str, list[str]]] = []

    async def customer_id(self, account_id: str) -> str:
        return "999"

    def install(content: str) -> list[tuple[str, str, list[str]]]:
        monkeypatch.setattr(BingAdsConnection, "customer_id", customer_id)
        monkeypatch.setattr(
            BingAdsConnection,
            "bulk_service_manager",
            lambda self, account_id, customer: FakeBulkServiceManager(content, calls, account_id, customer),
        )
        return calls

    return install


class TestEntityAssets:
    def test_entities_are_snapshots_tagged_entity(self):
        for key in ENTITY_ASSETS:
            asset = _asset(_source(), key)
            assert isinstance(asset.partitioning, il.SnapshotPartitionConfig), key
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Entity"], key

    async def test_ad_groups_come_from_the_bulk_file_with_the_resolved_customer(self, bulk):
        calls = bulk(AD_GROUPS_FILE)
        source = _connected_source()
        frame = await _run(source, "ad_groups")
        assert calls == [("111", "999", ["AdGroups"])]
        assert list(frame["Type"]) == ["Ad Group", "Ad Group"]
        assert "Bid" not in frame.columns and "Name" not in frame.columns

    @pytest.mark.parametrize(
        ("key", "entities"), [("campaigns", "Campaigns"), ("ad_groups", "AdGroups"), ("ads", "Ads")]
    )
    async def test_each_asset_downloads_its_own_entity_type(self, bulk, key: str, entities: str):
        calls = bulk(ADS_FILE)
        await _run(_connected_source(), key)
        assert calls == [("111", "999", [entities])]

    async def test_a_soap_fault_during_the_download_names_its_operation_errors(self, bulk, monkeypatch):
        bulk(ADS_FILE)
        fault = _web_fault(
            SimpleNamespace(
                ApiFaultDetail=SimpleNamespace(
                    BatchErrors="",
                    OperationErrors=SimpleNamespace(
                        OperationError=SimpleNamespace(ErrorCode="CustomerIdHasToBeSpecified", Message="no customer")
                    ),
                )
            )
        )

        def download_file(self, download_parameters: Any) -> str:
            raise fault

        monkeypatch.setattr(FakeBulkServiceManager, "download_file", download_file)
        with pytest.raises(RuntimeError, match="CustomerIdHasToBeSpecified: no customer"):
            await _run(_connected_source(), "campaigns")

    async def test_another_download_error_is_raised_as_is(self, bulk, monkeypatch):
        bulk(ADS_FILE)

        def download_file(self, download_parameters: Any) -> str:
            raise TimeoutError("bulk download timed out")

        monkeypatch.setattr(FakeBulkServiceManager, "download_file", download_file)
        with pytest.raises(TimeoutError, match="timed out"):
            await _run(_connected_source(), "ads")

    async def test_bulk_values_reconcile_to_their_types(self, bulk):
        bulk(AD_GROUPS_FILE)
        source = _connected_source()
        first, second = _reconcile(source, "ad_groups", await _run(source, "ad_groups"))
        assert (first["id"], first["parent_id"], first["ad_group"], first["date"]) == (
            "9007199254740993",
            "555",
            "Group A",
            DAY,
        )
        assert (first["start_date"], first["end_date"], second["end_date"]) == (
            dt.date(2026, 10, 1),
            dt.date(2026, 12, 31),
            None,
        )
        assert first["modified_time"] == pd.Timestamp("2026-11-04 09:15:02.123", tz="UTC")
        assert (first["cpc_bid"], first["use_optimized_targeting"], second["use_optimized_targeting"]) == (
            0.45,
            True,
            False,
        )

    async def test_every_ad_type_is_kept(self, bulk):
        bulk(ADS_FILE)
        source = _connected_source()
        rows = _reconcile(source, "ads", await _run(source, "ads"))
        assert [(row["type"], row["id"], row["parent_id"]) for row in rows] == [
            ("Responsive Search Ad", "1", "9"),
            ("Expanded Text Ad", "2", "9"),
            ("Some Future Ad", "3", "9"),
        ]
        assert rows[0]["headline"] == '[{"text":"Shop"}]'

    async def test_the_account_leaves_out_its_address_and_tax_registration(self, monkeypatch):
        async def account(self, account_id: str) -> dict[str, Any]:
            return {
                "Id": account_id,
                "Name": "Acct",
                "CurrencyCode": "EUR",
                "TimeZone": "AmsterdamBerlinBernRomeStockholmVienna",
                "ParentCustomerId": "999",
                "LastModifiedTime": "2026-10-01T08:30:00.123",
                "PauseReason": None,
                "LinkedAgencies": [{"Id": "5", "Name": "Agency"}],
                "BusinessAddress": [{"City": "Hamburg"}],
                "TaxCertificate": [{"Status": "Valid"}],
            }

        monkeypatch.setattr(BingAdsConnection, "account", account)
        source = _connected_source()
        rows = await _run(source, "accounts")
        assert not {"BusinessAddress", "TaxCertificate"} & set(rows[0])
        [row] = _reconcile(source, "accounts", rows)
        assert (row["id"], row["date"], row["parent_customer_id"], row["time_zone"]) == (
            "111",
            DAY,
            "999",
            "AmsterdamBerlinBernRomeStockholmVienna",
        )
        assert row["linked_agencies"] == '[{"Id": "5", "Name": "Agency"}]'
        assert row["last_modified_time"] == pd.Timestamp("2026-10-01 08:30:00.123")

    def test_the_excluded_account_fields_are_not_declared(self):
        declared = set(schemas.Accounts.model_fields)
        assert not declared & {"business_address", "tax_certificate", "tax_information"}


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
        row: dict[str, object] = dict.fromkeys(constants.AD_PERFORMANCE_FIELDS)
        row["TimePeriod"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        TableData.from_data(normalized).reconcile(AdsStats)  # must not raise


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
