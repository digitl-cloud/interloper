"""Regression tests for the CampaignManager360 source.

CM360 report downloads are CSV files with a metadata preamble (ending at a
"Report Fields" line), human-readable headers, and a trailing "Grand Total:"
summary row. The custom normalizer must map those headers onto the schema
columns — notably spelling ``%`` out as ``pct`` so "% Viewable Impressions"
does not collide with "Viewable Impressions" — and survive the host→child
DAG-spec round-trip. What each asset sends and returns is pinned at the HTTP
level, on a real ``dfareporting`` client over a scripted exchange.
"""

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace
from typing import Any

import pytest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpMockSequence
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData

from interloper_assets.campaign_manager_360 import constants
from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection
from interloper_assets.campaign_manager_360.connector import schemas
from interloper_assets.campaign_manager_360.connector.normalizer import CampaignManager360Normalizer
from interloper_assets.campaign_manager_360.connector.source import CampaignManager360

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
    return CampaignManager360(
        id="src-1", profile_id="111", account_id="222", connection=CampaignManager360Connection(service_account_key="x")
    )


def _normalizer() -> CampaignManager360Normalizer:
    return CampaignManager360Normalizer(snake_case_digits=True, flatten_max_level=2)


_BASE = "https://dfareporting.googleapis.com/dfareporting/v5"
_DAY = dt.date(2026, 7, 10)
_CONTEXT = SimpleNamespace(partition_date=_DAY)

_PINNED_FILE = b"""Report Name,report_standard_2026-07-10_2026-07-10
Date Range,2026-07-10 - 2026-07-10

Report Fields
Date,Campaign,Impressions,Clicks
2026-07-10,Campaign A,100,5
2026-07-10,Campaign B,200,10
Grand Total:,,300,15
"""
_PINNED_ROWS = [
    {"Date": "2026-07-10", "Campaign": "Campaign A", "Impressions": 100, "Clicks": 5},
    {"Date": "2026-07-10", "Campaign": "Campaign B", "Impressions": 200, "Clicks": 10},
]

_Answer = tuple[dict[str, str], str | bytes]


def _json(body: dict[str, Any]) -> _Answer:
    return {"status": "200"}, json.dumps(body)


_DELETED: _Answer = ({"status": "204"}, "")


def _report_answers(*statuses: str, file: bytes = _PINNED_FILE) -> list[_Answer]:
    return [
        _json({"id": "1"}),
        _json({"id": "2"}),
        *(_json({"status": status}) for status in statuses),
        ({"status": "200"}, file),
        _DELETED,
    ]


def _report_requests(body: dict[str, Any]) -> list[tuple[str, str, str | None]]:
    return [
        ("POST", f"{_BASE}/userprofiles/111/reports?alt=json", json.dumps(body)),
        ("POST", f"{_BASE}/userprofiles/111/reports/1/run?alt=json", None),
        ("GET", f"{_BASE}/userprofiles/111/reports/1/files/2?alt=json", None),
        ("GET", f"{_BASE}/reports/1/files/2?alt=media", None),
        ("DELETE", f"{_BASE}/userprofiles/111/reports/1?", None),
    ]


class Discovery:
    """Build real ``dfareporting`` clients over one scripted HTTP exchange, counting the builds.

    Stands in for the connection's ``client``: every request its clients send is
    recorded on the one HTTP mock, which answers them in order.
    """

    def __init__(self, *answers: _Answer):
        """Script the answers.

        Args:
            *answers: One answer per request, in order: its headers, ``status`` included, and its body.
        """
        self.http = HttpMockSequence(list(answers))
        self.builds = 0

    def __call__(self) -> Any:
        """Build a client on the scripted HTTP, as the connection's ``client`` does.

        Returns:
            A ``dfareporting`` discovery client.
        """
        self.builds += 1
        return build(constants.API_SERVICE, constants.API_VERSION, http=self.http)

    @property
    def requests(self) -> list[tuple[str, str, str | None]]:
        """The requests sent so far: each one's method, URL and body as sent."""
        return [(method, uri, body) for uri, method, body, _ in self.http.request_sequence]


def _asset(discovery: Discovery, key: str, advertiser_id: str = "") -> Any:
    connection = CampaignManager360Connection(service_account_key="{}")
    connection.__dict__["client"] = discovery
    source = CampaignManager360(
        id="src-1", profile_id="111", account_id="222", advertiser_id=advertiser_id, connection=connection
    )
    return next(asset for asset in source.assets if type(asset).key == key)


class TestHeaderNormalization:
    """CSV headers land on schema columns; ``%`` becomes ``pct``, digits split."""

    def test_parsed_report_conforms_to_schema(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE", file=_REPORT_FILE))
        rows = _asset(discovery, "campaigns_stats").data(context=_CONTEXT)
        normalized = _normalizer().normalize(rows)
        table = TableData.from_data(normalized)
        df = table.reconcile(schemas.CampaignsStats)
        TableData.from_data(df).reconcile(schemas.CampaignsStats, strict=True)  # must not raise
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


class TestRequests:
    """What each asset sends and returns, pinned at the HTTP level on a real ``dfareporting`` client."""

    def test_campaigns_stats_runs_a_standard_report_of_the_day(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))

        assert _asset(discovery, "campaigns_stats").data(context=_CONTEXT) == _PINNED_ROWS
        assert discovery.requests == _report_requests(
            {
                "accountId": "222",
                "name": "report_standard_2026-07-10_2026-07-10",
                "type": "STANDARD",
                "criteria": {
                    "dateRange": {"startDate": "2026-07-10", "endDate": "2026-07-10"},
                    "dimensions": [{"name": name} for name in constants.CAMPAIGN_DIMENSIONS],
                    "metricNames": constants.CAMPAIGN_METRICS,
                },
            }
        )
        assert discovery.builds == 1

    def test_ads_stats_runs_a_standard_report_of_the_day(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))

        assert _asset(discovery, "ads_stats").data(context=_CONTEXT) == _PINNED_ROWS
        assert discovery.requests == _report_requests(
            {
                "accountId": "222",
                "name": "report_standard_2026-07-10_2026-07-10",
                "type": "STANDARD",
                "criteria": {
                    "dateRange": {"startDate": "2026-07-10", "endDate": "2026-07-10"},
                    "dimensions": [{"name": name} for name in constants.AD_DIMENSIONS],
                    "metricNames": constants.AD_METRICS,
                },
            }
        )
        assert discovery.builds == 1

    def test_reach_stats_runs_a_reach_report_and_stamps_the_day(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))

        rows = _asset(discovery, "reach_stats").data(context=_CONTEXT)

        assert rows == [{**row, "date": _DAY} for row in _PINNED_ROWS]
        assert discovery.requests == _report_requests(
            {
                "accountId": "222",
                "name": "report_reach_2026-07-10_2026-07-10",
                "type": "REACH",
                "reachCriteria": {
                    "dateRange": {"startDate": "2026-07-10", "endDate": "2026-07-10"},
                    "dimensions": [{"name": name} for name in constants.UNIQUE_REACH_DIMENSIONS],
                    "metricNames": constants.UNIQUE_REACH_METRICS,
                },
            }
        )
        assert discovery.builds == 1

    def test_a_failed_report_raises_and_its_definition_is_still_deleted(self):
        discovery = Discovery(_json({"id": "1"}), _json({"id": "2"}), _json({"status": "FAILED"}), _DELETED)

        with pytest.raises(RuntimeError, match=r"^Report 1 failed with status FAILED$"):
            _asset(discovery, "campaigns_stats").data(context=_CONTEXT)
        assert [request[:2] for request in discovery.requests[1:]] == [
            ("POST", f"{_BASE}/userprofiles/111/reports/1/run?alt=json"),
            ("GET", f"{_BASE}/userprofiles/111/reports/1/files/2?alt=json"),
            ("DELETE", f"{_BASE}/userprofiles/111/reports/1?"),
        ]

    def test_a_failed_cleanup_is_logged_and_the_rows_are_kept(self, caplog: pytest.LogCaptureFixture):
        answers = _report_answers("REPORT_AVAILABLE")
        answers[-1] = ({"status": "500"}, "")
        discovery = Discovery(*answers)

        assert _asset(discovery, "campaigns_stats").data(context=_CONTEXT) == _PINNED_ROWS
        assert "Failed to delete report 1: <HttpError 500" in caplog.text

    def test_a_refused_report_raises_the_vendors_error_and_sends_nothing_more(self):
        discovery = Discovery(({"status": "403"}, json.dumps({"error": {"code": 403, "message": "No access"}})))

        with pytest.raises(HttpError, match="No access"):
            _asset(discovery, "campaigns_stats").data(context=_CONTEXT)
        assert len(discovery.requests) == 1

    def test_custom_audiences_pages_the_advertisers_remarketing_lists(self):
        audience = {
            "id": "1",
            "name": "Buyers",
            "listPopulationRule": {
                "floodlightActivityId": "7",
                "listPopulationClauses": [{"terms": [{"operator": "NUM_EQUALS", "value": "1"}]}],
            },
        }
        discovery = Discovery(
            _json({"remarketingLists": [audience], "nextPageToken": "next"}),
            _json({"remarketingLists": [{"id": "2", "name": "Visitors"}]}),
        )
        asset = _asset(discovery, "custom_audiences", advertiser_id="9")

        rows = asset.data(context=_CONTEXT)

        assert discovery.requests == [
            ("GET", f"{_BASE}/userprofiles/111/remarketingLists?advertiserId=9&alt=json", None),
            ("GET", f"{_BASE}/userprofiles/111/remarketingLists?advertiserId=9&pageToken=next&alt=json", None),
        ]
        assert discovery.builds == 1
        assert [(row["id"], row["date"]) for row in rows] == [("1", _DAY), ("2", _DAY)]
        written = TableData.from_data(asset.normalizer.normalize(rows)).reconcile(schemas.CustomAudiences)
        columns = [
            "id",
            "name",
            "list_population_rule_floodlight_activity_id",
            "list_population_rule_list_population_clauses",
        ]
        assert written[columns].to_dict("records") == [
            {
                "id": "1",
                "name": "Buyers",
                "list_population_rule_floodlight_activity_id": "7",
                "list_population_rule_list_population_clauses": json.dumps(
                    audience["listPopulationRule"]["listPopulationClauses"]
                ),
            },
            {
                "id": "2",
                "name": "Visitors",
                "list_population_rule_floodlight_activity_id": None,
                "list_population_rule_list_population_clauses": None,
            },
        ]
        assert set(written["date"]) == {_DAY}

    def test_an_advertiser_without_remarketing_lists_has_no_audiences(self):
        discovery = Discovery(_json({"kind": "dfareporting#remarketingListsListResponse"}))

        assert _asset(discovery, "custom_audiences", advertiser_id="9").data(context=_CONTEXT) == []
        assert len(discovery.requests) == 1

    def test_custom_audiences_without_an_advertiser_raises_before_any_request(self):
        discovery = Discovery()

        with pytest.raises(
            ValueError, match=r"^The custom_audiences asset requires the source's advertiser_id to be set$"
        ):
            _asset(discovery, "custom_audiences").data(context=_CONTEXT)
        assert discovery.builds == 0


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, CampaignManager360Normalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 2
        # The % -> pct behavior must survive in the child pod.
        assert normalizer.column_name("Active View: % Viewable Impressions") == ("active_view_pct_viewable_impressions")
