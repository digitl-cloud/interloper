"""Regression tests for the FacebookAds source.

Facebook insights nest ``actions``/``action_values`` lists that
``FacebookActionsNormalizer`` pivots into one column per action type
(``actions`` -> ``actions_link_click``); entity reports nest dicts
(``creative``) that the base normalizer flattens. These tests pin that
the custom normalizer survives the host→child spec round-trip, and that
frames reconcile against the ported schemas (missing nullable columns are
filled, so partial action sets are fine).
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from requests.structures import CaseInsensitiveDict

from interloper_assets.facebook_ads import constants
from interloper_assets.facebook_ads.api import FacebookGraphApi
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.connector import schemas
from interloper_assets.facebook_ads.connector.normalizer import PIVOT_COLUMNS, FacebookActionsNormalizer
from interloper_assets.facebook_ads.connector.source import FacebookAds


def _source() -> Any:
    connection = FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")
    return FacebookAds(id="src-1", account_id="123", connection=connection)


class TestSourceNormalizer:
    def test_all_assets_use_the_actions_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, FacebookActionsNormalizer), type(asset).key

    def test_all_ten_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "campaigns_stats",
            "ads_stats",
            "ads_stats_by_age_gender",
            "ads_stats_by_country",
            "videos_stats",
            "custom_audiences",
            "ads",
            "campaigns",
            "ad_accounts",
            "adsets",
        }


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_custom_normalizer_survives_roundtrip(self):
        norm = self._child("ads_stats").normalizer
        assert isinstance(norm, FacebookActionsNormalizer)  # not degraded to the base
        assert norm.flatten_max_level == 1

    def test_insights_row_reconciles_against_ads_schema(self):
        child = self._child("ads_stats")
        rows = [
            {"date_start": "2026-06-10", "account_id": "123", "actions": [{"action_type": "link_click", "value": "7"}]}
        ]
        normalized = child.normalizer.normalize(rows)
        assert "actions_link_click" in normalized.columns
        out = TableData.from_data(normalized).reconcile(schemas.AdsStats)
        assert int(out.loc[0, "actions_link_click"]) == 7

    def test_sparse_row_passes_validation(self):
        """A sparse insights frame still passes validation.

        Only a few of the 200 fields arrive, and every schema field is
        optional, so absent action types are not 'Field required' errors.
        """
        df = pd.DataFrame([{"date_start": "2026-06-10", "account_id": "123", "actions_link_click": 7}])
        TableData.from_data(df).reconcile(schemas.AdsStats, strict=True)  # must not raise

    def test_entity_row_keeps_the_creative_whole_and_reconciles(self):
        child = self._child("ads")
        rows = [{"id": "456", "name": "My Ad", "creative": {"id": "789"}}]
        normalized = child.normalizer.normalize(rows)
        assert "creative_id" not in normalized.columns
        out = TableData.from_data(normalized).reconcile(schemas.Ads)
        assert json.loads(out.loc[0, "creative"]) == {"id": "789"}


class TestSchemaParity:
    """Both stats assets fetch the same action arrays; the schemas must not drop them unevenly."""

    _ACTION_FAMILIES = (
        "actions_",
        "action_values_",
        "cost_per_action_type_",
        "cost_per_unique_action_type_",
        "unique_actions_",
    )

    def test_ads_stats_declares_every_campaign_action_column(self):
        campaigns = schemas.CampaignsStats.model_fields
        ads = schemas.AdsStats.model_fields
        expected = {name: info for name, info in campaigns.items() if name.startswith(self._ACTION_FAMILIES)}

        missing = sorted(set(expected) - set(ads))
        assert not missing, f"AdsStats lacks {len(missing)} campaign action columns: {missing}"

        mismatched = {
            name: (str(info.annotation), str(ads[name].annotation))
            for name, info in expected.items()
            if info.annotation != ads[name].annotation
        }
        assert not mismatched, mismatched

    def test_ads_stats_declares_every_requested_scalar(self):
        # Breakdown dimensions come back as columns too, though they are not in the fields list.
        breakdowns = {"publisher_platform", "platform_position", "impression_device"}
        requested = {field for field in constants.ADS_INSIGHT_FIELDS if field not in PIVOT_COLUMNS} | breakdowns

        missing = sorted(requested - set(schemas.AdsStats.model_fields))
        assert not missing, f"requested but undeclared on AdsStats (silently dropped): {missing}"

    def test_campaigns_stats_declares_the_shared_scalars(self):
        for name in ("attribution_setting", "quality_ranking", "converted_product_quantity"):
            assert name in schemas.CampaignsStats.model_fields, name

    @pytest.mark.parametrize(
        ("fields", "schema"),
        [
            (constants.AD_ACCOUNT_ENTITY_FIELDS, schemas.AdAccounts),
            (constants.ADSET_ENTITY_FIELDS, schemas.Adsets),
            (constants.AD_ENTITY_FIELDS, schemas.Ads),
            (constants.CAMPAIGN_ENTITY_FIELDS, schemas.Campaigns),
        ],
    )
    def test_entity_schemas_declare_exactly_the_requested_fields(self, fields: list[str], schema: type[il.Schema]):
        declared = set(schema.model_fields) - {"date"}
        assert sorted(set(fields) - declared) == [], "requested but undeclared (silently dropped)"
        assert sorted(declared - set(fields)) == [], "declared but never requested (always NULL)"


class TestNewEntities:
    """Ad accounts and ad sets keep nested objects whole, under the vendor's name."""

    def _child(self, key: str) -> Any:
        return TestSpecRoundtripAndReconcile()._child(key)

    def test_entity_normalizer_survives_roundtrip_without_flattening(self):
        for key in ("ad_accounts", "adsets", "ads", "campaigns"):
            normalizer = self._child(key).normalizer
            assert isinstance(normalizer, FacebookActionsNormalizer), key
            assert normalizer.flatten_max_level == 0, key

    def test_adset_targeting_lands_whole_as_json(self):
        child = self._child("adsets")
        rows = [
            {
                "id": "1",
                "targeting": {"geo_locations": {"countries": ["DE"]}, "age_min": 18},
                "promoted_object": {"pixel_id": "9", "custom_event_type": "PURCHASE"},
                "date": "2026-10-01",
            }
        ]
        normalized = child.normalizer.normalize(rows)
        assert not [column for column in normalized.columns if column.startswith("targeting_")]
        out = TableData.from_data(normalized).reconcile(schemas.Adsets)
        assert json.loads(out.loc[0, "targeting"]) == {"geo_locations": {"countries": ["DE"]}, "age_min": 18}
        assert json.loads(out.loc[0, "promoted_object"])["pixel_id"] == "9"

    def test_ad_account_keeps_the_vendors_business_name(self):
        child = self._child("ad_accounts")
        rows = [{"id": "act_1", "business": {"id": "5", "name": "Acme Holding"}, "business_name": "Acme GmbH"}]
        normalized = child.normalizer.normalize(rows)
        out = TableData.from_data(normalized).reconcile(schemas.AdAccounts)
        assert out.loc[0, "business_name"] == "Acme GmbH"

    def test_adset_instants_and_minor_unit_amounts_are_typed(self):
        child = self._child("adsets")
        rows = [
            {
                "id": "1",
                "created_time": "2026-09-01T10:00:00+0200",
                "updated_time": "2026-09-02T10:00:00+0200",
                "daily_budget": "1500",
                "budget_remaining": "0",
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Adsets, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert (out.loc[0, "daily_budget"], out.loc[0, "budget_remaining"]) == (1500, 0)

    def test_ad_account_instants_and_minor_unit_amounts_are_typed(self):
        child = self._child("ad_accounts")
        rows = [{"id": "act_1", "created_time": "2020-01-15T09:30:00+0100", "amount_spent": "123456", "spend_cap": "0"}]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.AdAccounts, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2020-01-15T08:30:00", tz="UTC")
        assert (out.loc[0, "amount_spent"], out.loc[0, "spend_cap"]) == (123456, 0)

    def test_campaign_objects_stay_whole_and_its_instants_and_amounts_are_typed(self):
        child = self._child("campaigns")
        adlabels = [{"id": "7", "name": "brand"}]
        rows = [
            {
                "id": "1",
                "adlabels": adlabels,
                "created_time": "2026-09-01T10:00:00+0200",
                "daily_budget": "1500",
                "spend_cap": "0",
                "start_time": "2026-09-05T00:00:00+0200",
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Campaigns, strict=True)
        assert json.loads(out.loc[0, "adlabels"]) == adlabels
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert (out.loc[0, "daily_budget"], out.loc[0, "spend_cap"]) == (1500, 0)
        assert out.loc[0, "start_time"] == "2026-09-05T00:00:00+0200"

    def test_ad_instants_bid_and_objects_are_typed(self):
        child = self._child("ads")
        rows = [
            {
                "id": "1",
                "created_time": "2026-09-01T10:00:00+0200",
                "bid_amount": 250,
                "adlabels": [{"id": "7", "name": "brand"}],
                "targeting": {"geo_locations": {"countries": ["DE"]}},
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Ads, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert out.loc[0, "bid_amount"] == 250
        assert json.loads(out.loc[0, "adlabels"]) == [{"id": "7", "name": "brand"}]
        assert json.loads(out.loc[0, "targeting"]) == {"geo_locations": {"countries": ["DE"]}}


class _Reply:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code = status
        self.text = json.dumps(body)
        self.headers = CaseInsensitiveDict()


class _Requests:
    def __init__(self, bodies: list[dict | _Reply]) -> None:
        self.replies = [body if isinstance(body, _Reply) else _Reply(200, body) for body in bodies]
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> _Reply:
        self.sent.append((method, url.removeprefix(_GRAPH), dict(kwargs.get("params") or kwargs.get("data") or {})))
        return self.replies.pop(0)


class _Session:
    GRAPH = constants.BASE_URL
    timeout = None

    def __init__(self, bodies: list[dict | _Reply]) -> None:
        self.requests = _Requests(bodies)


_GRAPH = f"{constants.BASE_URL}/v{constants.API_VERSION}/"
_DAY = dt.date(2026, 10, 1)
_PLATFORM = ["publisher_platform", "platform_position", "impression_device"]
_JOB = [{"report_run_id": "777"}, {"id": "777", "async_status": "Job Completed", "async_percent_completion": 100}]
_TOO_HEAVY = _Reply(500, {"error": {"code": 1, "message": "Please reduce the amount of data you're asking for"}})


def _page(rows: list[dict], after: str | None = None) -> dict[str, Any]:
    paging = {"paging": {"cursors": {"after": after}, "next": f"{_GRAPH}next"}} if after else {}
    return {"data": rows, **paging}


def _encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _run(source: Any, key: str) -> list[dict]:
    asset = next(asset for asset in source.assets if type(asset).key == key)
    return asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=_DAY).window))


def _source_on(*bodies: dict | _Reply) -> tuple[Any, list[tuple[str, str, dict[str, Any]]]]:
    source = _source()
    client = FacebookGraphApi(_Session(list(bodies)), api_version=f"v{constants.API_VERSION}")
    source.connection.__dict__["client"] = client
    return source, client._session.requests.sent


class TestRequests:
    """What each asset sends to the Graph API and returns, pinned at the HTTP session under the SDK."""

    @pytest.mark.parametrize(
        ("key", "fields", "level", "breakdowns"),
        [
            ("campaigns_stats", constants.CAMPAIGNS_FIELDS, "campaign", _PLATFORM),
            ("ads_stats", constants.ADS_INSIGHT_FIELDS, "ad", _PLATFORM),
            ("ads_stats_by_age_gender", constants.GENDER_AGE_FIELDS, "ad", ["gender", "age"]),
            ("ads_stats_by_country", constants.GEO_FIELDS, "ad", ["country", "region"]),
            ("videos_stats", constants.VIDEOS_FIELDS, "ad", _PLATFORM),
        ],
    )
    def test_a_report_runs_one_day_as_a_job_and_reads_every_page_of_its_result(
        self, key: str, fields: list[str], level: str, breakdowns: list[str]
    ):
        rows = [{"ad_id": "1", "date_start": "2026-10-01"}, {"ad_id": "2", "date_start": "2026-10-01"}]
        source, sent = _source_on(*_JOB, _page(rows[:1], after="A"), _page(rows[1:]))

        assert _run(source, key) == rows
        assert sent == [
            (
                "POST",
                "act_123/insights",
                {
                    "level": level,
                    "time_range": _encoded({"since": "2026-10-01", "until": "2026-10-01"}),
                    "breakdowns": _encoded(breakdowns),
                    "action_breakdowns": _encoded(["action_type"]),
                    "fields": _encoded(fields),
                },
            ),
            ("GET", "777/", {}),
            ("GET", "777/insights", {}),
            ("GET", "777/insights", {"after": "A"}),
        ]

    @pytest.mark.parametrize(
        ("key", "edge", "fields"),
        [
            ("campaigns", "campaigns", constants.CAMPAIGN_ENTITY_FIELDS),
            ("adsets", "adsets", constants.ADSET_ENTITY_FIELDS),
            ("ads", "ads", constants.AD_ENTITY_FIELDS),
        ],
    )
    def test_an_entity_reads_every_page_of_live_objects_and_stamps_the_day(
        self, key: str, edge: str, fields: list[str]
    ):
        first = {"id": "1", "bid_amount": None, "targeting": {"age_min": 18, "age_max": None}}
        source, sent = _source_on(_page([first], after="A"), _page([{"id": "2"}]))

        assert _run(source, key) == [
            {**first, "date": _DAY},
            {"id": "2", "date": _DAY},
        ]
        query = {"limit": 100, "fields": ",".join(fields)}
        assert sent == [("GET", f"act_123/{edge}", query), ("GET", f"act_123/{edge}", {**query, "after": "A"})]

    def test_custom_audiences_read_every_page_and_stamp_the_day(self):
        source, sent = _source_on(_page([{"id": "1"}], after="A"), _page([{"id": "2"}]))

        assert _run(source, "custom_audiences") == [{"id": "1", "date": _DAY}, {"id": "2", "date": _DAY}]
        query = {"fields": ",".join(constants.CUSTOM_AUDIENCES_FIELDS)}
        assert sent == [
            ("GET", "act_123/customaudiences", query),
            ("GET", "act_123/customaudiences", {**query, "after": "A"}),
        ]

    def test_the_ad_account_is_read_in_one_request_and_stamped(self):
        source, sent = _source_on({"id": "act_123", "account_id": "123", "currency": "EUR"})

        assert _run(source, "ad_accounts") == [{"id": "act_123", "account_id": "123", "currency": "EUR", "date": _DAY}]
        assert sent == [("GET", "act_123/", {"fields": ",".join(constants.AD_ACCOUNT_ENTITY_FIELDS)})]

    def test_a_page_halved_for_size_stays_halved_for_that_read_only(self):
        source, sent = _source_on(_TOO_HEAVY, _page([{"id": "1"}], after="A"), _page([]), _page([{"id": "2"}]))

        assert _run(source, "campaigns") == [{"id": "1", "date": _DAY}]
        assert _run(source, "ads") == [{"id": "2", "date": _DAY}]
        assert [(path, query["limit"]) for _, path, query in sent] == [
            ("act_123/campaigns", 100),
            ("act_123/campaigns", 50),
            ("act_123/campaigns", 50),
            ("act_123/ads", 100),
        ]

    def test_the_account_requests_no_field_that_needs_a_page(self):
        # Facebook answers 400 for the whole account when one of these comes without a page_id
        for name in ("show_checkout_experience", "has_page_authorized_adaccount"):
            assert name not in constants.AD_ACCOUNT_ENTITY_FIELDS
