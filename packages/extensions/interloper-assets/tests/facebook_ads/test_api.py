"""Tests for the Graph API client and the Marketing API engine built on it.

The client waits, retries and asks for less in one place; the engine reads
nodes, walks paged edges by their ``after`` cursor and runs insights reports
as jobs. Both are pinned against a fake HTTP session under the SDK.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pytest
import requests
from facebook_business.exceptions import FacebookError, FacebookRequestError
from requests.structures import CaseInsensitiveDict

from interloper_assets.facebook_ads import api as graph
from interloper_assets.facebook_ads.api import Attempts, FacebookGraphApi, FacebookMarketingApi

REFUSAL = "Please reduce the amount of data you're asking for, then retry your request"
GRAPH = "https://graph.facebook.com/v23.0/"


class _Reply:
    def __init__(self, status: int, body: dict, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self.text = json.dumps(body)
        self.headers = CaseInsensitiveDict(headers or {})


def _ok(body: dict | None = None, headers: dict[str, str] | None = None) -> _Reply:
    return _Reply(200, body if body is not None else {"id": "1"}, headers)


def _error(status: int, code: int, message: str = "error", headers: dict[str, str] | None = None) -> _Reply:
    return _Reply(status, {"error": {"code": code, "message": message}}, headers)


class _Requests:
    def __init__(self, replies: list[_Reply | Exception]) -> None:
        self.replies = replies
        self.sent: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> _Reply:
        self.sent.append({"method": method, "url": url, **(kwargs.get("params") or kwargs.get("data") or {})})
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class _Session:
    GRAPH = "https://graph.facebook.com"
    timeout = None

    def __init__(self, replies: list[_Reply | Exception]) -> None:
        self.requests = _Requests(replies)


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(graph, "sleep", slept.append)
    return slept


def _business_usage(**limit: float) -> dict[str, str]:
    return {"x-business-use-case-usage": json.dumps({"1": [limit]})}


def _api(*replies: _Reply | Exception) -> FacebookGraphApi:
    return FacebookGraphApi(_Session(list(replies)), api_version="v23.0")


def _sent(api: FacebookGraphApi) -> list[dict[str, Any]]:
    return api._session.requests.sent


def _engine(*replies: _Reply | Exception) -> FacebookMarketingApi:
    return FacebookMarketingApi(_api(*replies))


def _requests(engine: FacebookMarketingApi) -> list[dict[str, Any]]:
    return [{**request, "url": request["url"].removeprefix(GRAPH)} for request in _sent(engine.client)]


class TestThrottling:
    def test_a_throttled_request_waits_as_long_as_meta_says_then_repeats(self, sleeps):
        usage = _business_usage(call_count=100, estimated_time_to_regain_access=3)
        api = _api(_error(400, 80004, "too many calls", usage), _ok())
        assert api.call("GET", ("act_1", "ads"), params={"limit": 25}).json() == {"id": "1"}
        assert sleeps == [180]
        assert len(_sent(api)) == 2

    def test_every_rate_limit_code_is_waited_out(self, sleeps):
        for code in (4, 17, 32, 613, 80000, 80014):
            api = _api(_error(400, code), _ok())
            api.call("GET", ("act_1", ""))
        assert len(sleeps) == 6

    def test_a_throttle_without_a_hint_backs_off_until_the_budget_is_spent(self, sleeps):
        api = _api(*[_error(400, 17)] * 20)
        with pytest.raises(FacebookRequestError) as raised:
            api.call("GET", ("act_1", ""))
        assert raised.value.api_error_code() == 17
        assert sleeps[:5] == [60, 120, 240, 480, 600]
        assert sum(sleeps) <= Attempts.THROTTLE_BUDGET.total_seconds()

    def test_a_lockout_meta_announces_is_waited_out_before_the_next_request(self, sleeps):
        api = _api(_ok(headers=_business_usage(total_time=100, estimated_time_to_regain_access=4)))
        api.call("GET", ("act_1", ""))
        assert sleeps == [240]

    def test_high_usage_without_a_lockout_is_not_paced(self, sleeps):
        busy = {
            "x-app-usage": json.dumps({"call_count": 95, "total_time": 10, "total_cputime": 10, "type": "app"}),
            "x-ad-account-usage": json.dumps({"acc_id_util_pct": 99, "reset_time_duration": 30}),
            "x-fb-ads-insights-throttle": json.dumps(
                {"app_id_util_pct": 95, "acc_id_util_pct": 10, "ads_api_access_tier": "development_access"}
            ),
            **_business_usage(call_count=96, estimated_time_to_regain_access=0),
        }
        api = _api(_ok(headers=busy), _ok(headers=busy))
        api.call("GET", ("act_1", "insights"))
        api.call("GET", ("act_1", ""))
        assert sleeps == []


class TestTransientFailures:
    def test_a_server_error_is_repeated_after_a_short_backoff(self, sleeps):
        unavailable, unknown = _error(503, 2, "Service temporarily unavailable"), _error(500, 1, "An unknown error")
        api = _api(unavailable, unknown, _ok())
        api.call("GET", ("act_1", ""))
        assert sleeps == [5, 10]

    def test_a_dropped_connection_is_repeated(self, sleeps):
        api = _api(requests.exceptions.ConnectionError("reset"), _ok())
        api.call("GET", ("act_1", ""))
        assert sleeps == [5]

    def test_a_connection_that_keeps_dropping_is_raised_after_the_last_attempt(self, sleeps):
        api = _api(*[requests.exceptions.ConnectionError("reset")] * Attempts.TRANSIENT_ATTEMPTS)
        with pytest.raises(requests.exceptions.ConnectionError):
            api.call("GET", ("act_1", ""))
        assert sleeps == [5, 10, 20, 40]

    def test_a_failure_that_persists_is_raised_after_the_last_attempt(self, sleeps):
        api = _api(*[_error(500, 1, "An unknown error occurred")] * Attempts.TRANSIENT_ATTEMPTS)
        with pytest.raises(FacebookRequestError):
            api.call("GET", ("act_1", ""))
        assert len(_sent(api)) == Attempts.TRANSIENT_ATTEMPTS

    def test_any_other_error_is_raised_at_once(self, sleeps):
        api = _api(_error(400, 100, "(#100) Invalid parameter"))
        with pytest.raises(FacebookRequestError):
            api.call("GET", ("act_1", ""))
        assert (sleeps, len(_sent(api))) == ([], 1)


class TestAskingForLess:
    def test_a_page_too_heavy_is_read_with_half_the_limit_which_its_cursor_keeps(self, sleeps):
        params = {"fields": "id,name", "limit": 25}
        api = _api(_error(500, 1, REFUSAL), _error(500, 1, REFUSAL), _ok({"data": []}))
        api.call("GET", ("act_1", "ads"), params=params)
        assert [request["limit"] for request in _sent(api)] == [25, 12, 6]
        assert params["limit"] == 6
        assert sleeps == []

    def test_a_page_without_a_limit_halves_the_graph_default(self, sleeps):
        params: dict[str, Any] = {"fields": "id"}
        api = _api(_error(500, 1, REFUSAL), _ok({"data": []}))
        api.call("GET", ("act_1", "campaigns"), params=params)
        assert params["limit"] == 12

    def test_a_page_of_one_still_too_heavy_fails(self, sleeps):
        api = _api(_error(500, 1, REFUSAL))
        with pytest.raises(FacebookRequestError, match="Call was not successful"):
            api.call("GET", ("act_1", "ads"), params={"limit": 1})

    def test_an_object_too_heavy_is_read_in_halves_of_its_fields_and_merged(self, sleeps):
        api = _api(
            _error(500, 1, REFUSAL),
            _error(500, 1, REFUSAL),
            _ok({"id": "act_1", "account_id": "1"}),
            _ok({"id": "act_1", "name": "Acct"}),
            _ok({"id": "act_1", "currency": "EUR", "timezone_name": "Europe/Berlin"}),
        )
        response = api.call("GET", ("act_1", ""), params={"fields": "account_id,name,currency,timezone_name"})
        assert response.json() == {
            "id": "act_1",
            "account_id": "1",
            "name": "Acct",
            "currency": "EUR",
            "timezone_name": "Europe/Berlin",
        }
        assert [request["fields"] for request in _sent(api)] == [
            "account_id,name,currency,timezone_name",
            "account_id,name",
            "account_id",
            "name",
            "currency,timezone_name",
        ]

    def test_an_object_field_too_heavy_alone_fails(self, sleeps):
        api = _api(_error(500, 1, REFUSAL), _error(500, 1, REFUSAL), _ok({"id": "act_1", "name": "Acct"}))
        with pytest.raises(FacebookRequestError, match="Call was not successful"):
            api.call("GET", ("act_1", ""), params={"fields": "failed_delivery_checks,name"})

    def test_a_write_too_heavy_is_raised_unchanged(self, sleeps):
        api = _api(_error(500, 1, REFUSAL))
        with pytest.raises(FacebookRequestError):
            api.call("POST", ("act_1", "insights"), params={"fields": "spend,impressions"})
        assert len(_sent(api)) == 1


def _page(rows: list[dict], after: str | None = None) -> _Reply:
    paging = {"paging": {"cursors": {"after": after}, "next": f"{GRAPH}next"}} if after else {}
    return _ok({"data": rows, **paging})


def _encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class TestGet:
    def test_reads_one_node_with_its_fields_comma_separated(self, sleeps):
        engine = _engine(_ok({"id": "act_1", "account_id": "1", "name": None}))

        assert engine.get("act_1", fields=["account_id", "name"]) == {"id": "act_1", "account_id": "1", "name": None}
        assert _requests(engine) == [{"method": "GET", "url": "act_1/", "fields": "account_id,name"}]

    def test_an_http_error_raises_facebooks_refusal(self, sleeps):
        engine = _engine(_error(400, 100, "(#100) Tried accessing nonexisting field"))

        with pytest.raises(FacebookRequestError) as raised:
            engine.get("act_1", fields=["nonexisting"])
        assert raised.value.api_error_message() == "(#100) Tried accessing nonexisting field"


class TestRows:
    def test_every_page_is_read_by_its_after_cursor(self, sleeps):
        engine = _engine(_page([{"id": "1"}], after="A"), _page([{"id": "2"}], after="B"), _page([{"id": "3"}]))

        assert engine.rows("act_1/ads", fields=["id", "name"], limit=100) == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        assert _requests(engine) == [
            {"method": "GET", "url": "act_1/ads", "fields": "id,name", "limit": 100},
            {"method": "GET", "url": "act_1/ads", "fields": "id,name", "limit": 100, "after": "A"},
            {"method": "GET", "url": "act_1/ads", "fields": "id,name", "limit": 100, "after": "B"},
        ]

    def test_a_refusal_on_a_later_page_raises(self, sleeps):
        engine = _engine(_page([{"id": "1"}], after="A"), _error(400, 190, "Error validating access token"))

        with pytest.raises(FacebookRequestError) as raised:
            engine.rows("act_1/campaigns", fields=["id"])
        assert raised.value.api_error_code() == 190

    def test_nothing_to_list_reads_one_page(self, sleeps):
        engine = _engine(_ok({"data": []}))

        assert engine.rows("act_1/customaudiences", fields=["id"]) == []
        assert len(_requests(engine)) == 1

    def test_a_response_without_paging_is_read_once(self, sleeps):
        engine = _engine(_ok({"data": [{"id": "1"}]}), _ok({"data": [{"id": "1"}]}))

        assert engine.rows("act_1/adsets", fields=["id"]) == [{"id": "1"}]
        assert len(_requests(engine)) == 1

    def test_an_empty_page_ends_the_walk_whatever_its_cursor_says(self, sleeps):
        engine = _engine(_page([{"id": "1"}], after="A"), _page([], after="B"), _page([{"id": "3"}]))

        assert engine.rows("act_1/ads", fields=["id"]) == [{"id": "1"}]
        assert len(_requests(engine)) == 2

    def test_a_cursor_without_an_after_ends_the_walk(self, sleeps):
        engine = _engine(_ok({"data": [{"id": "1"}], "paging": {"cursors": {}, "next": f"{GRAPH}next"}}))

        assert engine.rows("act_1/ads", fields=["id"]) == [{"id": "1"}]
        assert len(_requests(engine)) == 1

    def test_a_field_list_given_as_one_string_is_sent_as_is(self, sleeps):
        engine = _engine(_page([{"id": "1"}]))

        engine.rows("act_1/ads", fields="id,name")
        assert _requests(engine)[0]["fields"] == "id,name"

    def test_a_page_halved_for_size_keeps_that_size_for_the_pages_after_it(self, sleeps):
        engine = _engine(_error(500, 1, REFUSAL), _page([{"id": "1"}], after="A"), _page([{"id": "2"}]))

        assert engine.rows("act_1/ads", fields=["id"], limit=100) == [{"id": "1"}, {"id": "2"}]
        assert [request["limit"] for request in _requests(engine)] == [100, 50, 50]

    def test_records_come_back_as_graph_sends_them(self, sleeps):
        record = {"id": "1", "bid_amount": None, "targeting": {"age_min": 18, "age_max": None}}
        engine = _engine(_page([record]))

        assert engine.rows("act_1/adsets", fields=["id", "bid_amount", "targeting"]) == [record]


class TestReport:
    @staticmethod
    def _status(status: str, completion: int) -> _Reply:
        return _ok({"id": "9", "async_status": status, "async_percent_completion": completion})

    def test_runs_one_day_as_a_job_polls_it_and_reads_every_page_of_its_rows(self, sleeps):
        engine = _engine(
            _ok({"report_run_id": "9"}),
            self._status("Job Running", 50),
            self._status("Job Completed", 100),
            _page([{"ad_id": "1"}], after="A"),
            _page([{"ad_id": "2"}]),
        )

        rows = engine.report(
            "act_1/insights",
            dt.date(2026, 10, 1),
            fields=["ad_id", "spend"],
            level="ad",
            breakdowns=["country"],
        )

        assert rows == [{"ad_id": "1"}, {"ad_id": "2"}]
        assert _requests(engine) == [
            {
                "method": "POST",
                "url": "act_1/insights",
                "fields": _encoded(["ad_id", "spend"]),
                "level": "ad",
                "breakdowns": _encoded(["country"]),
                "time_range": _encoded({"since": "2026-10-01", "until": "2026-10-01"}),
            },
            {"method": "GET", "url": "9/"},
            {"method": "GET", "url": "9/"},
            {"method": "GET", "url": "9/insights"},
            {"method": "GET", "url": "9/insights", "after": "A"},
        ]
        assert sleeps == [FacebookMarketingApi.REPORT_POLL.total_seconds()]

    def test_a_refused_job_raises_facebooks_refusal(self, sleeps):
        engine = _engine(_error(400, 100, "(#100) Invalid breakdowns"))

        with pytest.raises(FacebookRequestError) as raised:
            engine.report("act_1/insights", dt.date(2026, 10, 1), fields=["spend"], breakdowns=["nonexisting"])
        assert raised.value.api_error_message() == "(#100) Invalid breakdowns"
        assert len(_requests(engine)) == 1

    def test_a_failed_job_raises(self, sleeps):
        engine = _engine(_ok({"report_run_id": "9"}), self._status("Job Failed", 40))

        with pytest.raises(FacebookError, match="Facebook insights report failed"):
            engine.report("act_1/insights", dt.date(2026, 10, 1), fields=["spend"], level="campaign")

    def test_a_job_that_does_not_complete_in_time_raises_its_last_status(self, sleeps, monkeypatch):
        monkeypatch.setattr(FacebookMarketingApi, "REPORT_TIMEOUT", dt.timedelta(0))
        engine = _engine(_ok({"report_run_id": "9"}), self._status("Job Running", 99))

        with pytest.raises(TimeoutError, match=r"did not complete within 0:00:00 \(last status: Job Running 99%\)"):
            engine.report("act_1/insights", dt.date(2026, 10, 1), fields=["spend"], level="campaign")
        assert sleeps == []
