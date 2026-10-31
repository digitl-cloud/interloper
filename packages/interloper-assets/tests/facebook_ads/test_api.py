"""Tests for FacebookGraphApi: every Graph API request waits, retries and asks for less in one place."""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests
from facebook_business.exceptions import FacebookRequestError
from requests.structures import CaseInsensitiveDict

from interloper_assets.facebook_ads import api as graph
from interloper_assets.facebook_ads.api import Attempts, FacebookGraphApi

REFUSAL = "Please reduce the amount of data you're asking for, then retry your request"


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

    def test_a_busy_account_is_paced_before_its_next_request(self, sleeps):
        busy = {"x-app-usage": json.dumps({"call_count": 95, "total_time": 10, "total_cputime": 10})}
        regain = _business_usage(total_time=91, estimated_time_to_regain_access=4)
        calm = {"x-ad-account-usage": json.dumps({"acc_id_util_pct": 40, "reset_time_duration": 0})}
        api = _api(_ok(headers=busy), _ok(headers=regain), _ok(headers=calm))
        for _ in range(3):
            api.call("GET", ("act_1", ""))
        assert sleeps == [60, 240]

    def test_the_async_insights_throttle_paces_too(self, sleeps):
        insights = {"app_id_util_pct": 95, "acc_id_util_pct": 10, "ads_api_access_tier": "development_access"}
        api = _api(_ok(headers={"x-fb-ads-insights-throttle": json.dumps(insights)}))
        api.call("GET", ("act_1", "insights"))
        assert sleeps == [60]

    def test_usage_headers_are_read_by_their_measures_alone(self, sleeps):
        headers = {
            "x-app-usage": json.dumps({"call_count": 10, "total_time": 5, "total_cputime": 5, "type": "app"}),
            "x-ad-account-usage": json.dumps({"acc_id_util_pct": 20, "ads_api_access_tier": "standard_access"}),
        }
        api = _api(_ok(headers=headers))
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
