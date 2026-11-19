"""Tests for the Graph API engine Facebook Insights and Instagram Insights read through.

Graph refuses a request with an error status and an ``error`` object carrying
its own ``code``, throttles with codes of its own rather than 429, pages an
edge along the absolute ``paging.next`` link, pages an insights edge by time
window, and reads list parameters as comma-separated names. These tests pin
each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.meta import api as graph
from interloper_assets.meta.api import Attempts, GraphApi

GRAPH = "https://graph.facebook.com/v26.0"


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(graph, "sleep", sleep)
    return slept


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> GraphApi:
    return GraphApi(il.AsyncRESTClient(GRAPH, transport=httpx2.MockTransport(handler)))


def _replies(*responses: httpx2.Response) -> Callable[[httpx2.Request], httpx2.Response]:
    queue = list(responses)
    return lambda _: queue.pop(0)


def _refused(status: int, code: int, message: str = "error", headers: dict[str, str] | None = None) -> httpx2.Response:
    body = {"error": {"message": message, "type": "OAuthException", "code": code}}
    return httpx2.Response(status, json=body, headers=headers)


def _business_usage(**limit: float) -> dict[str, str]:
    return {"x-business-use-case-usage": json.dumps({"42": [limit]})}


class TestGet:
    async def test_returns_the_object_as_graph_sends_it(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})

        assert await _api(handler).get("/42", fields="access_token") == {"access_token": "page-token", "id": "42"}
        (request,) = requests
        assert str(request.url) == f"{GRAPH}/42?fields=access_token"

    async def test_list_parameters_are_sent_comma_separated(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"id": "42"})

        await _api(handler).get("/42", fields=["id", "name"], limit=100)

        assert list(requests[0].url.params.multi_items()) == [("fields", "id,name"), ("limit", "100")]

    async def test_a_refusal_raises_its_error_status(self, sleeps):
        api = _api(_replies(_refused(400, 190, "Error validating access token")))

        with pytest.raises(httpx2.HTTPStatusError, match="400 Bad Request") as raised:
            await api.get("/42", fields="access_token")

        assert GraphApi.error_code(raised.value.response) == 190
        assert sleeps == []

    async def test_an_http_error_raises(self):
        api = _api(lambda _: httpx2.Response(404, text="<html>not found</html>"))

        with pytest.raises(httpx2.HTTPStatusError, match="404 Not Found"):
            await api.get("/42")


class TestRows:
    async def test_follows_the_next_link_across_pages(self):
        next_link = f"{GRAPH}/1784/media?fields=id&after=p2"
        requests: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(str(request.url))
            if request.url.params.get("after") == "p2":
                return httpx2.Response(200, json={"data": [{"id": "2"}], "paging": {"previous": "p"}})
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        assert await _api(handler).rows("/1784/media", fields=["id"]) == [{"id": "1"}, {"id": "2"}]
        assert requests == [f"{GRAPH}/1784/media?fields=id", next_link]

    async def test_list_parameters_are_sent_comma_separated(self):
        requests: list[httpx2.Request] = []
        insights = "insights.metric(post_clicks,post_media_view){name,values,id}"

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": []})

        await _api(handler).rows("/42/published_posts", fields=["id", "message", insights], since="2026-03-19")

        assert list(requests[0].url.params.multi_items()) == [
            ("fields", f"id,message,{insights}"),
            ("since", "2026-03-19"),
        ]

    async def test_a_next_link_is_followed_without_the_token_graph_copies_into_it(self):
        next_link = f"{GRAPH}/me/accounts?access_token=graph-token&limit=100&after=c2"
        requests: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(str(request.url))
            if request.url.params.get("after") == "c2":
                return httpx2.Response(200, json={"data": [{"id": "2"}]})
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        assert await _api(handler).rows("/me/accounts", limit=100) == [{"id": "1"}, {"id": "2"}]
        assert requests[1] == f"{GRAPH}/me/accounts?limit=100&after=c2"

    async def test_a_refusal_on_a_later_page_raises_without_the_token(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("after") == "c2":
                return _refused(400, 100, "(#100) Invalid parameter")
            next_link = f"{GRAPH}/me/accounts?access_token=graph-token&after=c2"
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        with pytest.raises(httpx2.HTTPStatusError, match="400 Bad Request") as raised:
            await _api(handler).rows("/me/accounts")

        assert GraphApi.error_code(raised.value.response) == 100
        assert "graph-token" not in str(raised.value)

    async def test_an_empty_edge_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [], "paging": {"cursors": {"before": "b", "after": "a"}}})

        assert await _api(handler).rows("/42/stories") == []
        assert len(requests) == 1

    @pytest.mark.parametrize("body", [{"data": [{"id": "1"}]}, {"data": [{"id": "1"}], "paging": None}])
    async def test_an_edge_without_a_next_link_is_read_once(self, body):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(500, text="runaway walk")
            return httpx2.Response(200, json=body)

        assert await _api(handler).rows("/42_1001/insights", metric="post_media_view") == [{"id": "1"}]
        assert len(requests) == 1

    async def test_a_page_without_data_raises(self):
        with pytest.raises(KeyError, match="data"):
            await _api(lambda _: httpx2.Response(200, json={"id": "42"})).rows("/me/accounts")


class TestReport:
    async def test_asks_an_insights_edge_for_one_day(self):
        requests: list[httpx2.Request] = []
        insight = {"name": "reach", "period": "day", "values": [{"value": 340}]}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [insight]})

        insights = await _api(handler).report(
            "/1784/insights", dt.date(2026, 9, 30), metric=["follower_count", "reach"], period="day"
        )

        assert insights == [insight]
        (request,) = requests
        assert request.url.path == "/v26.0/1784/insights"
        assert list(request.url.params.multi_items()) == [
            ("metric", "follower_count,reach"),
            ("period", "day"),
            ("since", "2026-09-30"),
            ("until", "2026-10-01"),
        ]

    async def test_reads_one_response_never_the_next_window(self):
        # Graph pages an insights edge by time window: next asks the same query for the following days
        requests: list[httpx2.Request] = []
        day = dt.date(2026, 9, 15)

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            since = dt.date.fromisoformat(request.url.params["since"])
            following, after = since + dt.timedelta(days=1), since + dt.timedelta(days=2)
            next_window = f"{GRAPH}/42/insights?metric=page_follows&since={following}&until={after}"
            paging = {"next": next_window} if since < day + dt.timedelta(days=3) else {}
            return httpx2.Response(200, json={"data": [{"name": str(since)}], "paging": paging})

        insights = await _api(handler).report("/42/insights", day, metric="page_follows")

        assert insights == [{"name": "2026-09-15"}]
        assert len(requests) == 1


class TestThrottling:
    async def test_a_throttled_request_waits_as_long_as_meta_says_then_repeats(self, sleeps):
        usage = _business_usage(call_count=100, estimated_time_to_regain_access=3)
        api = _api(_replies(_refused(400, 80001, "too many calls", usage), httpx2.Response(200, json={"id": "42"})))

        assert await api.get("/42") == {"id": "42"}
        assert sleeps == [180]

    @pytest.mark.parametrize("code", [4, 17, 32, 613, 80001, 80002])
    async def test_every_rate_limit_code_is_waited_out(self, sleeps, code):
        api = _api(_replies(_refused(403, code), httpx2.Response(200, json={"id": "42"})))

        assert await api.get("/42") == {"id": "42"}
        assert len(sleeps) == 1

    async def test_a_throttle_without_a_hint_backs_off_until_the_budget_is_spent(self, sleeps):
        api = _api(_replies(*[_refused(400, 17, "User request limit reached")] * 20))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.get("/42")

        assert GraphApi.error_code(raised.value.response) == 17
        assert sleeps[:5] == [60, 120, 240, 480, 600]
        assert sum(sleeps) <= Attempts.THROTTLE_BUDGET.total_seconds()

    async def test_a_throttled_page_is_repeated_alone(self, sleeps):
        next_link = f"{GRAPH}/1784/media?after=p2"
        requests: list[str] = []
        page_two = [_refused(400, 4, "Application request limit reached"), httpx2.Response(200, json={"data": [2]})]

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(str(request.url))
            if request.url.params.get("after") == "p2":
                return page_two.pop(0)
            return httpx2.Response(200, json={"data": [1], "paging": {"next": next_link}})

        assert await _api(handler).rows("/1784/media") == [1, 2]
        assert requests == [f"{GRAPH}/1784/media", next_link, next_link]
        assert sleeps == [60]

    async def test_any_other_refusal_is_raised_at_once(self, sleeps):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _refused(400, 100, "(#100) Invalid parameter")

        with pytest.raises(httpx2.HTTPStatusError):
            await _api(handler).rows("/1784/media")

        assert (sleeps, len(requests)) == ([], 1)


class TestErrorCode:
    def test_reads_graphs_own_code(self):
        assert GraphApi.error_code(_refused(400, 100, "(#100) Invalid parameter")) == 100

    def test_a_body_that_is_not_json_has_no_code(self):
        assert GraphApi.error_code(httpx2.Response(400, text="<html>bad</html>")) is None

    def test_a_body_without_an_error_object_has_no_code(self):
        assert GraphApi.error_code(httpx2.Response(400, json={"message": "bad"})) is None
