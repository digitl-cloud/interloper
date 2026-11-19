"""Tests for the LinkedIn REST API engine LinkedIn Ads and LinkedIn Organic share.

LinkedIn reads its query in Rest.li 2.0 syntax, which an ordinary query
encoder breaks, wraps every list in ``elements``, pages a collection by the
``next`` link in ``paging.links``, a search finder by a ``nextPageToken``
cursor and an older finder by ``start``/``count``, and refuses a request with
an HTTP error status. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.linkedin.api import LinkedinApi, LinkedinApiError

BASE_URL = "https://api.linkedin.com/rest"
DAY = dt.date(2026, 9, 15)


@pytest.fixture
def berlin_clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run the test on a machine whose local zone is Berlin, then restore the zone.

    Args:
        monkeypatch: Sets ``TZ`` for the test only.

    Yields:
        Nothing; the zone is Berlin while the test runs.
    """
    monkeypatch.setenv("TZ", "Europe/Berlin")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> LinkedinApi:
    return LinkedinApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _params(request: httpx2.Request) -> dict[str, str]:
    pairs = [part.split("=", 1) for part in request.url.query.decode().split("&") if part]
    params = dict(pairs)
    assert len(params) == len(pairs), f"a parameter is sent twice: {pairs}"
    return params


def _recording(sent: list[str], respond: Callable[[httpx2.Request], httpx2.Response]) -> Any:
    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(str(request.url))
        return respond(request)

    return handler


def _page(elements: list[dict[str, Any]], next_href: str | None = None) -> httpx2.Response:
    links = [{"rel": "prev", "href": "/rest/functions?start=0&count=10"}]
    if next_href is not None:
        links.append({"type": "application/json", "rel": "next", "href": next_href})
    return httpx2.Response(200, json={"elements": elements, "paging": {"start": 0, "count": 10, "links": links}})


class TestGet:
    async def test_returns_the_body_of_one_response(self):
        requests: list[httpx2.Request] = []
        account = {"id": 123, "name": "Acme"}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=account)

        body = await _api(handler).get("/adAccounts/123", fields=["id", "name"])

        assert body == account
        (request,) = requests
        assert request.method == "GET"
        assert request.url.path == "/rest/adAccounts/123"
        assert request.url.query.decode() == "fields=id,name"

    async def test_a_path_without_parameters_is_sent_without_a_query(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={})

        await _api(handler).get("/adAccounts/123")

        assert str(requests[0].url) == "https://api.linkedin.com/rest/adAccounts/123"

    async def test_a_refusal_raises_linkedins_own_code_and_message_without_the_url(self):
        refused = {
            "status": 401,
            "serviceErrorCode": 65601,
            "code": "REVOKED_ACCESS_TOKEN",
            "message": "The token used in the request has been revoked by the user",
        }
        api = _api(lambda _: httpx2.Response(401, json=refused))

        with pytest.raises(LinkedinApiError) as raised:
            await api.get("/adAccounts/123", fields=["id"])

        assert str(raised.value) == (
            "LinkedIn refused the request (HTTP 401, REVOKED_ACCESS_TOKEN, service error 65601): "
            "The token used in the request has been revoked by the user"
        )
        assert raised.value.code == "REVOKED_ACCESS_TOKEN"
        assert raised.value.service_error_code == 65601

    async def test_a_refusal_is_still_its_http_error_status(self):
        api = _api(lambda _: httpx2.Response(403, json={"status": 403, "message": "Not enough permissions"}))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.get("/adAccounts/123")

        assert raised.value.response.status_code == 403

    async def test_a_refusal_with_only_a_service_error_names_it(self):
        refused = {"message": "Empty oauth2_access_token", "serviceErrorCode": 401, "status": 401}

        with pytest.raises(LinkedinApiError) as raised:
            await _api(lambda _: httpx2.Response(401, json=refused)).get("/adAccounts/123")

        assert (
            str(raised.value) == "LinkedIn refused the request (HTTP 401, service error 401): Empty oauth2_access_token"
        )
        assert raised.value.code is None

    @pytest.mark.parametrize("body", [{"text": "<html>denied</html>"}, {"json": ["not", "an", "error"]}])
    async def test_an_http_error_without_a_linkedin_body_raises_its_status(self, body: dict[str, Any]):
        with pytest.raises(LinkedinApiError) as raised:
            await _api(lambda _: httpx2.Response(502, **body)).get("/adAccounts/123")

        assert str(raised.value) == "LinkedIn refused the request (HTTP 502): Bad Gateway"
        assert raised.value.code is None
        assert raised.value.service_error_code is None


class TestRestliQuery:
    async def _sent(self, **params: Any) -> dict[str, str]:
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={})

        await _api(handler).get("/adAnalytics", **params)
        return _params(requests[0])

    async def test_lists_records_and_days_are_written_in_restli_syntax(self):
        sent = await self._sent(
            pivots=["SHARE", "CAMPAIGN"],
            timeGranularity={"value": "DAILY"},
            dateRange={"start": DAY, "end": DAY},
            search={"status": {"values": ["ACTIVE", "PAUSED"]}},
        )

        assert sent == {
            "pivots": "List(SHARE,CAMPAIGN)",
            "timeGranularity": "(value:DAILY)",
            "dateRange": "(start:(year:2026,month:9,day:15),end:(year:2026,month:9,day:15))",
            "search": "(status:(values:List(ACTIVE,PAUSED)))",
        }

    async def test_a_value_is_percent_encoded_down_to_its_reserved_characters(self):
        sent = await self._sent(accounts=["urn:li:sponsoredAccount:123"], name="a,b (c)/d")

        assert sent == {"accounts": "List(urn%3Ali%3AsponsoredAccount%3A123)", "name": "a%2Cb%20%28c%29%2Fd"}

    async def test_numbers_and_booleans_are_written_as_restli_reads_them(self):
        sent = await self._sent(count=10, test=True, archived=False)

        assert sent == {"count": "10", "test": "true", "archived": "false"}

    async def test_the_projection_is_a_bare_comma_separated_list(self):
        sent = await self._sent(q="statistics", fields=["pivotValues", "dateRange", "impressions"])

        assert sent == {"q": "statistics", "fields": "pivotValues,dateRange,impressions"}

    async def test_values_are_percent_encoded_and_structures_written_in_rest_li_syntax(self):
        sent: list[str] = []

        await _api(_recording(sent, lambda _: _page([]))).rows(
            "/organizationalEntityShareStatistics",
            q="organizationalEntity",
            organizationalEntity="urn:li:organization:2414183",
            shares=["urn:li:share:1", "urn:li:share:2"],
            timeIntervals={"timeGranularityType": "DAY", "timeRange": {"start": 1, "end": 2}},
            count=100,
        )

        assert sent == [
            (
                f"{BASE_URL}/organizationalEntityShareStatistics?q=organizationalEntity"
                "&organizationalEntity=urn%3Ali%3Aorganization%3A2414183"
                "&shares=List(urn%3Ali%3Ashare%3A1,urn%3Ali%3Ashare%3A2)"
                "&timeIntervals=(timeGranularityType:DAY,timeRange:(start:1,end:2))"
                "&count=100"
            )
        ]

    async def test_a_projection_is_sent_percent_encoded_like_any_value(self):
        sent: list[str] = []

        await _api(_recording(sent, lambda _: _page([]))).rows(
            "/organizationAcls", projection="(elements*(organization~(id,localizedName)),paging)"
        )

        assert sent == [
            f"{BASE_URL}/organizationAcls?projection=%28elements%2A%28organization~%28id%2ClocalizedName%29%29%2Cpaging%29"
        ]


class TestRows:
    async def test_every_page_is_read_through_its_next_link(self):
        sent: list[str] = []

        def respond(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("start") == "10":
                return _page([{"id": 2}])
            return _page([{"id": 1}], next_href="/rest/functions?start=10&count=10")

        assert await _api(_recording(sent, respond)).rows("/functions") == [{"id": 1}, {"id": 2}]
        assert sent == [f"{BASE_URL}/functions", f"{BASE_URL}/functions?start=10&count=10"]

    async def test_an_absolute_next_link_is_followed_as_is(self):
        sent: list[str] = []
        next_page = "https://api.linkedin.com/rest/functions?start=10&count=10"

        def respond(request: httpx2.Request) -> httpx2.Response:
            return _page([{"id": 2}]) if str(request.url) == next_page else _page([{"id": 1}], next_href=next_page)

        assert await _api(_recording(sent, respond)).rows("/functions") == [{"id": 1}, {"id": 2}]
        assert sent == [f"{BASE_URL}/functions", next_page]

    async def test_a_next_link_keeps_its_encoded_query(self):
        sent: list[str] = []
        next_href = "/rest/organizationAcls?count=100&projection=%28elements%2Cpaging%29&q=roleAssignee&start=100"

        def respond(request: httpx2.Request) -> httpx2.Response:
            return _page([]) if "start=" in str(request.url) else _page([{"id": 1}], next_href=next_href)

        await _api(_recording(sent, respond)).rows("/organizationAcls", q="roleAssignee")
        assert sent[1] == f"https://api.linkedin.com{next_href}"

    async def test_a_refusal_on_a_later_page_raises(self):
        def respond(request: httpx2.Request) -> httpx2.Response:
            if "start=" in str(request.url):
                return httpx2.Response(403, json={"status": 403, "message": "Not enough permissions"})
            return _page([{"id": 1}], next_href="/rest/functions?start=10&count=10")

        with pytest.raises(LinkedinApiError, match=re.escape("(HTTP 403): Not enough permissions")):
            await _api(respond).rows("/functions")

    async def test_an_http_error_raises_before_the_body_is_read(self):
        api = _api(lambda _: httpx2.Response(401, text="<html>denied</html>"))

        with pytest.raises(LinkedinApiError, match=re.escape("(HTTP 401): Unauthorized")):
            await api.rows("/functions")

    async def test_an_empty_collection_reads_one_page(self):
        sent: list[str] = []

        assert await _api(_recording(sent, lambda _: _page([]))).rows("/functions") == []
        assert len(sent) == 1

    async def test_a_response_without_paging_is_read_once(self):
        sent: list[str] = []

        def respond(_: httpx2.Request) -> httpx2.Response:
            # a finder that does not page answers the same elements again; end a runaway walk
            if len(sent) > 3:
                return httpx2.Response(400, text="runaway walk")
            return httpx2.Response(200, json={"elements": [{"id": 1}]})

        assert await _api(_recording(sent, respond)).rows("/organizationalEntityFollowerStatistics") == [{"id": 1}]
        assert len(sent) == 1

    async def test_an_empty_page_ends_the_walk_even_when_it_links_a_next_one(self):
        sent: list[str] = []

        def respond(request: httpx2.Request) -> httpx2.Response:
            start = int(request.url.params.get("start", "0"))
            elements = [{"id": 1}] if start == 0 else []
            return _page(elements, next_href=f"/rest/functions?start={start + 10}&count=10")

        assert await _api(_recording(sent, respond)).rows("/functions") == [{"id": 1}]
        assert sent == [f"{BASE_URL}/functions", f"{BASE_URL}/functions?start=10&count=10"]

    async def test_a_response_without_elements_has_no_rows(self):
        assert await _api(lambda _: httpx2.Response(200, json={"paging": {"links": []}})).rows("/functions") == []


class TestCursorRows:
    async def test_every_page_is_read_by_its_token(self):
        requests: list[httpx2.Request] = []
        pages = {
            None: {"elements": [{"id": 1}], "metadata": {"nextPageToken": "tok/1"}},
            "tok%2F1": {"elements": [{"id": 2}], "metadata": {}},
        }

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=pages[_params(request).get("pageToken")])

        rows = await _api(handler).cursor_rows("/adAccounts/123/adCampaigns", q="search", fields=["id"])

        assert rows == [{"id": 1}, {"id": 2}]
        assert [request.url.path for request in requests] == ["/rest/adAccounts/123/adCampaigns"] * 2
        assert _params(requests[0]) == {"q": "search", "fields": "id", "pageSize": "1000"}
        assert _params(requests[1]) == {"q": "search", "fields": "id", "pageSize": "1000", "pageToken": "tok%2F1"}

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "pageToken" in _params(request):
                return httpx2.Response(403, json={"status": 403, "code": "ACCESS_DENIED"})
            return httpx2.Response(200, json={"elements": [{"id": 1}], "metadata": {"nextPageToken": "2"}})

        with pytest.raises(LinkedinApiError, match=re.escape("(HTTP 403, ACCESS_DENIED): Forbidden")):
            await _api(handler).cursor_rows("/adAccounts/123/adCampaigns", q="search")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 1000}, "elements": []})

        assert await _api(handler).cursor_rows("/adAccounts/123/adCampaigns", q="search") == []
        assert len(requests) == 1

    @pytest.mark.parametrize("paging", [{}, {"metadata": None}, {"metadata": {}}])
    async def test_a_response_without_paging_info_is_read_once(self, paging: dict[str, Any]):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={})
            return httpx2.Response(200, json={"elements": [{"id": 1}], **paging})

        assert await _api(handler).cursor_rows("/adAccounts/123/adCampaigns", q="search") == [{"id": 1}]
        assert len(requests) == 1


class TestOffsetRows:
    async def test_every_page_is_read_until_an_empty_one(self):
        sent: list[str] = []
        pages = {"0": [{"id": 1}, {"id": 2}], "100": [{"id": 3}], "200": []}

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(f"{request.url.path}?{request.url.query.decode()}")
            return httpx2.Response(200, json={"elements": pages[request.url.params["start"]]})

        rows = await _api(handler).offset_rows("/adAccounts", q="search")

        assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert sent == [
            "/rest/adAccounts?q=search&count=100&start=0",
            "/rest/adAccounts?q=search&count=100&start=100",
            "/rest/adAccounts?q=search&count=100&start=200",
        ]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["start"] != "0":
                return httpx2.Response(403, json={"status": 403, "code": "ACCESS_DENIED"})
            return httpx2.Response(200, json={"elements": [{"id": 1}]})

        with pytest.raises(LinkedinApiError, match=re.escape("(HTTP 403, ACCESS_DENIED): Forbidden")):
            await _api(handler).offset_rows("/adAccounts", q="search")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 100, "total": 0}, "elements": []})

        assert await _api(handler).offset_rows("/adAccounts", q="search") == []
        assert len(requests) == 1

    async def test_a_page_without_elements_fails_rather_than_ending_the_list(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["start"] == "0":
                return httpx2.Response(200, json={"elements": [{"id": 1}]})
            return httpx2.Response(200, json={"paging": {"start": 100, "count": 100}})

        with pytest.raises(KeyError, match="elements"):
            await _api(handler).offset_rows("/adAccounts", q="search")

    async def test_an_endpoint_that_ignores_the_offset_is_read_as_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that ignores ``start`` answers the same page forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={"elements": []})
            return httpx2.Response(200, json={"elements": [{"id": 1}, {"id": 2}]})

        assert await _api(handler).offset_rows("/adAccounts", q="search") == [{"id": 1}, {"id": 2}]
        assert len(requests) == 2


class TestReport:
    async def test_asks_ad_analytics_for_one_day(self):
        requests: list[httpx2.Request] = []
        row = {"pivotValues": ["urn:li:sponsoredCampaign:42"], "impressions": 3}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 10}, "elements": [row]})

        rows = await _api(handler).report(
            DAY,
            q="analytics",
            pivot="CAMPAIGN",
            timeGranularity={"value": "DAILY"},
            accounts=["urn:li:sponsoredAccount:123"],
            fields=["pivotValues", "impressions"],
        )

        assert rows == [row]
        (request,) = requests
        assert request.method == "GET"
        assert request.url.path == "/rest/adAnalytics"
        assert _params(request) == {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "timeGranularity": "(value:DAILY)",
            "accounts": "List(urn%3Ali%3AsponsoredAccount%3A123)",
            "fields": "pivotValues,impressions",
            "dateRange": "(start:(year:2026,month:9,day:15),end:(year:2026,month:9,day:15))",
        }

    async def test_a_day_without_rows_is_empty(self):
        assert await _api(lambda _: httpx2.Response(200, json={})).report(DAY, q="statistics") == []


class TestStatistics:
    async def test_asks_a_statistics_finder_for_one_utc_day(self):
        sent: list[str] = []
        row = {"timeRange": {"start": 1789430400000, "end": 1789516800000}, "totalShareStatistics": {"clickCount": 1}}

        rows = await _api(_recording(sent, lambda _: httpx2.Response(200, json={"elements": [row]}))).statistics(
            "/organizationalEntityShareStatistics",
            dt.date(2026, 9, 15),
            q="organizationalEntity",
            organizationalEntity="urn:li:organization:2414183",
        )

        assert rows == [row]
        assert sent == [
            (
                f"{BASE_URL}/organizationalEntityShareStatistics?q=organizationalEntity"
                "&organizationalEntity=urn%3Ali%3Aorganization%3A2414183"
                "&timeIntervals=(timeGranularityType:DAY,timeRange:(start:1789430400000,end:1789516800000))"
            )
        ]

    @pytest.mark.usefixtures("berlin_clock")
    async def test_the_day_is_the_utc_day_whatever_the_machines_zone(self):
        sent: list[str] = []

        await _api(_recording(sent, lambda _: _page([]))).statistics(
            "/organizationPageStatistics", dt.date(2026, 10, 25), q="organization", organization="urn:li:organization:1"
        )

        # 2026-10-25 is the day Berlin leaves summer time; the window is still midnight to midnight UTC
        assert "timeRange:(start:1792886400000,end:1792972800000)" in sent[0]
