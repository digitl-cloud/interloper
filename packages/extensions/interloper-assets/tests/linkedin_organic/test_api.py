"""Tests for the LinkedIn Organic API engine.

LinkedIn reads its queries in Rest.li 2.0 syntax and rejects the structure
percent-encoded, answers a collection's ``elements`` with its next page in
``paging.links``, and refuses with an HTTP error status. These tests pin each
against a mock transport.
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

from interloper_assets.linkedin_organic.api import LinkedinOrganicApi
from interloper_assets.linkedin_organic.constants import BASE_URL


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


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> LinkedinOrganicApi:
    return LinkedinOrganicApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


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

        with pytest.raises(
            httpx2.HTTPStatusError,
            match=re.escape(f"Client error '403 Forbidden' for url '{BASE_URL}/functions?start=10&count=10'"),
        ):
            await _api(respond).rows("/functions")

    async def test_an_http_error_raises_before_the_body_is_read(self):
        api = _api(lambda _: httpx2.Response(401, text="<html>denied</html>"))

        with pytest.raises(httpx2.HTTPStatusError, match="401 Unauthorized"):
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


class TestQuery:
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


class TestReport:
    async def test_asks_a_statistics_finder_for_one_utc_day(self):
        sent: list[str] = []
        row = {"timeRange": {"start": 1789430400000, "end": 1789516800000}, "totalShareStatistics": {"clickCount": 1}}

        rows = await _api(_recording(sent, lambda _: httpx2.Response(200, json={"elements": [row]}))).report(
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

        await _api(_recording(sent, lambda _: _page([]))).report(
            "/organizationPageStatistics", dt.date(2026, 10, 25), q="organization", organization="urn:li:organization:1"
        )

        # 2026-10-25 is the day Berlin leaves summer time; the window is still midnight to midnight UTC
        assert "timeRange:(start:1792886400000,end:1792972800000)" in sent[0]
