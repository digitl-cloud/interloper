"""Tests for The Trade Desk API engine.

The Trade Desk refuses a REST request with an HTTP status, but a GraphQL query
with HTTP 200 and an ``errors`` list; GraphQL connections page by cursor; a
MyReports report is a one-time schedule whose execution is polled until it
delivers a CSV. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.thetradedesk import constants
from interloper_assets.thetradedesk.api import TheTradeDeskApi

DAY = dt.date(2026, 10, 1)
SCHEDULES = f"{constants.BASE_URL}/myreports/reportschedule"
EXECUTIONS = f"{constants.BASE_URL}/myreports/reportexecution/query/partners"
DOWNLOAD_URL = "https://download.example/report.csv"
OTHER_DOWNLOAD_URL = "https://download.example/other.csv"
CSV = 'Date,Advertiser ID,Impressions\n01/10/2026,adv1,"1,5"\n'


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> TheTradeDeskApi:
    return TheTradeDeskApi(il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler)))


def _page(path: tuple[str, ...], nodes: list[dict[str, Any]], cursor: str | None = None) -> dict[str, Any]:
    listing: dict[str, Any] = {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}
    for field in reversed(path):
        listing = {field: listing}
    return {"data": listing}


class _MyReports:
    """A MyReports server whose execution reports each given state in turn, then stays in the last."""

    def __init__(
        self,
        *states: str | None,
        csv: str = CSV,
        delete_status: int = 200,
        refused: str = "",
        duplicated: bool = False,
    ) -> None:
        """Set up the server.

        Args:
            *states: The execution's state on each poll; None for an execution not listed yet.
            csv: The delivery's content.
            delete_status: The HTTP status the schedule's deletion answers.
            refused: A URL answered with HTTP 403; none when empty.
            duplicated: Whether the schedule lists a second execution and each execution a
                second delivery, whose URL the server does not answer.
        """
        self.states = list(states)
        self.csv = csv
        self.delete_status = delete_status
        self.refused = refused
        self.duplicated = duplicated
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request as MyReports would.

        Args:
            request: The request the engine sent.

        Returns:
            The response for the request's URL.

        Raises:
            AssertionError: If the engine sends a request MyReports has no answer for.
        """
        self.requests.append(request)
        url = str(request.url)
        if url == self.refused:
            return httpx2.Response(403, json={"Message": "Forbidden"})
        if url == SCHEDULES:
            return httpx2.Response(200, json={"ReportScheduleId": 42})
        if url == EXECUTIONS:
            state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
            if state is None:
                return httpx2.Response(200, json={"Result": []})
            deliveries = [{"DownloadURL": DOWNLOAD_URL}]
            if self.duplicated:
                deliveries.append({"DownloadURL": OTHER_DOWNLOAD_URL})
            execution = {"ReportExecutionState": state, "ReportDeliveries": deliveries}
            return httpx2.Response(200, json={"Result": [execution, execution] if self.duplicated else [execution]})
        if url == DOWNLOAD_URL:
            return httpx2.Response(200, text=self.csv)
        if url == f"{SCHEDULES}/42":
            return httpx2.Response(self.delete_status)
        raise AssertionError(f"unexpected request {request.method} {url}")

    def sent(self) -> list[tuple[str, str]]:
        """List the requests the server received.

        Returns:
            Each request's method and URL, in order.
        """
        return [(request.method, str(request.url)) for request in self.requests]


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the engine's waits between polls instead of sleeping through them.

    Args:
        monkeypatch: Replaces the engine's ``sleep`` for the test only.

    Returns:
        The seconds of every wait, in order.
    """
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr("interloper_assets.thetradedesk.api.sleep", sleep)
    return waits


class TestQuery:
    async def test_sends_the_first_page_window_with_the_filters(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"Result": [{"PartnerId": "p1"}], "ResultCount": 1})

        assert await _api(handler).query("/partner/query", PartnerIds=["p1"]) == [{"PartnerId": "p1"}]
        (request,) = requests
        assert (request.method, str(request.url)) == ("POST", f"{constants.BASE_URL}/partner/query")
        assert json.loads(request.content) == {"PageStartIndex": 0, "PageSize": 100, "PartnerIds": ["p1"]}

    async def test_reads_every_page_until_the_filtered_total(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(constants, "QUERY_PAGE_SIZE", 2)
        partners = [{"PartnerId": f"p{index}"} for index in range(5)]
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            body = json.loads(request.content)
            bodies.append(body)
            page = partners[body["PageStartIndex"] : body["PageStartIndex"] + body["PageSize"]]
            return httpx2.Response(
                200, json={"Result": page, "ResultCount": len(page), "TotalFilteredCount": len(partners)}
            )

        assert await _api(handler).query("/partner/query", PartnerIds=["p"]) == partners
        assert bodies == [
            {"PageStartIndex": 0, "PageSize": 2, "PartnerIds": ["p"]},
            {"PageStartIndex": 2, "PageSize": 2, "PartnerIds": ["p"]},
            {"PageStartIndex": 4, "PageSize": 2, "PartnerIds": ["p"]},
        ]

    async def test_a_response_without_a_filtered_total_is_read_once(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(constants, "QUERY_PAGE_SIZE", 2)
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"Result": [{"PartnerId": "p0"}, {"PartnerId": "p1"}], "ResultCount": 2})

        assert await _api(handler).query("/partner/query") == [{"PartnerId": "p0"}, {"PartnerId": "p1"}]
        assert len(requests) == 1

    async def test_an_empty_page_ends_the_walk_short_of_the_total(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(constants, "QUERY_PAGE_SIZE", 2)
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            page = [{"PartnerId": "p0"}, {"PartnerId": "p1"}] if len(requests) == 1 else []
            return httpx2.Response(200, json={"Result": page, "ResultCount": len(page), "TotalFilteredCount": 9})

        assert await _api(handler).query("/partner/query") == [{"PartnerId": "p0"}, {"PartnerId": "p1"}]
        assert len(requests) == 2

    async def test_an_empty_result_reads_as_no_rows(self):
        api = _api(lambda _: httpx2.Response(200, json={"Result": [], "ResultCount": 0}))

        assert await api.query("/partner/query") == []

    async def test_a_null_result_reads_as_no_rows(self):
        api = _api(lambda _: httpx2.Response(200, json={"Result": None}))

        assert await api.query("/partner/query") == []

    async def test_a_response_without_a_result_reads_as_no_rows(self):
        api = _api(lambda _: httpx2.Response(200, json={"ResultCount": 0}))

        assert await api.query("/partner/query") == []

    async def test_a_refusal_raises_with_the_trade_desks_message(self):
        api = _api(lambda _: httpx2.Response(403, json={"Message": "Forbidden"}))

        with pytest.raises(
            httpx2.HTTPStatusError, match="The Trade Desk refused POST /v3/partner/query with HTTP 403: Forbidden"
        ) as raised:
            await api.query("/partner/query")
        assert raised.value.response.status_code == 403

    async def test_a_refusal_without_a_message_raises_its_status(self):
        api = _api(lambda _: httpx2.Response(502, text="<html>Bad Gateway</html>"))

        with pytest.raises(httpx2.HTTPStatusError, match="502"):
            await api.query("/partner/query")


class TestNodes:
    async def test_follows_the_cursor_until_the_last_page(self):
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert str(request.url) == constants.GRAPHQL_URL
            body = json.loads(request.content)
            bodies.append(body)
            if body["variables"]["after"] is None:
                return httpx2.Response(200, json=_page(("partner", "advertisers"), [{"id": "adv1"}], cursor="next"))
            return httpx2.Response(200, json=_page(("partner", "advertisers"), [{"id": "adv2"}]))

        rows = await _api(handler).nodes(constants.ADVERTISER_IDS_QUERY, ("partner", "advertisers"), partnerId="p")

        assert rows == [{"id": "adv1"}, {"id": "adv2"}]
        assert bodies == [
            {"query": constants.ADVERTISER_IDS_QUERY, "variables": {"partnerId": "p", "first": 100, "after": None}},
            {"query": constants.ADVERTISER_IDS_QUERY, "variables": {"partnerId": "p", "first": 100, "after": "next"}},
        ]

    async def test_errors_answered_at_200_fail_the_query(self):
        api = _api(
            lambda _: httpx2.Response(200, json={"errors": [{"message": "AUTHENTICATION_FAILURE"}], "data": None})
        )

        with pytest.raises(RuntimeError, match="The Trade Desk GraphQL query failed: AUTHENTICATION_FAILURE"):
            await api.nodes(constants.ADVERTISERS_QUERY, ("partner", "advertisers"), partnerId="p")

    async def test_a_refusal_on_a_later_page_fails_the_query(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if json.loads(request.content)["variables"]["after"] is None:
                return httpx2.Response(200, json=_page(("partner", "advertisers"), [{"id": "adv1"}], cursor="next"))
            return httpx2.Response(200, json={"errors": [{"message": "RATE_LIMITED"}, {"code": 7}]})

        with pytest.raises(RuntimeError, match=r"query failed: RATE_LIMITED; \{'code': 7\}"):
            await _api(handler).nodes(constants.ADVERTISERS_QUERY, ("partner", "advertisers"), partnerId="p")

    async def test_an_http_error_raises(self):
        api = _api(lambda _: httpx2.Response(401, text="<html>denied</html>"))

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await api.nodes(constants.ADVERTISERS_QUERY, ("partner", "advertisers"), partnerId="p")

    async def test_an_unseen_parent_fails_rather_than_reading_as_no_nodes(self):
        api = _api(lambda _: httpx2.Response(200, json={"data": {"advertiser": None}}))

        with pytest.raises(
            RuntimeError,
            match=r"The Trade Desk returned no advertiser for \{'advertiserId': 'a'\}: the API key cannot see it",
        ):
            await api.nodes(constants.CAMPAIGNS_QUERY, ("advertiser", "campaigns"), advertiserId="a")

    async def test_a_parent_without_the_connection_reads_as_no_nodes(self):
        api = _api(lambda _: httpx2.Response(200, json={"data": {"advertiser": {"campaigns": None}}}))

        assert await api.nodes(constants.CAMPAIGNS_QUERY, ("advertiser", "campaigns"), advertiserId="a") == []

    async def test_an_empty_connection_reads_as_no_nodes(self):
        api = _api(lambda _: httpx2.Response(200, json=_page(("advertiser", "campaigns"), [])))

        assert await api.nodes(constants.CAMPAIGNS_QUERY, ("advertiser", "campaigns"), advertiserId="a") == []

    @pytest.mark.parametrize("page_info", [{"hasNextPage": True, "endCursor": None}, {"hasNextPage": True}])
    async def test_a_next_page_without_a_cursor_raises(self, page_info: dict[str, Any]):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # without a cursor the walk would ask for the first page forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={"errors": [{"message": "runaway walk"}]})
            listing = {"nodes": [{"id": "c1"}], "pageInfo": page_info}
            return httpx2.Response(200, json={"data": {"advertiser": {"campaigns": listing}}})

        with pytest.raises(
            RuntimeError,
            match=r"The Trade Desk said advertiser.campaigns for \{'advertiserId': 'a'\} has a next page "
            r"but gave no cursor",
        ):
            await _api(handler).nodes(constants.CAMPAIGNS_QUERY, ("advertiser", "campaigns"), advertiserId="a")
        assert len(requests) == 1

    async def test_a_connection_without_page_info_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # a connection that does not page would answer the same nodes forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={"errors": [{"message": "runaway walk"}]})
            return httpx2.Response(200, json={"data": {"advertiser": {"campaigns": {"nodes": [{"id": "c1"}]}}}})

        rows = await _api(handler).nodes(constants.CAMPAIGNS_QUERY, ("advertiser", "campaigns"), advertiserId="a")

        assert rows == [{"id": "c1"}]
        assert len(requests) == 1


class TestAdvertiserNodes:
    async def test_lists_the_partners_advertisers_then_reads_each_ones_connection(self):
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            body = json.loads(request.content)
            bodies.append(body)
            if "partnerId" in body["variables"]:
                return httpx2.Response(200, json=_page(("partner", "advertisers"), [{"id": "adv1"}, {"id": "adv2"}]))
            advertiser = body["variables"]["advertiserId"]
            return httpx2.Response(200, json=_page(("advertiser", "adGroups"), [{"id": f"ag-{advertiser}"}]))

        rows = await _api(handler).advertiser_nodes("p", constants.AD_GROUPS_QUERY, "adGroups")

        assert sorted(row["id"] for row in rows) == ["ag-adv1", "ag-adv2"]
        assert bodies[0] == {
            "query": constants.ADVERTISER_IDS_QUERY,
            "variables": {"partnerId": "p", "first": 100, "after": None},
        }
        assert sorted(body["variables"]["advertiserId"] for body in bodies[1:]) == ["adv1", "adv2"]
        assert {body["query"] for body in bodies[1:]} == {constants.AD_GROUPS_QUERY}

    async def test_forwards_the_variables_to_each_advertisers_query(self):
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            body = json.loads(request.content)
            bodies.append(body)
            if "partnerId" in body["variables"]:
                return httpx2.Response(200, json=_page(("partner", "advertisers"), [{"id": "adv1"}]))
            return httpx2.Response(200, json=_page(("advertiser", "campaigns"), []))

        await _api(handler).advertiser_nodes("p", constants.CAMPAIGNS_QUERY, "campaigns", isArchived=False)

        assert [body["variables"] for body in bodies] == [
            {"partnerId": "p", "first": 100, "after": None},
            {"isArchived": False, "advertiserId": "adv1", "first": 100, "after": None},
        ]

    async def test_a_partner_without_advertisers_reads_nothing_more(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=_page(("partner", "advertisers"), []))

        assert await _api(handler).advertiser_nodes("p", constants.CAMPAIGNS_QUERY, "campaigns") == []
        assert len(requests) == 1


class TestReport:
    async def test_runs_a_one_time_schedule_for_the_day_and_deletes_it(self, sleeps: list[float]):
        server = _MyReports("Complete")

        rows = await _api(server).report(
            "p", "168005", DAY, ReportDateFormat="US", ReportNumericFormat="US", TimeZone="Europe/Berlin"
        )

        assert rows == [{"Date": "01/10/2026", "Advertiser ID": "adv1", "Impressions": "1,5"}]
        assert server.sent() == [
            ("POST", SCHEDULES),
            ("POST", EXECUTIONS),
            ("GET", DOWNLOAD_URL),
            ("DELETE", f"{SCHEDULES}/42"),
        ]
        assert json.loads(server.requests[0].content) == {
            "RequestedByUserName": "interloper",
            "IncludeHeaders": True,
            "PartnerFilters": ["p"],
            "ReportDateRange": "Custom",
            "ReportStartDateInclusive": "2026-10-01",
            "ReportEndDateExclusive": "2026-10-02",
            "ReportFileFormat": "CSV",
            "ReportFrequency": "Once",
            "ReportScheduleName": "interloper_168005_2026-10-01",
            "ReportTemplateId": "168005",
            "ReportDateFormat": "US",
            "ReportNumericFormat": "US",
            "TimeZone": "Europe/Berlin",
        }
        assert json.loads(server.requests[1].content) == {
            "PageStartIndex": 0,
            "PageSize": 100,
            "PartnerIds": ["p"],
            "ReportScheduleIds": ["42"],
        }
        assert sleeps == []

    async def test_polls_with_backoff_until_the_execution_completes(self, sleeps: list[float]):
        server = _MyReports(None, "Pending", "Running", "Running", "Running", "Complete")

        assert await _api(server).report("p", "168005", DAY) != []
        assert sleeps == [60.0, 120.0, 240.0, 300.0, 300.0]
        assert server.sent().count(("POST", EXECUTIONS)) == 6

    @pytest.mark.parametrize("state", ["Failed", "Error"])
    async def test_a_failed_execution_raises_and_still_deletes_the_schedule(self, state: str, sleeps: list[float]):
        server = _MyReports(state)

        with pytest.raises(RuntimeError, match=f"Report schedule 42 failed with state {state}"):
            await _api(server).report("p", "168005", DAY)
        assert server.sent()[-1] == ("DELETE", f"{SCHEDULES}/42")

    async def test_an_execution_past_the_deadline_raises_and_still_deletes_the_schedule(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_TIMEOUT", 0)
        server = _MyReports("Pending")

        with pytest.raises(RuntimeError, match="Report schedule 42 did not complete within 0s"):
            await _api(server).report("p", "168005", DAY)
        assert server.sent()[-1] == ("DELETE", f"{SCHEDULES}/42")
        assert sleeps == []

    @pytest.mark.parametrize("refused", [EXECUTIONS, DOWNLOAD_URL])
    async def test_a_refusal_after_the_schedule_is_created_raises_and_still_deletes_it(
        self, refused: str, sleeps: list[float]
    ):
        server = _MyReports("Complete", refused=refused)

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(server).report("p", "168005", DAY)
        assert server.sent()[-1] == ("DELETE", f"{SCHEDULES}/42")

    async def test_the_first_of_several_executions_and_deliveries_is_read(
        self, caplog: pytest.LogCaptureFixture, sleeps: list[float]
    ):
        server = _MyReports("Complete", duplicated=True)

        with caplog.at_level(logging.WARNING):
            rows = await _api(server).report("p", "168005", DAY)

        assert rows == [{"Date": "01/10/2026", "Advertiser ID": "adv1", "Impressions": "1,5"}]
        assert ("GET", DOWNLOAD_URL) in server.sent()
        assert "Schedule 42 has multiple executions; using the first" in caplog.text
        assert "Schedule 42 has multiple deliveries; using the first" in caplog.text

    @pytest.mark.parametrize("delivered", [{"ReportDeliveries": []}, {"ReportDeliveries": None}, {}])
    async def test_a_complete_execution_without_a_delivery_raises_and_still_deletes_the_schedule(
        self, delivered: dict[str, Any], sleeps: list[float]
    ):
        server = _MyReports("Complete")

        def handler(request: httpx2.Request) -> httpx2.Response:
            response = server(request)
            if str(request.url) == EXECUTIONS:
                return httpx2.Response(200, json={"Result": [{"ReportExecutionState": "Complete", **delivered}]})
            return response

        with pytest.raises(RuntimeError, match="Report schedule 42 completed without a delivery"):
            await _api(handler).report("p", "168005", DAY)
        assert server.sent() == [("POST", SCHEDULES), ("POST", EXECUTIONS), ("DELETE", f"{SCHEDULES}/42")]

    async def test_an_empty_report_reads_as_no_rows(self, sleeps: list[float]):
        assert await _api(_MyReports("Complete", csv="Date,Impressions\n")).report("p", "168005", DAY) == []

    async def test_a_failed_delete_is_logged_and_the_rows_still_return(
        self, caplog: pytest.LogCaptureFixture, sleeps: list[float]
    ):
        server = _MyReports("Complete", delete_status=404)

        with caplog.at_level(logging.WARNING):
            rows = await _api(server).report("p", "168005", DAY)

        assert rows != []
        assert "Failed to delete report schedule 42" in caplog.text

    async def test_a_refused_schedule_raises_without_polling(self, sleeps: list[float]):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(400, json={"Message": "Invalid ReportTemplateId"})

        with pytest.raises(httpx2.HTTPStatusError, match="HTTP 400: Invalid ReportTemplateId"):
            await _api(handler).report("p", "unknown", DAY)
        assert len(requests) == 1
