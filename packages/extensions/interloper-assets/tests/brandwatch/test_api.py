"""Tests for the Brandwatch (Falcon.io) Measure API engine.

Measure answers insights as a job: a POST names the days, the metrics and
whatever else the insights are for, and is answered with an
``insightsRequestId``; the request is polled until ``READY``, then read page by
page, each page naming the ``nextPage``. A request names at most
``MAX_METRICS_PER_REQUEST`` metrics. These tests pin each against a mock
transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.brandwatch import api as api_module
from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.api import BrandwatchApi

DAY = dt.date(2026, 10, 1)


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> BrandwatchApi:
    return BrandwatchApi(il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler)))


def _sent(request: httpx2.Request) -> httpx2.Request:
    """Copy a request as it is sent.

    The paginating client sends every page with one request object, rewriting
    its URL, so a test keeps what each send carried rather than the object.

    Args:
        request: The request the mock transport received.

    Returns:
        An independent copy of its method, URL, headers and body.
    """
    return httpx2.Request(request.method, request.url, headers=request.headers, content=request.content)


def _point(value: Any, date: str = "2026-10-01") -> dict[str, Any]:
    return {"value": value, "date": date, "channelId": "chan-1"}


def _page(insights: Any, next_page: int | None = None) -> httpx2.Response:
    paging = {"nextPage": next_page} if next_page else {}
    return httpx2.Response(200, json={"data": {"insights": insights, "paging": paging}})


def _ready_job(pages: dict[str, httpx2.Response]) -> Callable[[httpx2.Request], httpx2.Response]:
    """Answer one insights request, ready at once, with the given pages.

    Args:
        pages: Each page's response, by the ``page`` it is asked for.

    Returns:
        The mock handler.
    """

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "POST":
            return httpx2.Response(200, json={"insightsRequestId": "request-1"})
        page = request.url.params.get("page")
        return httpx2.Response(200, json={"status": "READY"}) if page is None else pages[page]

    return handler


class TestGet:
    async def test_returns_the_body_of_one_response(self):
        requests: list[httpx2.Request] = []
        metrics = {"data": [{"id": "channel/fans/day", "level": "CHANNEL", "networks": ["FACEBOOK"]}]}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=metrics)

        assert await _api(handler).get("/metrics/channel", networks="facebook") == metrics
        (request,) = requests
        assert (request.method, str(request.url)) == (
            "GET",
            "https://api.falcon.io/measure/v2/metrics/channel?networks=facebook",
        )

    async def test_a_refusal_raises_its_http_error(self):
        def handler(_: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(401, json={"fault": {"faultstring": "Invalid ApiKey"}})

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await _api(handler).get("/metrics/channel")


class TestReport:
    async def test_requests_the_day_then_polls_and_reads_every_page(self):
        requests: list[httpx2.Request] = []
        pages = {
            "1": _page({"channel/fans/day": [_point(10)]}, next_page=2),
            "2": _page({"channel/likes/day": [_point(3)]}),
        }
        answer = _ready_job(pages)

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(_sent(request))
            return answer(request)

        insights = await _api(handler).report(
            "/insights/channel", DAY, ["channel/fans/day", "channel/likes/day"], channelIds=["chan-1"]
        )

        assert insights == {"channel/fans/day": [_point(10)], "channel/likes/day": [_point(3)]}
        assert [(request.method, request.url.path, request.url.query) for request in requests] == [
            ("POST", "/measure/v2/insights/channel", b""),
            ("GET", "/measure/v2/insights/request-1", b""),
            ("GET", "/measure/v2/insights/request-1", b"page=1"),
            ("GET", "/measure/v2/insights/request-1", b"page=2"),
        ]
        body = json.loads(requests[0].content)
        assert body == {
            "since": "2026-10-01",
            "until": "2026-10-01",
            "channelIds": ["chan-1"],
            "metricIds": ["channel/fans/day", "channel/likes/day"],
        }
        assert list(body) == ["since", "until", "channelIds", "metricIds"]

    async def test_a_metric_on_several_pages_keeps_every_point_in_order(self):
        pages = {
            "1": _page({"channel/fans/day": [_point(1, "2026-10-01")]}, next_page=2),
            "2": _page({"channel/fans/day": [_point(2, "2026-10-02")]}),
        }

        insights = await _api(_ready_job(pages)).report("/insights/channel", DAY, ["channel/fans/day"])

        assert insights == {"channel/fans/day": [_point(1, "2026-10-01"), _point(2, "2026-10-02")]}

    async def test_metrics_beyond_the_cap_are_requested_in_batches_and_merged(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(constants, "MAX_METRICS_PER_REQUEST", 2)
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                bodies.append(json.loads(request.content))
                return httpx2.Response(200, json={"insightsRequestId": f"request-{len(bodies)}"})
            if "page" not in request.url.params:
                return httpx2.Response(200, json={"status": "READY"})
            batch = bodies[int(request.url.path.rsplit("-", 1)[-1]) - 1]["metricIds"]
            return _page({metric_id: [_point(metric_id)] for metric_id in batch})

        metric_ids = ["channel/fans/day", "channel/likes/day", "channel/reach/day"]
        insights = await _api(handler).report("/insights/channel", DAY, metric_ids, channelIds=["chan-1"])

        assert [body["metricIds"] for body in bodies] == [metric_ids[:2], metric_ids[2:]]
        assert {body["channelIds"][0] for body in bodies} == {"chan-1"}
        assert insights == {metric_id: [_point(metric_id)] for metric_id in metric_ids}

    async def test_a_response_without_paging_info_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(_sent(request))
            if request.method == "POST":
                return httpx2.Response(200, json={"insightsRequestId": "request-1"})
            if "page" not in request.url.params:
                return httpx2.Response(200, json={"status": "READY"})
            return httpx2.Response(200, json={"data": {"insights": {"channel/fans/day": [_point(1)]}}})

        insights = await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"])

        assert insights == {"channel/fans/day": [_point(1)]}
        assert [request.url.params.get("page") for request in requests] == [None, None, "1"]

    async def test_no_insights_is_an_empty_result(self):
        pages = {"1": httpx2.Response(200, json={"data": {"insights": None, "paging": None}})}

        assert await _api(_ready_job(pages)).report("/insights/channel", DAY, ["channel/fans/day"]) == {}

    async def test_a_metric_without_points_is_kept_empty(self):
        pages = {"1": _page({"channel/fans/day": None})}

        assert await _api(_ready_job(pages)).report("/insights/channel", DAY, ["channel/fans/day"]) == {
            "channel/fans/day": []
        }

    async def test_a_request_in_progress_is_polled_until_ready(self, monkeypatch: pytest.MonkeyPatch):
        sleeps: list[float] = []

        async def record_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr(api_module, "sleep", record_sleep)
        statuses = iter([{"status": "IN_PROGRESS"}, {}, {"status": "READY"}])
        polls: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"insightsRequestId": "request-1"})
            if "page" in request.url.params:
                return _page({"channel/fans/day": [_point(1)]})
            polls.append(request)
            return httpx2.Response(200, json=next(statuses))

        assert await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"]) == {
            "channel/fans/day": [_point(1)]
        }
        assert len(polls) == 3
        assert sleeps == [constants.POLL_INTERVAL] * 2

    async def test_a_failed_request_raises_its_status(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"insightsRequestId": "request-1"})
            return httpx2.Response(200, json={"status": "FAILED"})

        with pytest.raises(RuntimeError, match=r"^Insights request request-1 failed with status FAILED$"):
            await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"])

    async def test_a_request_not_ready_in_time_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(constants, "POLL_TIMEOUT", 0)
        polls: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"insightsRequestId": "request-1"})
            polls.append(request)
            return httpx2.Response(200, json={"status": "IN_PROGRESS"})

        with pytest.raises(RuntimeError, match=r"^Insights request request-1 not ready within 0s$"):
            await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"])
        assert len(polls) == 1

    async def test_a_refused_request_raises_its_http_error(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(_sent(request))
            return httpx2.Response(401, json={"message": "Invalid API key"})

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"])
        assert len(requests) == 1

    async def test_a_refused_poll_raises_its_http_error(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"insightsRequestId": "request-1"})
            return httpx2.Response(404, json={"message": "Unknown request"})

        with pytest.raises(httpx2.HTTPStatusError, match="404"):
            await _api(handler).report("/insights/channel", DAY, ["channel/fans/day"])

    async def test_a_refused_page_raises_its_http_error(self):
        pages = {"1": _page({"channel/fans/day": [_point(1)]}, next_page=2), "2": httpx2.Response(403)}

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(_ready_job(pages)).report("/insights/channel", DAY, ["channel/fans/day"])
