"""Tests for the Criteo API engine.

Criteo versions every path, wraps each entity as a JSON:API resource
(``{id, type, attributes}``) under ``data``, pages its lists by ``offset``
and ``limit`` with no total, and answers its statistics report with
``Rows``. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.criteo.api import CriteoApi, CriteoApiError
from interloper_assets.criteo.constants import BASE_URL

CAMPAIGN = {"id": "7", "type": "Campaign", "attributes": {"id": "7", "name": "Prospecting"}}


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> CriteoApi:
    return CriteoApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _resources(*resources: dict[str, Any]) -> httpx2.Response:
    return httpx2.Response(200, json={"data": list(resources), "errors": [], "warnings": []})


def _refused(_: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(403, json={"errors": [{"title": "Forbidden"}]})


class TestGet:
    async def test_returns_the_resources_of_one_response(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _resources({"id": "1", "type": "Advertiser", "attributes": {"advertiserName": "Example"}})

        assert await _api(handler).get("/advertisers/me") == [
            {"advertiserName": "Example", "id": "1", "type": "Advertiser"}
        ]
        (request,) = requests
        assert (request.method, request.url.path) == ("GET", "/2026-07/advertisers/me")
        assert not request.url.params

    async def test_query_parameters_are_sent(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _resources()

        await _api(handler).get("/advertisers/me", fields="name")

        assert dict(requests[0].url.params) == {"fields": "name"}

    async def test_a_resources_own_fields_win_over_its_attributes(self):
        resource = {"id": "7", "type": "Campaign", "attributes": {"id": "other", "name": "Prospecting"}, "meta": 1}

        assert await _api(lambda _: _resources(resource)).get("/campaigns") == [
            {"id": "7", "type": "Campaign", "name": "Prospecting", "meta": 1}
        ]

    async def test_a_resource_without_attributes_keeps_its_own_fields(self):
        resource = {"id": "7", "type": "Campaign", "attributes": None}

        assert await _api(lambda _: _resources(resource)).get("/campaigns") == [{"id": "7", "type": "Campaign"}]

    async def test_no_data_is_an_empty_result(self):
        api = _api(lambda _: httpx2.Response(200, json={"data": None, "errors": [], "warnings": []}))

        assert await api.get("/advertisers/me") == []

    async def test_a_response_without_data_raises(self):
        api = _api(lambda _: httpx2.Response(200, json={"errors": [], "warnings": []}))

        with pytest.raises(KeyError, match="data"):
            await api.get("/advertisers/me")

    async def test_a_success_carrying_errors_raises_each_errors_code_and_detail(self):
        errors = [
            {"code": "resource-not-found", "title": "Not found", "detail": "Advertiser 1 is not accessible"},
            {"title": "Unexpected"},
        ]
        api = _api(lambda _: httpx2.Response(200, json={"data": [CAMPAIGN], "errors": errors, "warnings": []}))

        with pytest.raises(CriteoApiError) as raised:
            await api.get("/advertisers/me")

        assert str(raised.value) == (
            "Criteo answered with errors: resource-not-found: Advertiser 1 is not accessible; Unexpected"
        )
        assert raised.value.errors == errors

    async def test_warnings_are_logged_and_the_rows_kept(self, caplog: pytest.LogCaptureFixture):
        warnings = [{"code": "deprecated-field", "detail": "Field budget is deprecated"}]
        api = _api(lambda _: httpx2.Response(200, json={"data": [CAMPAIGN], "errors": [], "warnings": warnings}))

        with caplog.at_level("WARNING"):
            rows = await api.get("/advertisers/me")

        assert rows == [{"id": "7", "type": "Campaign", "name": "Prospecting"}]
        assert "deprecated-field: Field budget is deprecated" in caplog.text

    async def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(_refused).get("/advertisers/me")


class TestPost:
    async def test_posts_the_body_and_returns_the_resources(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _resources(CAMPAIGN)

        rows = await _api(handler).post("/marketing-solutions/campaigns/search", filters={"advertiserIds": ["123"]})

        assert rows == [{"id": "7", "type": "Campaign", "name": "Prospecting"}]
        (request,) = requests
        assert (request.method, request.url.path) == ("POST", "/2026-07/marketing-solutions/campaigns/search")
        assert not request.url.params
        assert json.loads(request.content) == {"filters": {"advertiserIds": ["123"]}}

    async def test_a_response_without_data_raises(self):
        api = _api(lambda _: httpx2.Response(200, json={"errors": [], "warnings": []}))

        with pytest.raises(KeyError, match="data"):
            await api.post("/marketing-solutions/campaigns/search", filters={})

    async def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(_refused).post("/marketing-solutions/campaigns/search", filters={})


class TestRows:
    async def test_every_page_is_read_until_an_empty_one(self):
        queries: list[dict[str, str]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            queries.append(dict(request.url.params))
            offset = request.url.params["offset"]
            if offset == "100":
                return _resources()
            return _resources({"id": offset, "type": "Ad", "attributes": {"name": f"ad {offset}"}})

        rows = await _api(handler).rows("/marketing-solutions/advertisers/123/ads", status="active")

        assert rows == [
            {"name": "ad 0", "id": "0", "type": "Ad"},
            {"name": "ad 50", "id": "50", "type": "Ad"},
        ]
        assert queries == [
            {"status": "active", "limit": "50", "offset": "0"},
            {"status": "active", "limit": "50", "offset": "50"},
            {"status": "active", "limit": "50", "offset": "100"},
        ]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["offset"] == "50":
                return _refused(request)
            return _resources(CAMPAIGN)

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(handler).rows("/marketing-solutions/advertisers/123/ads")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _resources()

        assert await _api(handler).rows("/marketing-solutions/advertisers/123/ads") == []
        assert len(requests) == 1

    async def test_a_page_carrying_errors_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            errors = [{"code": "partial-failure", "detail": "Ad 9 could not be read"}]
            return httpx2.Response(200, json={"data": [CAMPAIGN], "errors": errors, "warnings": []})

        with pytest.raises(CriteoApiError, match="partial-failure: Ad 9 could not be read"):
            await _api(handler).rows("/marketing-solutions/advertisers/1/ads")

    async def test_a_page_without_data_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["offset"] == "50":
                return httpx2.Response(200, json={"errors": [], "warnings": []})
            return _resources(CAMPAIGN)

        with pytest.raises(KeyError, match="data"):
            await _api(handler).rows("/marketing-solutions/advertisers/123/ads")

    async def test_a_page_of_null_data_ends_the_walk(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.params["offset"] == "50":
                return httpx2.Response(200, json={"data": None, "errors": [], "warnings": []})
            return _resources(CAMPAIGN)

        assert await _api(handler).rows("/marketing-solutions/advertisers/123/ads") == [
            {"id": "7", "type": "Campaign", "name": "Prospecting"}
        ]
        assert len(requests) == 2

    async def test_an_endpoint_that_ignores_the_offset_yields_its_list_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list at every offset; end a runaway walk
            if len(requests) > 3:
                return _resources()
            return _resources(CAMPAIGN)

        assert await _api(handler).rows("/advertisers/me") == [{"id": "7", "type": "Campaign", "name": "Prospecting"}]
        assert len(requests) == 2


class TestReport:
    async def test_posts_the_statistics_query_for_one_day(self):
        requests: list[httpx2.Request] = []
        row = {"Day": "2026-10-01", "CampaignId": "7", "Clicks": 3}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"Rows": [row], "Total": {}})

        rows = await _api(handler).report(
            dt.date(2026, 10, 1),
            advertiserIds="123",
            dimensions=["Day", "CampaignId"],
            metrics=["Clicks"],
            currency="EUR",
            timezone="Europe/Zurich",
        )

        assert rows == [row]
        (request,) = requests
        assert (request.method, request.url.path) == ("POST", "/2026-07/statistics/report")
        assert json.loads(request.content) == {
            "startDate": "2026-10-01",
            "endDate": "2026-10-01",
            "advertiserIds": "123",
            "dimensions": ["Day", "CampaignId"],
            "metrics": ["Clicks"],
            "currency": "EUR",
            "timezone": "Europe/Zurich",
            "format": "json",
        }

    async def test_a_day_without_statistics_is_empty(self):
        api = _api(lambda _: httpx2.Response(200, json={"Rows": [], "Total": {}}))

        assert await api.report(dt.date(2026, 10, 1), advertiserIds="123", dimensions=["Day"], metrics=["Clicks"]) == []

    async def test_a_query_naming_its_own_day_is_refused(self):
        api = _api(lambda _: httpx2.Response(200, json={"Rows": [], "Total": {}}))

        with pytest.raises(TypeError, match="startDate"):
            await api.report(dt.date(2026, 10, 1), advertiserIds="123", startDate="2026-01-01")

    async def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(_refused).report(dt.date(2026, 10, 1), advertiserIds="123")
