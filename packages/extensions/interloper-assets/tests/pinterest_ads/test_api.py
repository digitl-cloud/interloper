"""Tests for the Pinterest Ads API engine.

Pinterest refuses a request with an HTTP error status, pages its lists by a
``bookmark`` cursor, and serves a report as a job: created, polled by its
token, then downloaded from a signed URL that rejects the API's token. These
tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.pinterest_ads import api as api_module
from interloper_assets.pinterest_ads.api import PinterestAdsApi, PinterestAdsApiError
from interloper_assets.pinterest_ads.constants import BASE_URL, DOWNLOAD_TIMEOUT, PAGE_SIZE

DAY = dt.date(2026, 9, 14)
REPORTS = "/v5/ad_accounts/549759962542/reports"
FILE_URL = "https://storage.example/r.json"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> PinterestAdsApi:
    return PinterestAdsApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


class TestGet:
    async def test_returns_the_one_response_as_sent(self):
        sent: list[tuple[str, str, bytes]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append((request.method, request.url.path, request.url.query))
            return httpx2.Response(200, json={"id": "1", "owner": {"id": "9"}})

        assert await _api(handler).get("/ad_accounts/1") == {"id": "1", "owner": {"id": "9"}}
        assert sent == [("GET", "/v5/ad_accounts/1", b"")]

    async def test_parameters_are_sent_as_the_query(self):
        sent: list[dict[str, str]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(dict(request.url.params))
            return httpx2.Response(200, json={"id": "1"})

        await _api(handler).get("/ad_accounts/1/campaigns/2", ad_account_id="1")

        assert sent == [{"ad_account_id": "1"}]

    async def test_a_refusal_raises_pinterests_own_code_and_message_without_the_url(self):
        api = _api(lambda _: httpx2.Response(401, json={"code": 2, "message": "Authentication failed."}))

        with pytest.raises(PinterestAdsApiError) as raised:
            await api.get("/ad_accounts/1", fields="id")

        assert str(raised.value) == "Pinterest refused the request (HTTP 401, code 2): Authentication failed."
        assert raised.value.code == 2

    async def test_a_refusal_is_still_its_http_error_status(self):
        api = _api(lambda _: httpx2.Response(403, json={"code": 29, "message": "Not permitted."}))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.get("/ad_accounts/1")

        assert raised.value.response.status_code == 403

    @pytest.mark.parametrize("body", [{"text": "<html>denied</html>"}, {"json": ["not", "an", "error"]}])
    async def test_an_http_error_without_a_pinterest_body_raises_its_status(self, body: dict[str, Any]):
        with pytest.raises(PinterestAdsApiError) as raised:
            await _api(lambda _: httpx2.Response(502, **body)).get("/ad_accounts/1")

        assert str(raised.value) == "Pinterest refused the request (HTTP 502): Bad Gateway"
        assert raised.value.code is None


class TestRows:
    async def test_follows_the_bookmark_to_the_last_page(self):
        pages = {
            None: {"items": [{"id": "1"}, {"id": "2"}], "bookmark": "b2"},
            "b2": {"items": [{"id": "3"}], "bookmark": None},
        }
        sent: list[tuple[str, str, dict[str, str]]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            # the cursor paginator rewrites one request in place, so record each page as it is sent
            sent.append((request.method, request.url.path, dict(request.url.params)))
            return httpx2.Response(200, json=pages[request.url.params.get("bookmark")])

        assert await _api(handler).rows("/ad_accounts/1/ads") == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        assert sent == [
            ("GET", "/v5/ad_accounts/1/ads", {"page_size": str(PAGE_SIZE)}),
            ("GET", "/v5/ad_accounts/1/ads", {"page_size": str(PAGE_SIZE), "bookmark": "b2"}),
        ]

    async def test_parameters_go_on_every_page_and_a_list_repeats(self):
        pages = {None: {"items": [{"id": "1"}], "bookmark": "b2"}, "b2": {"items": [], "bookmark": None}}
        sent: list[list[tuple[str, str]]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(request.url.params.multi_items())
            return httpx2.Response(200, json=pages[request.url.params.get("bookmark")])

        await _api(handler).rows("/ad_accounts/1/campaigns", entity_statuses=["ACTIVE", "ARCHIVED"])

        assert len(sent) == 2
        for items in sent:
            assert [value for key, value in items if key == "entity_statuses"] == ["ACTIVE", "ARCHIVED"]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("bookmark") == "b2":
                return httpx2.Response(403, json={"code": 29, "message": "You are not permitted to access that."})
            return httpx2.Response(200, json={"items": [{"id": "1"}], "bookmark": "b2"})

        with pytest.raises(PinterestAdsApiError, match=re.escape("(HTTP 403, code 29): You are not permitted")):
            await _api(handler).rows("/ad_accounts/1/ads")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"items": [], "bookmark": None})

        assert await _api(handler).rows("/ad_accounts/1/ads") == []
        assert len(requests) == 1

    async def test_a_response_without_a_bookmark_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list on every request; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(400, json={"message": "runaway walk"})
            return httpx2.Response(200, json={"items": [{"id": "1"}]})

        assert await _api(handler).rows("/ad_accounts") == [{"id": "1"}]
        assert len(requests) == 1


class _ReportJob:
    """A Pinterest report job: answers each poll from ``polls`` in turn and serves ``content`` as its file.

    A poll answer is the job's status, or an HTTP error status to refuse that poll with.
    """

    def __init__(self, polls: list[dict[str, Any] | int], content: bytes) -> None:
        self.polls = polls
        self.content = content
        self.refuse_creation = False
        self.download_status = 200
        self.sent: list[tuple[str, str, dict[str, str]]] = []
        self.bodies: list[dict[str, Any]] = []
        self.downloads: list[httpx2.Request] = []

    def api(self, request: httpx2.Request) -> httpx2.Response:
        self.sent.append((request.method, request.url.path, dict(request.url.params)))
        if request.method == "POST":
            self.bodies.append(json.loads(request.content))
            if self.refuse_creation:
                return httpx2.Response(400, json={"code": 1, "message": "Invalid columns"})
            return httpx2.Response(200, json={"token": "tok", "report_status": "IN_PROGRESS"})
        answer = self.polls.pop(0)
        if isinstance(answer, int):
            return httpx2.Response(answer, json={"code": 2, "message": "Authentication failed."})
        return httpx2.Response(200, json=answer)

    def storage(self, request: httpx2.Request) -> httpx2.Response:
        self.downloads.append(request)
        return httpx2.Response(self.download_status, content=self.content)


ReportJobFactory = Callable[..., tuple[PinterestAdsApi, _ReportJob]]


@pytest.fixture
def report_job(monkeypatch: pytest.MonkeyPatch) -> ReportJobFactory:
    """Build an engine over a fake report job, with no wait between polls.

    Args:
        monkeypatch: Removes the poll interval and routes the unauthenticated download to the fake.

    Returns:
        A factory taking the poll answers, the file's content and the API client's headers.
    """
    real_client = httpx2.AsyncClient
    monkeypatch.setattr(api_module, "_POLL_INTERVAL", 0)

    def install(
        *polls: dict[str, Any] | int, content: bytes = b"", headers: dict[str, str] | None = None
    ) -> tuple[PinterestAdsApi, _ReportJob]:
        job = _ReportJob(list(polls), content)
        monkeypatch.setattr(
            httpx2,
            "AsyncClient",
            lambda **kwargs: real_client(transport=httpx2.MockTransport(job.storage), **kwargs),
        )
        client = il.AsyncRESTClient(BASE_URL, headers=headers, transport=httpx2.MockTransport(job.api))
        return PinterestAdsApi(client), job

    return install


IN_PROGRESS = {"report_status": "IN_PROGRESS", "url": None}
FINISHED = {"report_status": "FINISHED", "url": FILE_URL}


class TestReport:
    async def test_runs_a_one_day_json_job_and_flattens_its_file(self, report_job: ReportJobFactory):
        content = json.dumps({"687001": [{"DATE": "2026-09-14", "AD_ID": "687001"}], "687002": [{"DATE": "x"}]})
        api, job = report_job(IN_PROGRESS, FINISHED, content=content.encode())

        rows = await api.report(
            "549759962542", DAY, granularity="DAY", level="PIN_PROMOTION", columns=["AD_ID", "SPEND_IN_DOLLAR"]
        )

        assert rows == [{"DATE": "2026-09-14", "AD_ID": "687001"}, {"DATE": "x"}]
        assert job.sent == [
            ("POST", REPORTS, {}),
            ("GET", REPORTS, {"token": "tok"}),
            ("GET", REPORTS, {"token": "tok"}),
        ]
        assert job.bodies == [
            {
                "start_date": "2026-09-14",
                "end_date": "2026-09-14",
                "granularity": "DAY",
                "level": "PIN_PROMOTION",
                "columns": ["AD_ID", "SPEND_IN_DOLLAR"],
                "report_format": "JSON",
            }
        ]
        (download,) = job.downloads
        assert (download.method, str(download.url)) == ("GET", FILE_URL)
        assert download.extensions["timeout"] == DOWNLOAD_TIMEOUT.as_dict()

    async def test_the_report_is_what_the_caller_asks_for(self, report_job: ReportJobFactory):
        api, job = report_job({"report_status": "FINISHED", "url": None})

        await api.report(
            "549759962542",
            DAY,
            granularity="TOTAL",
            level="PIN_PROMOTION_TARGETING",
            columns=["PIN_PROMOTION_ID"],
            targeting_types=["APPTYPE"],
        )

        (body,) = job.bodies
        assert (body["granularity"], body["level"], body["targeting_types"]) == (
            "TOTAL",
            "PIN_PROMOTION_TARGETING",
            ["APPTYPE"],
        )

    @pytest.mark.parametrize("parameter", ["start_date", "end_date", "report_format"])
    async def test_the_caller_cannot_change_the_day_or_the_format(self, report_job: ReportJobFactory, parameter: str):
        api, job = report_job()

        with pytest.raises(TypeError):
            await api.report("549759962542", DAY, level="CAMPAIGN", **{parameter: "CSV"})
        assert job.sent == []

    async def test_the_file_is_downloaded_without_the_api_token(self, report_job: ReportJobFactory):
        api, job = report_job(FINISHED, content=b"{}", headers={"Authorization": "Bearer access"})

        assert await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"]) == []
        (download,) = job.downloads
        assert "Authorization" not in download.headers

    async def test_a_finished_job_without_a_file_has_no_rows(self, report_job: ReportJobFactory):
        api, job = report_job({"report_status": "FINISHED", "url": None})

        assert await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"]) == []
        assert job.downloads == []

    async def test_an_empty_file_has_no_rows(self, report_job: ReportJobFactory):
        api, _ = report_job(FINISHED, content=b"")

        assert await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"]) == []

    @pytest.mark.parametrize("status", ["DOES_NOT_EXIST", "EXPIRED", "FAILED", "CANCELLED"])
    async def test_a_failed_job_raises_its_status(self, report_job: ReportJobFactory, status: str):
        api, job = report_job(IN_PROGRESS, {"report_status": status})

        with pytest.raises(RuntimeError, match=rf"^Pinterest report tok ended with status {status}$"):
            await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])
        assert job.downloads == []

    async def test_a_job_not_ready_in_time_raises(self, report_job: ReportJobFactory, monkeypatch: pytest.MonkeyPatch):
        api, job = report_job(IN_PROGRESS)
        monkeypatch.setattr(api_module, "_REPORT_TIMEOUT", 0)

        with pytest.raises(RuntimeError, match=r"^Pinterest report tok was not ready within 0s$"):
            await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])
        assert [method for method, _, _ in job.sent] == ["POST", "GET"]

    async def test_a_refused_job_raises(self, report_job: ReportJobFactory):
        api, job = report_job()
        job.refuse_creation = True

        with pytest.raises(PinterestAdsApiError, match=re.escape("(HTTP 400, code 1): Invalid columns")):
            await api.report("549759962542", DAY, level="CAMPAIGN", columns=["NOT_A_COLUMN"])
        assert job.sent == [("POST", REPORTS, {})]
        assert job.downloads == []

    async def test_an_http_error_while_polling_raises(self, report_job: ReportJobFactory):
        api, job = report_job(IN_PROGRESS, 401)

        with pytest.raises(PinterestAdsApiError, match=re.escape("(HTTP 401, code 2): Authentication failed.")):
            await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])
        assert job.downloads == []

    async def test_an_http_error_while_downloading_raises_as_storage_sent_it(self, report_job: ReportJobFactory):
        api, job = report_job(FINISHED, content=b"<Error>AccessDenied</Error>")
        job.download_status = 403

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.report("549759962542", DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])
        assert not isinstance(raised.value, PinterestAdsApiError)
