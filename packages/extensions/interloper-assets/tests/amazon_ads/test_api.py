"""Tests for the Amazon Ads API engine.

Amazon scopes every call but the profile listing to one advertising profile
by a header, refuses a request with an HTTP error status, pages its list
calls by a ``nextToken`` in the POST body (Sponsored Products and Brands) or
by ``startIndex`` (Sponsored Display), and serves reports as jobs: created,
polled, then downloaded as gzipped JSON. These tests pin each against a mock
transport, with the waits between attempts recorded rather than slept.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest
import tenacity as tc

from interloper_assets.amazon_ads import api, constants
from interloper_assets.amazon_ads.api import AmazonAdsApi

API_URL = "https://advertising-api-eu.amazon.com"
AUTH_URL = "https://api.amazon.co.uk"
REPORT_URL = "https://offline-report-storage.example/report.json.gz?signature=s"
REPORT_MEDIA_TYPE = "application/vnd.createasyncreportrequest.v3+json"
CAMPAIGN_MEDIA_TYPE = "application/vnd.spCampaign.v3+json"
DAY = dt.date(2026, 10, 1)


def _api(handler: Callable[[httpx2.Request], httpx2.Response], *, auth: httpx2.Auth | None = None) -> AmazonAdsApi:
    # the client repeats nothing itself, so each test sees the engine's own repeats
    client = il.RESTClient(API_URL, auth=auth, transport=httpx2.MockTransport(handler))
    return AmazonAdsApi(client)


def _headers(request: httpx2.Request) -> dict[str, str | None]:
    names = ("amazon-advertising-api-scope", "content-type", "accept")
    return {name: request.headers.get(name) for name in names}


def _ordered(content: bytes) -> str:
    # re-encoded with its keys in the order sent, so a comparison pins the order too
    return json.dumps(json.loads(content))


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record every wait between a report job's attempts instead of sleeping it.

    Args:
        monkeypatch: Replaces the engine's ``sleep`` for the test only.

    Returns:
        The seconds of each wait, in order.
    """
    waits: list[float] = []
    monkeypatch.setattr(api, "sleep", waits.append)
    return waits


class Downloads:
    """A stand-in for ``httpx2.get`` serving report files and recording each call."""

    def __init__(self) -> None:
        """Start with every download succeeding with no rows."""
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.responses: list[httpx2.Response | httpx2.TransportError] = []
        self.rows: list[dict[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> httpx2.Response:
        """Answer one download: the next queued response, else the gzipped JSON of ``rows``.

        A queued transport error is raised instead, as a dropped connection would be.

        Args:
            url: The report file's URL.
            **kwargs: The download's options.

        Returns:
            The response, carrying its request so a failure status raises.
        """
        self.calls.append((url, kwargs))
        request = httpx2.Request("GET", url)
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, httpx2.TransportError):
                raise response
            response.request = request
            return response
        return httpx2.Response(200, content=gzip.compress(json.dumps(self.rows).encode()), request=request)


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> Downloads:
    """Serve every report download from a `Downloads` stand-in.

    Args:
        monkeypatch: Replaces ``httpx2.get`` for the test only.

    Returns:
        The stand-in, to queue responses on and read calls from.
    """
    stand_in = Downloads()
    monkeypatch.setattr(httpx2, "get", stand_in)
    return stand_in


class TestGet:
    def test_returns_the_one_response_of_an_unscoped_call(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[{"profileId": 1}])

        assert _api(handler).get("/v2/profiles") == [{"profileId": 1}]
        [request] = requests
        assert (request.method, request.url.path, request.url.query) == ("GET", "/v2/profiles", b"")
        assert _headers(request)["amazon-advertising-api-scope"] is None

    def test_parameters_are_sent_as_the_query(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[])

        _api(handler).get("/v2/profiles", profileTypeFilter="seller")

        assert requests[0].url.query == b"profileTypeFilter=seller"

    def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError):
            _api(lambda _: httpx2.Response(401, json={"code": "UNAUTHORIZED"})).get("/v2/profiles")


class TestRows:
    def test_follows_the_next_token_in_the_body(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if "nextToken" not in json.loads(request.content):
                return httpx2.Response(200, json={"campaigns": [{"campaignId": "1"}], "nextToken": "t1"})
            return httpx2.Response(200, json={"campaigns": [{"campaignId": "2"}], "totalResults": 2})

        rows = _api(handler).rows(
            "123",
            "/sp/campaigns/list",
            media_type=CAMPAIGN_MEDIA_TYPE,
            key="campaigns",
            stateFilter={"include": ["ENABLED"]},
            includeExtendedDataFields=True,
        )

        assert rows == [{"campaignId": "1"}, {"campaignId": "2"}]
        first, second = requests
        body = {"stateFilter": {"include": ["ENABLED"]}, "includeExtendedDataFields": True, "maxResults": 100}
        assert _ordered(first.content) == json.dumps(body)
        assert _ordered(second.content) == json.dumps({**body, "nextToken": "t1"})
        for request in requests:
            assert (request.method, request.url.path) == ("POST", "/sp/campaigns/list")
            assert _headers(request) == {
                "amazon-advertising-api-scope": "123",
                "content-type": CAMPAIGN_MEDIA_TYPE,
                "accept": CAMPAIGN_MEDIA_TYPE,
            }

    def test_a_response_without_a_next_token_is_the_last_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"campaigns": [{"campaignId": "1"}]})

        rows = _api(handler).rows("123", "/sp/campaigns/list", media_type=CAMPAIGN_MEDIA_TYPE, key="campaigns")

        assert rows == [{"campaignId": "1"}]
        assert len(requests) == 1

    def test_a_page_repeating_the_token_it_was_asked_with_is_the_last(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if len(requests) > 3:
                raise AssertionError("read past a repeated token")
            return httpx2.Response(200, json={"campaigns": [{"campaignId": str(len(requests))}], "nextToken": "t1"})

        rows = _api(handler).rows("123", "/sp/campaigns/list", media_type=CAMPAIGN_MEDIA_TYPE, key="campaigns")

        assert rows == [{"campaignId": "1"}, {"campaignId": "2"}]
        assert [json.loads(request.content).get("nextToken") for request in requests] == [None, "t1"]

    def test_a_page_returning_an_earlier_token_is_the_last(self):
        tokens = {None: "t1", "t1": "t2", "t2": "t1"}
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if len(requests) > 4:
                raise AssertionError("read past a repeated token")
            token = json.loads(request.content).get("nextToken")
            return httpx2.Response(200, json={"campaigns": [{"campaignId": token}], "nextToken": tokens[token]})

        rows = _api(handler).rows("123", "/sp/campaigns/list", media_type=CAMPAIGN_MEDIA_TYPE, key="campaigns")

        assert rows == [{"campaignId": None}, {"campaignId": "t1"}, {"campaignId": "t2"}]
        assert len(requests) == 3

    def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"totalResults": 0})

        assert _api(handler).rows("123", "/sp/campaigns/list", media_type=CAMPAIGN_MEDIA_TYPE, key="campaigns") == []
        assert len(requests) == 1

    def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "nextToken" in json.loads(request.content):
                return httpx2.Response(403, json={"code": "UNAUTHORIZED", "details": "Not authorized"})
            return httpx2.Response(200, json={"campaigns": [{"campaignId": "1"}], "nextToken": "t1"})

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            _api(handler).rows("123", "/sp/campaigns/list", media_type=CAMPAIGN_MEDIA_TYPE, key="campaigns")


class TestOffsetRows:
    def test_pages_by_start_index_until_an_empty_page(self):
        # the paginator advances one request in place, so each is recorded as it is sent
        calls: list[tuple[str, str, bytes, str | None]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(
                (
                    request.method,
                    request.url.path,
                    request.url.query,
                    request.headers.get("amazon-advertising-api-scope"),
                )
            )
            start = int(request.url.params["startIndex"])
            return httpx2.Response(200, json=[{"campaignId": start + i} for i in range(100)] if start < 200 else [])

        rows = _api(handler).offset_rows("123", "/sd/campaigns/extended", stateFilter="enabled,paused")

        assert [row["campaignId"] for row in rows] == list(range(200))
        query = "stateFilter=enabled%2Cpaused&count=100&startIndex={}"
        assert calls == [
            ("GET", "/sd/campaigns/extended", query.format(start).encode(), "123") for start in (0, 100, 200)
        ]

    def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[])

        assert _api(handler).offset_rows("123", "/sd/campaigns/extended") == []
        assert len(requests) == 1

    def test_an_endpoint_ignoring_the_start_index_raises_instead_of_looping(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if len(requests) > 3:
                raise AssertionError("read past a repeated page")
            return httpx2.Response(200, json=[{"campaignId": i} for i in range(100)])

        with pytest.raises(RuntimeError, match="startIndex=100 repeats the page at startIndex=0"):
            _api(handler).offset_rows("123", "/sd/campaigns/extended")
        assert len(requests) == 2

    def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["startIndex"] != "0":
                return httpx2.Response(403, json={"code": "UNAUTHORIZED"})
            return httpx2.Response(200, json=[{"campaignId": 1}])

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            _api(handler).offset_rows("123", "/sd/campaigns/extended")


class ReportServer:
    """The reporting endpoints: create a job, then answer each poll with the next status."""

    def __init__(self, statuses: list[str], *, create: list[httpx2.Response] | None = None) -> None:
        """Queue what the endpoints answer.

        Args:
            statuses: The status of each poll in turn, the last repeated; a
                ``COMPLETED`` poll carries the report file's URL.
            create: Responses the create call answers before it accepts the job.
        """
        self.statuses = statuses
        self.create = create or []
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one call to the reporting endpoints, or to the token endpoint.

        Args:
            request: The request sent.

        Returns:
            The endpoint's answer.
        """
        if request.url.host == "api.amazon.co.uk":
            return httpx2.Response(200, json={"access_token": f"token-{len(self.requests)}"})
        self.requests.append(request)
        if request.method == "POST":
            if self.create:
                return self.create.pop(0)
            return httpx2.Response(200, json={"reportId": "r-1", "status": "PENDING"})
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        body = {"reportId": "r-1", "status": status}
        if status == "COMPLETED":
            body["url"] = REPORT_URL
        if status == "FAILED":
            body["failureReason"] = "Report generation failed"
        return httpx2.Response(200, json=body)


def _auth() -> il.OAuth2RefreshTokenAuth:
    return il.OAuth2RefreshTokenAuth(
        base_url=AUTH_URL,
        token_endpoint="/auth/o2/token",
        client_id="cid",
        client_secret="secret",
        refresh_token="refresh",
    )


def _report(engine: AmazonAdsApi) -> list[dict[str, Any]]:
    return engine.report(
        "123",
        DAY,
        reportTypeId="spCampaigns",
        adProduct="SPONSORED_PRODUCTS",
        groupBy=["campaign"],
        columns=["campaignId", "cost"],
        timeUnit="DAILY",
    )


class TestReport:
    def test_creates_a_job_for_the_day_waits_for_it_and_downloads_its_rows(
        self, downloads: Downloads, sleeps: list[float]
    ):
        downloads.rows = [{"date": "2026-10-01", "campaignId": 1, "cost": 1.5}]
        server = ReportServer(["COMPLETED"])

        assert _report(_api(server)) == downloads.rows

        create, status = server.requests
        assert (create.method, create.url.path, create.url.query) == ("POST", "/reporting/reports", b"")
        assert _ordered(create.content) == json.dumps(
            {
                "name": "spCampaigns | 2026-10-01 - 2026-10-01",
                "startDate": "2026-10-01",
                "endDate": "2026-10-01",
                "configuration": {
                    "reportTypeId": "spCampaigns",
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy": ["campaign"],
                    "columns": ["campaignId", "cost"],
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            }
        )
        assert (status.method, status.url.path, status.url.query) == ("GET", "/reporting/reports/r-1", b"")
        report_headers = {"amazon-advertising-api-scope": "123", "content-type": REPORT_MEDIA_TYPE, "accept": "*/*"}
        assert _headers(create) == _headers(status) == report_headers
        assert downloads.calls == [(REPORT_URL, {"timeout": 60.0})]
        assert sleeps == []

    def test_polls_with_a_doubling_wait_up_to_a_minute_until_the_job_completes(
        self, downloads: Downloads, sleeps: list[float]
    ):
        server = ReportServer(["PENDING", "PROCESSING", "PENDING", "PENDING", "PENDING", "COMPLETED"])

        assert _report(_api(server)) == []

        assert [request.method for request in server.requests] == ["POST"] + ["GET"] * 6
        assert sleeps == [10, 20, 40, 60, 60]
        assert len(downloads.calls) == 1

    def test_a_refused_creation_is_asked_again_five_minutes_later_each_time_up_to_fifteen(
        self, downloads: Downloads, sleeps: list[float]
    ):
        duplicate = [httpx2.Response(425, json={"code": "425", "detail": "duplicate"}) for _ in range(5)]
        server = ReportServer(["COMPLETED"], create=duplicate)

        assert _report(_api(server)) == []

        assert [request.method for request in server.requests] == ["POST"] * 6 + ["GET"]
        assert sleeps == [60, 360, 660, 900, 900]

    @pytest.mark.parametrize("status", [425, 500, 503])
    def test_a_creation_failing_transiently_is_asked_again(
        self, status: int, downloads: Downloads, sleeps: list[float]
    ):
        server = ReportServer(["COMPLETED"], create=[httpx2.Response(status, json={"code": str(status)})])

        assert _report(_api(server)) == []

        assert [request.method for request in server.requests] == ["POST", "POST", "GET"]
        assert sleeps == [60]

    def test_a_creation_whose_connection_drops_is_asked_again(self, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["COMPLETED"])
        drops = iter([httpx2.ConnectError("connection refused")])

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                drop = next(drops, None)
                if drop is not None:
                    raise drop
            return server(request)

        assert _report(_api(handler)) == []
        assert sleeps == [60]

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    def test_a_creation_refused_for_good_raises_at_once(self, status: int, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["COMPLETED"], create=[httpx2.Response(status, json={"code": str(status)})])

        with pytest.raises(httpx2.HTTPStatusError, match=str(status)):
            _report(_api(server))
        assert [request.method for request in server.requests] == ["POST"]
        assert sleeps == []

    def test_creation_past_its_deadline_raises_the_last_refusal(
        self, monkeypatch: pytest.MonkeyPatch, downloads: Downloads, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_CREATE_DEADLINE", dt.timedelta(0))
        server = ReportServer(["COMPLETED"], create=[httpx2.Response(425, json={"code": "425"})])

        with pytest.raises(httpx2.HTTPStatusError, match="425"):
            _report(_api(server))
        assert sleeps == []

    def test_a_failed_job_raises_amazons_reason_at_once(self, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["PENDING", "FAILED"])

        with pytest.raises(RuntimeError, match="Report r-1 for profile 123 failed: Report generation failed"):
            _report(_api(server))
        assert [request.method for request in server.requests] == ["POST", "GET", "GET"]
        assert sleeps == [10]
        assert downloads.calls == []

    def test_a_job_still_pending_past_its_deadline_raises_a_retry_error(
        self, monkeypatch: pytest.MonkeyPatch, downloads: Downloads, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_WAIT_DEADLINE", dt.timedelta(0))

        with pytest.raises(tc.RetryError):
            _report(_api(ReportServer(["PENDING"])))
        assert downloads.calls == []

    def test_a_poll_failing_past_its_deadline_raises_the_failure(
        self, monkeypatch: pytest.MonkeyPatch, downloads: Downloads, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_WAIT_DEADLINE", dt.timedelta(0))

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"reportId": "r-1"})
            return httpx2.Response(503, json={"code": "503"})

        with pytest.raises(httpx2.HTTPStatusError, match="503"):
            _report(_api(handler))

    def test_a_download_failing_past_its_deadline_raises_the_failure(
        self, monkeypatch: pytest.MonkeyPatch, downloads: Downloads, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_DOWNLOAD_DEADLINE", dt.timedelta(0))
        downloads.responses = [httpx2.Response(503)]

        with pytest.raises(httpx2.HTTPStatusError, match="503"):
            _report(_api(ReportServer(["COMPLETED"])))
        assert len(downloads.calls) == 1

    @pytest.mark.parametrize(
        "failure",
        [httpx2.Response(429), httpx2.Response(503), httpx2.ConnectError("connection refused")],
        ids=str,
    )
    def test_a_poll_failing_transiently_is_polled_again(
        self, failure: httpx2.Response | httpx2.TransportError, downloads: Downloads, sleeps: list[float]
    ):
        server = ReportServer(["COMPLETED"])
        failures = iter([failure])

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "GET":
                answer = next(failures, None)
                if isinstance(answer, httpx2.TransportError):
                    raise answer
                if answer is not None:
                    return answer
            return server(request)

        assert _report(_api(handler)) == []
        assert sleeps == [10]

    @pytest.mark.parametrize("status", [400, 403, 404])
    def test_a_poll_refused_for_good_raises_at_once(self, status: int, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["COMPLETED"])

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "GET":
                return httpx2.Response(status, json={"code": str(status)})
            return server(request)

        with pytest.raises(httpx2.HTTPStatusError, match=str(status)):
            _report(_api(handler))
        assert [request.method for request in server.requests] == ["POST"]
        assert sleeps == []
        assert downloads.calls == []

    @pytest.mark.parametrize(
        "failure", [httpx2.Response(503), httpx2.Response(500), httpx2.ReadTimeout("timed out")], ids=str
    )
    def test_a_download_failing_transiently_is_downloaded_again(
        self, failure: httpx2.Response | httpx2.TransportError, downloads: Downloads, sleeps: list[float]
    ):
        downloads.responses = [failure]
        downloads.rows = [{"campaignId": 1}]

        assert _report(_api(ReportServer(["COMPLETED"]))) == [{"campaignId": 1}]

        assert len(downloads.calls) == 2
        assert sleeps == [10]

    @pytest.mark.parametrize("status", [400, 403, 404])
    def test_a_download_refused_for_good_raises_at_once(self, status: int, downloads: Downloads, sleeps: list[float]):
        downloads.responses = [httpx2.Response(status)]

        with pytest.raises(httpx2.HTTPStatusError, match=str(status)):
            _report(_api(ReportServer(["COMPLETED"])))
        assert len(downloads.calls) == 1
        assert sleeps == []

    def test_a_failed_download_does_not_name_the_signed_url(self, downloads: Downloads, sleeps: list[float]):
        downloads.responses = [httpx2.Response(403)]

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            _report(_api(ReportServer(["COMPLETED"])))
        assert "signature" not in str(raised.value)

    def test_the_signed_url_is_never_logged(
        self, caplog: pytest.LogCaptureFixture, downloads: Downloads, sleeps: list[float]
    ):
        downloads.responses = [httpx2.Response(503)]

        with caplog.at_level(logging.DEBUG):
            _report(_api(ReportServer(["PENDING", "COMPLETED"])))

        assert caplog.messages
        assert not [message for message in caplog.messages if "signature" in message]

    def test_a_poll_that_is_not_an_http_error_fails_at_once(self, downloads: Downloads, sleeps: list[float]):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "POST":
                return httpx2.Response(200, json={"reportId": "r-1"})
            return httpx2.Response(200, content=b"not json")

        with pytest.raises(json.JSONDecodeError):
            _report(_api(handler))
        assert sleeps == []

    def test_the_job_authenticates_once(self, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["PENDING", "PENDING", "COMPLETED"])

        _report(_api(server, auth=_auth()))

        assert [request.headers["Authorization"] for request in server.requests] == ["Bearer token-0"] * 4

    def test_a_token_expiring_during_the_job_is_exchanged_again(self, downloads: Downloads, sleeps: list[float]):
        server = ReportServer(["PENDING", "COMPLETED"])
        expired = {"Bearer token-0"}

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.method == "GET" and request.headers.get("Authorization") in expired:
                return httpx2.Response(401, json={"code": "UNAUTHORIZED"})
            return server(request)

        assert _report(_api(handler, auth=_auth())) == []

        assert [request.headers["Authorization"] for request in server.requests] == [
            "Bearer token-0",
            "Bearer token-1",
            "Bearer token-1",
        ]
        assert sleeps == [10]


def _retry_state() -> tc.RetryCallState:
    return tc.RetryCallState(retry_object=tc.Retrying(), fn=None, args=(), kwargs={})


class TestRetryLog:
    def test_a_throttled_attempt_logs_the_announced_wait(self, caplog: pytest.LogCaptureFixture):
        request = httpx2.Request("POST", f"{API_URL}/reporting/reports")
        response = httpx2.Response(429, headers={"Retry-After": "7"}, request=request)
        error = httpx2.HTTPStatusError("Too Many Requests", request=request, response=response)
        state = _retry_state()
        state.set_exception((type(error), error, None))
        state.next_action = tc.RetryAction(60)

        with caplog.at_level(logging.DEBUG, logger=api.__name__):
            AmazonAdsApi._log_retry(state)

        assert "Retry-After response header: 7" in caplog.messages

    def test_a_state_without_an_outcome_is_refused(self):
        with pytest.raises(RuntimeError, match="before outcome was set"):
            AmazonAdsApi._log_retry(_retry_state())

    def test_a_state_without_a_next_action_is_refused(self):
        state = _retry_state()
        state.set_result({"status": "PENDING"})

        with pytest.raises(RuntimeError, match="before next_action was set"):
            AmazonAdsApi._log_retry(state)
