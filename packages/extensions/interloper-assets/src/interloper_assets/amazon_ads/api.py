"""The Amazon Ads API engine every Amazon Ads request goes through."""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
from time import sleep
from typing import Any

import httpx2
import interloper as il
import tenacity as tc

from interloper_assets.amazon_ads import constants

logger = logging.getLogger(__name__)

_SCOPE = "Amazon-Advertising-API-Scope"
_REPORT_MEDIA_TYPE = "application/vnd.createasyncreportrequest.v3+json"


class AmazonAdsApi:
    """The Amazon Ads API: its profile scope, its two list pagings and its report jobs.

    Every call but the profile listing reads one advertising profile, named by
    the ``Amazon-Advertising-API-Scope`` header, so each scoped method takes
    the profile first. Amazon refuses a request with an HTTP error status,
    which every method raises as is.

    A report is a job: created, polled until it completes, then downloaded as
    gzipped JSON from a signed URL outside the API. Each phase repeats a
    failure until its deadline (``REPORT_*_DEADLINE``), and every attempt
    first drops the cached access token, so it authenticates afresh.
    """

    def __init__(self, client: il.RESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every API request goes through, carrying the
                connection's OAuth token and client id.
        """
        self.client = client

    def get(self, path: str, **params: Any) -> Any:
        """Read the one response of a call no profile scopes, such as ``/v2/profiles``.

        Args:
            path: The endpoint, relative to the region's API URL.
            **params: The query parameters.

        Returns:
            The response's JSON, as Amazon sent it.
        """
        response = self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    def rows(self, profile_id: str, path: str, *, media_type: str, key: str, **body: Any) -> list[dict[str, Any]]:
        """Read every row of a list call, following the ``nextToken`` each page returns.

        Sponsored Products and Sponsored Brands list their entities by a POST
        whose body carries the filters and, from the second page on, the token
        the previous page returned. A page without a token is the last.

        Args:
            profile_id: The profile whose entities are listed.
            path: The list call, such as ``/sp/campaigns/list``.
            media_type: The resource's vendor media type, sent as both content type and accept.
            key: The response key holding a page's rows, such as ``campaigns`` or ``adGroups``.
            **body: The call's body in Amazon's names, such as its ``stateFilter``;
                the page size and the token are added.

        Returns:
            Every row, in the order Amazon returned them.
        """
        headers = {_SCOPE: profile_id, "Content-Type": media_type, "Accept": media_type}
        body = {**body, "maxResults": constants.PAGE_SIZE}
        rows: list[dict[str, Any]] = []
        while True:
            response = self.client.post(path, headers=headers, json=body)
            response.raise_for_status()
            page = response.json()
            rows.extend(page.get(key, []))
            if not page.get("nextToken"):
                return rows
            body = {**body, "nextToken": page["nextToken"]}

    def offset_rows(self, profile_id: str, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of a list call that pages by ``startIndex``, until an empty page.

        Sponsored Display lists its entities by a GET answering a bare list of
        at most ``count`` rows from ``startIndex`` on, with no total.

        Args:
            profile_id: The profile whose entities are listed.
            path: The list call, such as ``/sd/campaigns/extended``.
            **params: The query parameters in Amazon's names, such as ``stateFilter``;
                the page size and the offset are added.

        Returns:
            Every row, in the order Amazon returned them.
        """
        pages = self.client.paginate(
            path,
            il.OffsetPaginator(limit=constants.PAGE_SIZE, offset_param="startIndex", limit_param="count"),
            params=params,
            headers={_SCOPE: profile_id},
        )
        return [row for page in pages for row in page]

    def report(self, profile_id: str, day: dt.date, **configuration: Any) -> list[dict[str, Any]]:
        """Read one day of any report: create its job, wait for it to complete, download its rows.

        Args:
            profile_id: The profile reported on.
            day: The report's only day.
            **configuration: What the report is, in Amazon's names: its
                ``reportTypeId``, ``adProduct``, ``groupBy``, ``columns`` and
                ``timeUnit``. The ``reportTypeId`` also names the job.

        Returns:
            The report's rows, as Amazon wrote them.
        """
        report_id = self._create_report(profile_id, day, configuration)
        url = self._await_report(profile_id, report_id)
        return self._download_report(url)

    def _create_report(self, profile_id: str, day: dt.date, configuration: dict[str, Any]) -> str:
        """Create a report job, asking again while Amazon refuses it.

        A refusal is asked again after a minute, then after five more minutes
        each time, up to fifteen.

        Args:
            profile_id: The profile reported on.
            day: The report's only day.
            configuration: What the report is, in Amazon's names; the file format is added.

        Returns:
            The job's id.
        """
        report_type_id = configuration["reportTypeId"]
        date = day.isoformat()
        body = {
            "name": f"{report_type_id} | {date} - {date}",
            "startDate": date,
            "endDate": date,
            "configuration": {**configuration, "format": "GZIP_JSON"},
        }
        headers = {_SCOPE: profile_id, "Content-Type": _REPORT_MEDIA_TYPE}
        retrying = self._retrying(
            stop=tc.stop_after_delay(constants.REPORT_CREATE_DEADLINE),
            wait=tc.wait_incrementing(start=60, increment=dt.timedelta(minutes=5), max=dt.timedelta(minutes=15)),
        )
        for attempt in retrying:
            with attempt:
                number = attempt.retry_state.attempt_number
                logger.info(f"Requesting {report_type_id} report for profile {profile_id} (attempt {number})")
                response = self.client.post("/reporting/reports", headers=headers, json=body)
                response.raise_for_status()
                job = response.json()
        report_id = job["reportId"]
        logger.info(f"Report id: {report_id} for profile {profile_id}")
        return report_id

    def _await_report(self, profile_id: str, report_id: str) -> str:
        """Poll a report job until it completes, polling again after an HTTP error.

        Args:
            profile_id: The profile reported on.
            report_id: The job's id.

        Returns:
            The signed URL of the completed report's file.
        """
        polling = self._retrying(
            stop=tc.stop_after_delay(constants.REPORT_WAIT_DEADLINE),
            retry=tc.retry_if_result(self._pending) | tc.retry_if_exception_type(httpx2.HTTPError),
        )
        report = polling(self._poll_report, profile_id, report_id)
        logger.info(f"Report URL: {report['url']} for profile {profile_id}")
        return report["url"]

    def _poll_report(self, profile_id: str, report_id: str) -> dict[str, Any]:
        """Read a report job's status once.

        Args:
            profile_id: The profile reported on.
            report_id: The job's id.

        Returns:
            The job as Amazon describes it, its ``url`` set once its ``status`` is ``COMPLETED``.
        """
        logger.info(f"Waiting for report {report_id} (profile {profile_id}) ")
        headers = {_SCOPE: profile_id, "Content-Type": _REPORT_MEDIA_TYPE}
        response = self.client.get(f"/reporting/reports/{report_id}", headers=headers)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _pending(report: dict[str, Any]) -> bool:
        """Whether a report job has yet to complete.

        Args:
            report: The job as a poll read it.

        Returns:
            True unless its status is ``COMPLETED``.
        """
        return report["status"] != "COMPLETED"

    def _download_report(self, url: str) -> list[dict[str, Any]]:
        """Download a completed report's file and decompress its rows, downloading again after a failure.

        The file lives outside the API and its URL carries its own signature,
        so it is fetched without the client's credentials.

        Args:
            url: The signed URL of the report's file.

        Returns:
            The report's rows.
        """
        for attempt in self._retrying(stop=tc.stop_after_delay(constants.REPORT_DOWNLOAD_DEADLINE)):
            with attempt:
                logger.info(f"Downloading report (attempt {attempt.retry_state.attempt_number})")
                response = httpx2.get(url, timeout=None)
                response.raise_for_status()
                rows = json.loads(gzip.decompress(response.content))
        return rows

    def _retrying(self, **options: Any) -> tc.Retrying:
        """Build the retry loop of one phase of a report job.

        A phase waits ten seconds, doubling to a minute, unless it says
        otherwise; drops the cached access token before each attempt; and logs
        each repeat. Past its deadline it raises the last attempt's error, or
        tenacity's ``RetryError`` when the last attempt found the job pending.

        Args:
            **options: The phase's ``stop``, and its ``wait`` or ``retry`` where they differ.

        Returns:
            The retry loop.
        """
        defaults: dict[str, Any] = {
            "sleep": sleep,
            "wait": tc.wait_exponential(multiplier=10, max=60),
            "before": self._forget_token,
            "before_sleep": self._log_retry,
            "reraise": True,
        }
        return tc.Retrying(**{**defaults, **options})

    def _forget_token(self, retry_state: tc.RetryCallState) -> None:
        """Drop the cached access token, so the attempt about to run authenticates afresh.

        Args:
            retry_state: The attempt about to run.
        """
        if isinstance(self.client.auth, il.OAuth2RefreshTokenAuth):
            self.client.auth.clear_token()

    @staticmethod
    def _log_retry(retry_state: tc.RetryCallState) -> None:
        """Log why an attempt is repeated and how long until the next one.

        Args:
            retry_state: The attempt that failed or found the job pending.

        Raises:
            RuntimeError: If tenacity calls it before the attempt has an outcome or a next action.
        """
        if retry_state.outcome is None:
            raise RuntimeError("log called before outcome was set")
        if retry_state.next_action is None:
            raise RuntimeError("log called before next_action was set")

        if retry_state.outcome.failed:
            error = retry_state.outcome.exception()
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Raised {error.__class__.__name__}: {error}.")
            if isinstance(error, httpx2.HTTPStatusError) and error.response.status_code == 429:
                logger.debug(f"Retry-After response header: {error.response.headers.get('Retry-After')}")
        else:
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Returned {retry_state.outcome.result()}")
