"""The Display & Video 360 API engine every Display & Video 360 request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from asyncio import sleep
from io import StringIO
from time import monotonic
from typing import Any

import interloper as il
import pandas as pd

from interloper_assets.display_video_360.constants import (
    BID_MANAGER_URL,
    MAX_RETRY_INTERVAL,
    MIN_RETRY_INTERVAL,
    REPORT_TIMEOUT,
)

logger = logging.getLogger(__name__)


class DisplayVideo360Api:
    """Display & Video 360: its entity resources and its Bid Manager report queries.

    Entities are read from the Display & Video API, the client's base URL, by
    their path (``/partners``, ``/advertisers/9/lineItems``) with the API's
    camelCase query parameters. Reports are Bid Manager queries, sent through
    the same client: the connection's service account holds both APIs'
    scopes. A refused request raises ``httpx2.HTTPStatusError``.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, on the Display & Video API's base URL and
                authenticated as the connection's service account.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read one object of the Display & Video API.

        Args:
            path: The object's path, such as ``/firstPartyAndPartnerAudiences/7``.
            **params: The query parameters, the object's scope among them.

        Returns:
            The object as the API sends it.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every object of a Display & Video API list, page by page.

        A page names the next page with ``nextPageToken``, the last page none,
        and lists its objects in its one other field.

        Args:
            path: The list's path, such as ``/partners``.
            **params: The query parameters, the scope among them, sent with every page.

        Returns:
            The objects of every page, in page order.
        """
        paginator = il.JSONCursorPaginator(cursor_path="nextPageToken", cursor_param="pageToken")
        rows: list[dict[str, Any]] = []
        async for page in self.client.paginate(path, paginator, params=params):
            rows.extend(self._page_rows(path, page))
        return rows

    async def report(self, day: dt.date, *, title: str, **params: Any) -> list[dict[str, Any]]:
        """Read one day of any Bid Manager report, through a one-time query.

        The query is created, run, polled until its report is done, and
        deleted afterwards whatever happened, so one-time queries do not pile
        up; a failed deletion is only logged, the report being read by then.
        The file's summary footer starts at its first row with an empty first
        field, so the report's first ``groupBys`` dimension must be one every
        row fills.

        Args:
            day: The report's only day.
            title: The query's title, as it shows in Display & Video 360.
            **params: What the report is, the query's ``params``: its ``type``, ``groupBys``, ``filters``,
                ``metrics`` and ``options``.

        Returns:
            The report's rows, keyed by the CSV's column headers.
        """
        query_id = (await self._bid_manager("POST", "/queries", json=self._query(day, title, params)))["queryId"]
        logger.info(f"Query id: {query_id} ({title})")
        try:
            run = await self._bid_manager("POST", f"/queries/{query_id}:run", params={"synchronous": "false"})
            report = await self._wait_for_report(query_id, run["key"]["reportId"])
            # a signed storage URL: the service account's token is not sent there
            response = await self.client.get(report["metadata"]["googleCloudStoragePath"], auth=None, timeout=None)
            response.raise_for_status()
            return self._csv_rows(response.content.decode("utf-8-sig"))
        finally:
            try:
                await self._bid_manager("DELETE", f"/queries/{query_id}")
            except Exception as error:  # noqa: BLE001 - cleanup only, the report is already read or lost
                logger.warning(f"Failed to delete query {query_id}: {error}")

    async def _bid_manager(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Send one Bid Manager request and read its answer.

        Args:
            method: The HTTP method.
            path: The path under the Bid Manager API, such as ``/queries``.
            **kwargs: The request's ``params`` or ``json`` body.

        Returns:
            The answer's JSON body; an empty one (a deletion's) reads as ``{}``.
        """
        response = await self.client.request(method, f"{BID_MANAGER_URL}{path}", **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    @staticmethod
    def _page_rows(path: str, response: dict[str, Any]) -> list[dict[str, Any]]:
        """Select a page's objects, the one field it carries beside ``nextPageToken``.

        The field is found rather than named: it is named after the resource
        on most endpoints but not all (``customBiddingScripts`` for
        ``/customBiddingAlgorithms/5/scripts``), and an empty page leaves it out.

        Args:
            path: The list's path, named in the refusal.
            response: The page as the API sends it.

        Returns:
            The page's objects; none when the page carries no field beside ``nextPageToken``.

        Raises:
            RuntimeError: If the page carries more than one field beside ``nextPageToken``.
        """
        fields = [name for name in response if name != "nextPageToken"]
        if len(fields) > 1:
            raise RuntimeError(f"A page of {path} carries more than one field beside nextPageToken: {fields}")
        return response[fields[0]] if fields else []

    @staticmethod
    def _query(day: dt.date, title: str, params: dict[str, Any]) -> dict[str, Any]:
        """Build a one-time CSV query over a single day.

        Args:
            day: The query's only day.
            title: The query's title.
            params: The query's ``params``, as the caller passed them.

        Returns:
            The query, as ``queries.create`` takes it.
        """
        date = {"year": day.year, "month": day.month, "day": day.day}
        return {
            "metadata": {
                "title": title,
                "dataRange": {"range": "CUSTOM_DATES", "customStartDate": date, "customEndDate": date},
                "format": "CSV",
            },
            "params": params,
            "schedule": {"frequency": "ONE_TIME"},
        }

    async def _wait_for_report(self, query_id: str, report_id: str) -> dict[str, Any]:
        """Poll a report until it is done, waiting twice as long after each poll up to the maximum.

        Args:
            query_id: The query the report belongs to.
            report_id: The report being produced.

        Returns:
            The done report, whose metadata names the file to download.

        Raises:
            RuntimeError: If the report fails, or is not done within ``REPORT_TIMEOUT`` seconds.
        """
        deadline = monotonic() + REPORT_TIMEOUT
        interval = float(MIN_RETRY_INTERVAL)
        while True:
            logger.info(f"Waiting for report {report_id} (query {query_id})...")
            report = await self._bid_manager("GET", f"/queries/{query_id}/reports/{report_id}")
            state = report["metadata"]["status"]["state"]

            if state == "DONE":
                return report
            if state == "FAILED":
                raise RuntimeError(f"Report {report_id} (query {query_id}) failed")

            if monotonic() >= deadline:
                raise RuntimeError(f"Report {report_id} did not complete within {REPORT_TIMEOUT}s")
            await sleep(interval)
            interval = min(interval * 2, MAX_RETRY_INTERVAL)

    @staticmethod
    def _csv_rows(text: str) -> list[dict[str, Any]]:
        """Read a report file's rows, without its summary footer.

        The footer (totals, then the query's metadata) starts at the first
        line whose leading field is empty. A report without data carries a
        "No data returned by the reporting service." row instead of rows.

        Args:
            text: The report file, decoded.

        Returns:
            The rows, keyed by column header, a ``-`` read as missing.
        """
        lines = []
        for line in text.split("\n"):
            if line.startswith(","):
                break
            lines.append(line)
        df = pd.read_csv(StringIO("\n".join(lines)), na_values=["-"], on_bad_lines="skip")
        if df.empty:
            return []
        first = df.iloc[:, 0].astype("string")
        if first.str.contains("No data returned by the reporting service.", na=False).any():
            return []
        return df.to_dict("records")
