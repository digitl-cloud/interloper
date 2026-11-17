"""The Display & Video 360 API engine every Display & Video 360 request goes through."""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections.abc import Callable
from io import StringIO
from typing import Any

import httpx2
import pandas as pd

from interloper_assets.display_video_360.constants import MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL, REPORT_TIMEOUT

logger = logging.getLogger(__name__)


class DisplayVideo360Api:
    """Display & Video 360: its entity resources and its Bid Manager report queries.

    Entities are read from the Display & Video API and reports from the Bid
    Manager API, each through a Google discovery service the connection
    builds. A service's ``httplib2`` transport is not thread-safe and the
    assets run concurrently in threads, so every method builds a service of
    its own and sends all of its requests on it.

    A resource is named by its path in the API's discovery document, dotted
    when it is nested (``partners``, ``advertisers.lineItems``), and takes the
    discovery method's own camelCase parameters. A refused request raises the
    client library's ``HttpError``, which carries Google's status and message.
    """

    def __init__(self, dv_client: Callable[[], Any], dbm_client: Callable[[], Any]) -> None:
        """Build the engine on the connection's two service builders.

        Args:
            dv_client: Builds a fresh Display & Video API service, authenticated with the connection's credentials.
            dbm_client: Builds a fresh Bid Manager API service, authenticated with the connection's credentials.
        """
        self.dv_client = dv_client
        self.dbm_client = dbm_client

    def get(self, resource: str, **params: Any) -> dict[str, Any]:
        """Read one object of a Display & Video API resource.

        Args:
            resource: The resource's dotted path, such as ``firstPartyAndPartnerAudiences``.
            **params: The ``get`` method's parameters, the object's id and scope among them.

        Returns:
            The object as the API sends it.
        """
        service = self.dv_client()
        return self._resource(service, resource).get(**params).execute()

    def rows(self, resource: str, **params: Any) -> list[dict[str, Any]]:
        """Read every object of a Display & Video API resource, page by page.

        A page names the next page with ``nextPageToken``, the last page none,
        and lists its objects in its one other field.

        Args:
            resource: The resource's dotted path, such as ``partners``.
            **params: The ``list`` method's parameters, the scope among them, sent with every page.

        Returns:
            The objects of every page, in page order.
        """
        service = self.dv_client()
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self._resource(service, resource).list(**params, pageToken=page_token).execute()
            rows.extend(self._page_rows(resource, response))
            page_token = response.get("nextPageToken")
            if not page_token:
                return rows

    def report(self, day: dt.date, *, title: str, **params: Any) -> list[dict[str, Any]]:
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
        service = self.dbm_client()
        query_id = service.queries().create(body=self._query(day, title, params)).execute()["queryId"]
        logger.info(f"Query id: {query_id} ({title})")
        try:
            report_id = service.queries().run(queryId=query_id, synchronous=False).execute()["key"]["reportId"]
            report = self._wait_for_report(service, query_id, report_id)
            response = httpx2.get(report["metadata"]["googleCloudStoragePath"], timeout=None)
            response.raise_for_status()
            return self._csv_rows(response.content.decode("utf-8-sig"))
        finally:
            try:
                service.queries().delete(queryId=query_id).execute()
            except Exception as error:  # noqa: BLE001 - cleanup only, the report is already read or lost
                logger.warning(f"Failed to delete query {query_id}: {error}")

    @staticmethod
    def _page_rows(resource: str, response: dict[str, Any]) -> list[dict[str, Any]]:
        """Select a page's objects, the one field it carries beside ``nextPageToken``.

        The field is found rather than named: it is named after the resource
        on most endpoints but not all (``customBiddingScripts`` for
        ``customBiddingAlgorithms.scripts``), and an empty page leaves it out.

        Args:
            resource: The resource's dotted path, named in the refusal.
            response: The page as the API sends it.

        Returns:
            The page's objects; none when the page carries no field beside ``nextPageToken``.

        Raises:
            RuntimeError: If the page carries more than one field beside ``nextPageToken``.
        """
        fields = [name for name in response if name != "nextPageToken"]
        if len(fields) > 1:
            raise RuntimeError(f"A page of {resource} carries more than one field beside nextPageToken: {fields}")
        return response[fields[0]] if fields else []

    @staticmethod
    def _resource(service: Any, path: str) -> Any:
        """Walk a discovery service down to one of its resources.

        Args:
            service: The discovery service the resource belongs to.
            path: The resource's dotted path, such as ``advertisers.lineItems``.

        Returns:
            The resource, whose methods build its requests.
        """
        resource = service
        for name in path.split("."):
            resource = getattr(resource, name)()
        return resource

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

    @staticmethod
    def _wait_for_report(service: Any, query_id: str, report_id: str) -> dict[str, Any]:
        """Poll a report until it is done, waiting twice as long after each poll up to the maximum.

        Args:
            service: The Bid Manager service the query was run on.
            query_id: The query the report belongs to.
            report_id: The report being produced.

        Returns:
            The done report, whose metadata names the file to download.

        Raises:
            RuntimeError: If the report fails, or is not done within ``REPORT_TIMEOUT`` seconds.
        """
        deadline = time.monotonic() + REPORT_TIMEOUT
        interval = float(MIN_RETRY_INTERVAL)
        while True:
            logger.info(f"Waiting for report {report_id} (query {query_id})...")
            report = service.queries().reports().get(queryId=query_id, reportId=report_id).execute()
            state = report["metadata"]["status"]["state"]

            if state == "DONE":
                return report
            if state == "FAILED":
                raise RuntimeError(f"Report {report_id} (query {query_id}) failed")

            if time.monotonic() >= deadline:
                raise RuntimeError(f"Report {report_id} did not complete within {REPORT_TIMEOUT}s")
            time.sleep(interval)
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
