"""The Campaign Manager 360 (DFA Reporting) API engine every Campaign Manager 360 request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from asyncio import sleep
from io import BytesIO
from time import monotonic
from typing import Any

import interloper as il
import pandas as pd

from interloper_assets.campaign_manager_360.constants import MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL, REPORT_TIMEOUT

logger = logging.getLogger(__name__)

# a report definition holds its window, dimensions and metrics under a field named for its type; the
# path-to-conversion report has no plain dimensions and is not run this way
_CRITERIA_FIELD = {
    "STANDARD": "criteria",
    "REACH": "reachCriteria",
    "FLOODLIGHT": "floodlightCriteria",
    "CROSS_MEDIA_REACH": "crossMediaReachCriteria",
}


class CampaignManager360Api:
    """The DFA Reporting API: its JSON reads, its paged lists and its report files.

    `get` and `rows` take an endpoint's path under the API's base URL, the user
    profile included where the endpoint is scoped to one
    (``/userprofiles/111/remarketingLists``), and its query parameters under
    the vendor's names. A refusal raises ``httpx2.HTTPStatusError``.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, authenticated as the connection's service account.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint that does not page.

        Args:
            path: The endpoint's path, such as ``/userprofiles``.
            **params: The query parameters, under the vendor's names.

        Returns:
            The response as the vendor sends it.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, key: str, **params: Any) -> list[dict[str, Any]]:
        """Read every record of a paged list, following its ``nextPageToken``.

        Args:
            path: The list's path, such as ``/userprofiles/111/remarketingLists``.
            key: The response field holding a page's records, such as ``remarketingLists``.
            **params: The query parameters, under the vendor's names, sent with every page.

        Returns:
            The records of every page, in page order.
        """
        paginator = il.JSONCursorPaginator(cursor_path="nextPageToken", cursor_param="pageToken")
        records: list[dict[str, Any]] = []
        async for page in self.client.paginate(path, paginator, params=params):
            records.extend(page.get(key) or [])
        return records

    async def report(
        self,
        profile_id: str,
        day: dt.date,
        *,
        account_id: str,
        report_type: str,
        dimensions: list[str],
        metrics: list[str],
        **criteria: Any,
    ) -> list[dict[str, Any]]:
        """Run one day of a report and read its file.

        The report is a one-shot definition: inserted, run, polled until its
        file is available, downloaded and deleted, best-effort, whatever the
        outcome. Each of those requests is scoped to the profile, which comes
        first. A ``PATH_TO_CONVERSION`` report has no plain dimensions and
        raises ``KeyError`` before any request.

        Args:
            profile_id: The user profile the report runs as, its ``profileId``.
            day: The report's only day.
            account_id: The account the report reads.
            report_type: The vendor's report type, such as ``STANDARD`` or ``REACH``.
            dimensions: The dimension names to break the report down by.
            metrics: The metric names to report.
            **criteria: Any further criteria, under the vendor's names, such as ``dimensionFilters``.

        Returns:
            The file's rows under its human-readable headers, without its trailing totals; every value is the
            file's text, and ``-`` reads as missing.
        """
        window = {"startDate": day.isoformat(), "endDate": day.isoformat()}
        definition = {
            "accountId": account_id,
            "name": f"report_{report_type.lower()}_{window['startDate']}_{window['endDate']}",
            "type": report_type,
            _CRITERIA_FIELD[report_type]: {
                "dateRange": window,
                "dimensions": [{"name": dimension} for dimension in dimensions],
                "metricNames": metrics,
                **criteria,
            },
        }
        reports = f"/userprofiles/{profile_id}/reports"
        report_id = int((await self._send("POST", reports, json=definition))["id"])
        logger.info(f"Report id: {report_id} ({report_type})")
        try:
            file_id = int((await self._send("POST", f"{reports}/{report_id}/run"))["id"])
            await self._wait(f"{reports}/{report_id}/files/{file_id}", report_id, file_id)
            logger.info(f"Downloading report {report_id} (file {file_id})...")
            response = await self.client.get(f"/reports/{report_id}/files/{file_id}", params={"alt": "media"})
            response.raise_for_status()
            return self._records(BytesIO(response.content))
        finally:
            try:
                await self._send("DELETE", f"{reports}/{report_id}")
            except Exception as error:  # noqa: BLE001 - cleanup must not mask the report's own outcome
                logger.warning(f"Failed to delete report {report_id}: {error}")

    async def _send(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Send one request of a report's lifecycle and read its answer.

        Args:
            method: The HTTP method.
            path: The endpoint's path.
            **kwargs: The request's ``json`` body, if any.

        Returns:
            The answer's JSON body; an empty one (a deletion's) reads as ``{}``.
        """
        response = await self.client.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    async def _wait(self, path: str, report_id: int, file_id: int) -> None:
        """Poll a report file until it is available, waiting longer each time, up to a cap.

        Args:
            path: The file's path under its profile's reports.
            report_id: The report's definition.
            file_id: The file the run produces.

        Raises:
            RuntimeError: If the report fails or is cancelled, or its file is not
                available within ``REPORT_TIMEOUT``.
        """
        deadline = monotonic() + REPORT_TIMEOUT
        interval = float(MIN_RETRY_INTERVAL)
        while True:
            logger.info(f"Waiting for report {report_id} (file {file_id})...")
            status = (await self._send("GET", path))["status"]

            if status == "REPORT_AVAILABLE":
                return
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Report {report_id} failed with status {status}")

            if monotonic() >= deadline:
                raise RuntimeError(f"Report {report_id} did not complete within {REPORT_TIMEOUT}s")
            await sleep(interval)
            interval = min(interval * 2, MAX_RETRY_INTERVAL)

    @staticmethod
    def _records(file: BytesIO) -> list[dict[str, Any]]:
        """Read a report file's rows: skip the preamble, drop the trailing totals.

        The file opens with a metadata preamble ending at a ``Report Fields``
        line, then the CSV proper, closed by a ``Grand Total:`` summary row.
        Every column is read as text: the totals row leaves the id columns
        blank, so a guessed dtype would read them as floats (``123.0``, and
        digits lost above 2^53). The schema's conformer casts the metrics.

        Args:
            file: The downloaded report file, rewound.

        Returns:
            The rows under the file's headers, every value the file's text; ``-`` reads as missing.
        """
        for line in file:
            if b"Report Fields" in line:
                break
        df = pd.read_csv(file, dtype=str, na_values=["-"])
        if df.empty:
            return []
        totals = df[df.iloc[:, 0].astype("string").str.contains("Grand Total:", na=False)].index
        if len(totals):
            df = df.iloc[: totals[-1]]
        return df.to_dict("records")
