"""The Campaign Manager 360 (DFA Reporting) API engine every Campaign Manager 360 request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable
from io import BytesIO
from time import monotonic, sleep
from typing import Any

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
    """The DFA Reporting API: its discovery methods, its paged lists and its report files.

    Every call builds its own discovery client with the connection's
    ``client``, which says why none is shared, and sends all its requests
    through it.

    `get` and `rows` name a method by its discovery id without the API's name
    (``userProfiles.list``, ``remarketingLists.list``) and pass its parameters
    under the vendor's names, the user profile (``profileId``) included. A
    refusal raises as Google's ``HttpError``, carrying the vendor's message.
    """

    def __init__(self, client: Callable[[], Any]) -> None:
        """Build the engine on the connection's client builder.

        Args:
            client: Builds a fresh authenticated ``dfareporting`` discovery client on each call.
        """
        self.client = client

    def get(self, method: str, **params: Any) -> dict[str, Any]:
        """Read the one response of a method that does not page.

        Args:
            method: The discovery method, such as ``userProfiles.list``.
            **params: The method's parameters, under the vendor's names.

        Returns:
            The response as the vendor sends it.
        """
        return self._request(self.client(), method, **params).execute()

    def rows(self, method: str, key: str, **params: Any) -> list[dict[str, Any]]:
        """Read every record of a paged list, following its ``nextPageToken``.

        Args:
            method: The discovery list method, such as ``remarketingLists.list``.
            key: The response field holding a page's records, such as ``remarketingLists``.
            **params: The method's parameters, under the vendor's names.

        Returns:
            The records of every page, in page order.
        """
        client = self.client()
        records: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self._request(client, method, **params, pageToken=page_token).execute()
            records.extend(response.get(key) or [])
            page_token = response.get("nextPageToken")
            if not page_token:
                return records

    def report(
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
            The file's rows under its human-readable headers, without its trailing totals; ``-`` reads as missing.
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
        client = self.client()
        report_id = int(client.reports().insert(profileId=profile_id, body=definition).execute()["id"])
        logger.info(f"Report id: {report_id} ({report_type})")
        try:
            file_id = int(client.reports().run(profileId=profile_id, reportId=report_id).execute()["id"])
            self._wait(client, profile_id, report_id, file_id)
            return self._records(self._download(client, report_id, file_id))
        finally:
            try:
                client.reports().delete(profileId=profile_id, reportId=report_id).execute()
            except Exception as exc:  # noqa: BLE001 - cleanup must not mask the report's own outcome
                logger.warning(f"Failed to delete report {report_id}: {exc}")

    @staticmethod
    def _request(client: Any, method: str, **params: Any) -> Any:
        """Build the request of a discovery method, walking its resources.

        Args:
            client: The discovery client to build it on.
            method: The method's discovery id without the API's name, such as ``reports.files.list``.
            **params: The method's parameters, under the vendor's names; ``None`` ones are not sent.

        Returns:
            The request, ready to ``execute``.
        """
        *resources, verb = method.split(".")
        target = client
        for resource in resources:
            target = getattr(target, resource)()
        return getattr(target, verb)(**params)

    @staticmethod
    def _wait(client: Any, profile_id: str, report_id: int, file_id: int) -> None:
        """Poll a report file until it is available, waiting longer each time, up to a cap.

        Args:
            client: The discovery client the report was run on.
            profile_id: The user profile the report runs as.
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
            status = (client.reports().files().get(profileId=profile_id, reportId=report_id, fileId=file_id).execute())[
                "status"
            ]

            if status == "REPORT_AVAILABLE":
                return
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Report {report_id} failed with status {status}")

            if monotonic() >= deadline:
                raise RuntimeError(f"Report {report_id} did not complete within {REPORT_TIMEOUT}s")
            sleep(interval)
            interval = min(interval * 2, MAX_RETRY_INTERVAL)

    @staticmethod
    def _download(client: Any, report_id: int, file_id: int) -> BytesIO:
        """Download a report file into memory.

        Args:
            client: The discovery client the report was run on.
            report_id: The report's definition.
            file_id: The file to download.

        Returns:
            The file's bytes, rewound.
        """
        from googleapiclient.http import MediaIoBaseDownload

        buffer = BytesIO()
        downloader = MediaIoBaseDownload(buffer, client.files().get_media(reportId=report_id, fileId=file_id))
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Downloading report {report_id}: {int(status.progress() * 100)}%")
        buffer.seek(0)
        return buffer

    @staticmethod
    def _records(file: BytesIO) -> list[dict[str, Any]]:
        """Read a report file's rows: skip the preamble, drop the trailing totals.

        The file opens with a metadata preamble ending at a ``Report Fields``
        line, then the CSV proper, closed by a ``Grand Total:`` summary row.

        Args:
            file: The downloaded report file, rewound.

        Returns:
            The rows under the file's headers; ``-`` reads as missing.
        """
        for line in file:
            if b"Report Fields" in line:
                break
        df = pd.read_csv(file, na_values=["-"])
        if df.empty:
            return []
        totals = df[df.iloc[:, 0].astype("string").str.contains("Grand Total:", na=False)].index
        if len(totals):
            df = df.iloc[: totals[-1]]
        return df.to_dict("records")
