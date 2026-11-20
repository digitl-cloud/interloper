"""The Impact Advertiser API engine every Impact request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from asyncio import get_running_loop, sleep
from typing import Any

import interloper as il

from interloper_assets.impact import constants

logger = logging.getLogger(__name__)


class ImpactApi:
    """The Impact Advertiser API: its paged lists and its export jobs.

    A list answers ``@numpages`` on every page and holds the page's records
    under the resource's own name (``Actions``, ``Campaigns``), so the other
    pages are fetched concurrently, at most ``PAGE_CONCURRENCY`` at once.
    Impact refuses a request with an HTTP error status, raised as it comes.

    An export runs as a job: the export endpoint answers the job's
    ``QueuedUri``, the job is polled until it completes, and its results are
    downloaded as JSON, paged like a list.

    ``rows`` and ``report`` take a day and send it as a window over it, in
    UTC: Impact's ``StartDate`` and ``EndDate``, or the pair a list names for
    another of its dates (``ActionDateStart`` and ``ActionDateEnd``). An
    endpoint that names its day otherwise gets it from the caller with its
    other parameters.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, authenticated as the account and
                rooted at its ``/Advertisers/{AccountSID}`` URL.
        """
        self.client = client

    async def rows(
        self,
        path: str,
        key: str,
        day: dt.date | None = None,
        window: tuple[str, str] = ("StartDate", "EndDate"),
        **params: Any,
    ) -> list[dict[str, Any]]:
        """Read every record of a paged list.

        Args:
            path: The endpoint, relative to the account's URL, such as ``/Actions``.
            key: The key a page holds its records under, such as ``Actions``.
            day: The day the list covers, sent as a window over it; ``None`` sends no window.
            window: The parameters the window's first and last second go in: Impact's ``StartDate``
                and ``EndDate`` by default, or the pair that filters another of the list's dates,
                such as ``("ActionDateStart", "ActionDateEnd")`` for an action's event date.
            **params: The query parameters.

        Returns:
            The records of every page, in page order.
        """
        bounds = self._window(day, *window) if day is not None else {}
        return await self._records(path, key, {**params, **bounds})

    async def export(self, path: str, key: str, **params: Any) -> list[dict[str, Any]]:
        """Export records as a job: queue it, wait for it, then download every page of its results.

        Args:
            path: The export endpoint, relative to the account's URL, such as
                ``/Programs/{id}/ClickExport``.
            key: The key a page of the download holds its records under, such as ``Clicks``.
            **params: The export's query parameters; the results are always asked for as JSON.

        Returns:
            The records of every page of the job's download, in page order.

        Raises:
            RuntimeError: If Impact answers the export without queueing a job.
        """
        response = await self.client.get(path, params={**params, "ResultFormat": "JSON"})
        response.raise_for_status()
        answer = response.json()
        if not answer.get("QueuedUri"):
            raise RuntimeError(f"Impact did not queue {path}: {answer}")
        job_id = answer["QueuedUri"].split("/")[-1]
        await self._wait(job_id)
        return await self._records(
            f"/Jobs/{job_id}/Download",
            key,
            {"Mode": "stream"},
            headers={"Accept": "application/octet-stream"},
        )

    async def report(self, report_id: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Export one day of a report.

        Args:
            report_id: The report, such as ``att_adv_performance_by_day_pm_only``.
            day: The report's only day.
            **params: The report's other parameters, such as its ``SUBAID``.

        Returns:
            The report's records.
        """
        return await self.export(f"/ReportExport/{report_id}", "Records", **params, **self._window(day))

    async def _records(
        self, path: str, key: str, params: dict[str, Any], headers: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        """Read every page of a paged response and collect its records.

        A response without ``@numpages`` is read as the only page: an endpoint
        that does not page ignores ``Page`` and would answer the same records
        forever.

        Args:
            path: The endpoint, relative to the account's URL.
            key: The key a page holds its records under; a page without it, or with it null, holds none.
            params: The query parameters of every page.
            headers: Headers of every page, beside the client's; ``None`` adds none.

        Returns:
            The records of every page, in page order.
        """
        pages = self.client.paginate(
            path,
            il.PageNumberPaginator(page_param="Page", total_path="@numpages", maximum_page=1),
            params=params,
            headers=headers,
            concurrency=constants.PAGE_CONCURRENCY,
        )
        return [row async for page in pages for row in page.get(key) or []]

    async def _wait(self, job_id: str) -> None:
        """Poll a queued job until it completes.

        Each poll depends on the previous one's status, so the polls run one
        after another, ``JOB_POLL_INTERVAL`` apart, until ``JOB_POLL_TIMEOUT``.
        A refused poll raises as it comes.

        Args:
            job_id: The job, as its ``QueuedUri`` names it.

        Raises:
            RuntimeError: If the job is cancelled or fails, or does not complete in time.
        """
        loop = get_running_loop()
        deadline = loop.time() + constants.JOB_POLL_TIMEOUT
        while True:
            logger.info(f"Waiting for Impact job {job_id}...")
            response = await self.client.get(f"/Jobs/{job_id}")
            response.raise_for_status()
            status = response.json()["Status"]

            if status == "COMPLETED":
                return
            if status in ("CANCELLED", "ERROR", "FAILED"):
                raise RuntimeError(f"Impact job {job_id} failed with status {status}")

            if loop.time() >= deadline:
                raise RuntimeError(f"Impact job {job_id} did not complete within {constants.JOB_POLL_TIMEOUT:.0f}s")
            await sleep(constants.JOB_POLL_INTERVAL)

    @staticmethod
    def _window(day: dt.date, start: str = "StartDate", end: str = "EndDate") -> dict[str, str]:
        """Encode a day as Impact's window over it.

        Args:
            day: The day to cover.
            start: The parameter the window's first second goes in.
            end: The parameter the window's last second goes in.

        Returns:
            The window's first and last second of the day, in UTC.
        """
        return {start: day.strftime("%Y-%m-%dT00:00:00Z"), end: day.strftime("%Y-%m-%dT23:59:59Z")}
