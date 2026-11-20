"""The Brandwatch (Falcon.io) Measure API engine every Brandwatch request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from asyncio import get_running_loop, sleep
from typing import Any

import httpx2
import interloper as il

from interloper_assets.brandwatch import constants

logger = logging.getLogger(__name__)


class BrandwatchApi:
    """The Brandwatch (Falcon.io) Measure API: its reads and its insights requests.

    Insights are read as a job. A POST names the days, the metrics and what
    else the insights are for (``channelIds`` on ``/insights/channel``), and is
    answered with an ``insightsRequestId``; the request is polled at
    ``/insights/{insightsRequestId}`` until it is ``READY``, then read there
    page by page, each page naming the ``nextPage`` until none is left. A
    request names at most ``MAX_METRICS_PER_REQUEST`` metrics, so a longer
    list is asked for in batches and their insights merged.

    Measure refuses a request with an HTTP error status, raised as it comes;
    a request that ends in any status but ``READY`` raises that status.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's API key.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/metrics/channel``.
            **params: The query parameters, beside the key the client sends on every request.

        Returns:
            The response's JSON body.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def report(
        self, path: str, day: dt.date, metric_ids: list[str], **body: Any
    ) -> dict[str, list[dict[str, Any]]]:
        """Read one day of insights.

        Args:
            path: The insights endpoint, relative to the API's base URL, such as ``/insights/channel``.
            day: The insights' only day, sent as the request's ``since`` and ``until``.
            metric_ids: The metrics to read, such as ``channel/fans/day``; any number, batched by the cap.
            **body: What else the request names, such as ``channelIds``, sent as is in its JSON body.

        Returns:
            Each metric's points as Measure sends them, by metric id, every batch and page merged
            in the order they were read.
        """
        insights: dict[str, list[dict[str, Any]]] = {}
        size = constants.MAX_METRICS_PER_REQUEST
        for start in range(0, len(metric_ids), size):
            batch = metric_ids[start : start + size]
            logger.info(f"Requesting Brandwatch {path} insights for {body} ({len(batch)} metrics)")
            request_id = await self._request(
                path, since=day.isoformat(), until=day.isoformat(), **body, metricIds=batch
            )
            await self._wait(request_id)
            pages = self.client.paginate(
                f"/insights/{request_id}",
                il.JSONCursorPaginator(cursor_path="data.paging.nextPage", cursor_param="page"),
                params={"page": 1},
                data_selector=self._insights,
            )
            async for page in pages:
                for metric_id, points in page.items():
                    insights.setdefault(metric_id, []).extend(points or [])
        return insights

    async def _request(self, path: str, **body: Any) -> str:
        """Ask for insights.

        Args:
            path: The insights endpoint, relative to the API's base URL.
            **body: The request's JSON body, in the order it is sent.

        Returns:
            The request's ``insightsRequestId``.
        """
        response = await self.client.post(path, json=body)
        response.raise_for_status()
        return response.json()["insightsRequestId"]

    async def _wait(self, request_id: str) -> None:
        """Poll an insights request until it is ``READY``.

        Each poll depends on the previous one's status, so the polls run one
        after another, ``POLL_INTERVAL`` apart, until ``POLL_TIMEOUT``. A
        response without a status is read as still in progress.

        Args:
            request_id: The request, as its ``insightsRequestId`` names it.

        Raises:
            RuntimeError: If the request ends in any status but ``READY``, or is not ready in time.
        """
        loop = get_running_loop()
        deadline = loop.time() + constants.POLL_TIMEOUT
        while True:
            response = await self.client.get(f"/insights/{request_id}")
            response.raise_for_status()
            status = response.json().get("status")

            if status == "READY":
                return
            if status and status != "IN_PROGRESS":
                raise RuntimeError(f"Insights request {request_id} failed with status {status}")

            if loop.time() >= deadline:
                raise RuntimeError(f"Insights request {request_id} not ready within {constants.POLL_TIMEOUT:.0f}s")
            await sleep(constants.POLL_INTERVAL)

    @staticmethod
    def _insights(response: httpx2.Response) -> dict[str, list[dict[str, Any]] | None]:
        """Select a page's insights, the way the paginating client reads each page.

        Args:
            response: A page's response, with a success status.

        Returns:
            The page's ``data.insights``, each metric's points by its id; empty when the page has none.
        """
        return response.json()["data"].get("insights") or {}
