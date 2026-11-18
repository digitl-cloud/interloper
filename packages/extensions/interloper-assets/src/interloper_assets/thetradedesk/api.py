"""The Trade Desk API engine every Trade Desk request goes through."""

from __future__ import annotations

import csv
import datetime as dt
import logging
from asyncio import get_running_loop, sleep
from typing import Any

import httpx2
import interloper as il

from interloper_assets.thetradedesk import constants

logger = logging.getLogger(__name__)


class TheTradeDeskApi:
    """The Trade Desk's REST queries, GraphQL connections and MyReports reports.

    The REST API refuses a request with an HTTP status and a ``Message``,
    which `_raise_refusal` raises. The GraphQL API
    answers a failed query with HTTP 200 and an ``errors`` list, so every page
    is read through `_listing`, which raises them. A MyReports report is a
    one-time schedule whose execution queues server-side: `report` creates
    it, polls the execution with backoff, downloads its CSV delivery and
    deletes the schedule.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's API key.
        """
        self.client = client

    async def query(self, path: str, **filters: Any) -> list[dict[str, Any]]:
        """Read every page of a REST query.

        Each page starts at the number of rows read so far, until the rows
        reach the response's ``TotalFilteredCount`` or a page comes back
        empty. A response without ``TotalFilteredCount`` is read as the only page.

        Args:
            path: The query endpoint, relative to the REST API's base URL, such as ``/partner/query``.
            **filters: The query's filters, sent in its JSON body beside the page window.

        Returns:
            The ``Result`` rows of every page; none when the first ``Result`` is missing or null.
        """
        rows: list[dict[str, Any]] = []
        while True:
            page = {"PageStartIndex": len(rows), "PageSize": constants.QUERY_PAGE_SIZE}
            response = await self.client.post(path, json={**page, **filters})
            self._raise_refusal(response)
            payload = response.json()
            results = payload.get("Result") or []
            rows.extend(results)
            if not results or len(rows) >= (payload.get("TotalFilteredCount") or 0):
                return rows

    async def nodes(self, query: str, path: tuple[str, ...], **variables: Any) -> list[dict[str, Any]]:
        """Read every node of a GraphQL connection, page by page.

        The query declares ``$first`` and ``$after`` and selects ``nodes`` and
        ``pageInfo { hasNextPage endCursor }`` on the connection; each page
        asks for the next cursor until the connection says there is none. A
        connection without ``pageInfo`` is read as its only page.

        Args:
            query: The GraphQL document.
            path: The fields leading from ``data`` to the connection, such as
                ``("partner", "advertisers")``. A null connection under an
                existing object reads as no nodes.
            **variables: The query's variables, without ``first`` and ``after``.

        Returns:
            The nodes of every page, as the API returned them.

        Raises:
            RuntimeError: If a page says there is a next one but gives no cursor
                to ask for it with: asking again without one would read the first page forever.
        """
        nodes: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            response = await self.client.post(
                constants.GRAPHQL_URL,
                json={"query": query, "variables": {**variables, "first": constants.GRAPHQL_PAGE_SIZE, "after": after}},
            )
            response.raise_for_status()
            listing = self._listing(response, path, variables)
            if listing is None:
                return nodes
            nodes.extend(listing.get("nodes") or [])
            page = listing.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                return nodes
            after = page.get("endCursor")
            if not after:
                raise RuntimeError(
                    f"The Trade Desk said {'.'.join(path)} for {variables} has a next page but gave no cursor"
                )

    async def advertiser_nodes(self, partner_id: str, query: str, field: str, **variables: Any) -> list[dict[str, Any]]:
        """Read a connection under every advertiser of a partner.

        GraphQL lists campaigns and ad groups under an advertiser only, so the
        partner's advertisers are listed first and each one's connection is
        read concurrently, at most ``ADVERTISER_CONCURRENCY`` at once.

        Args:
            partner_id: The partner whose advertisers are walked.
            query: A query taking ``$advertiserId`` that pages the advertiser's connection.
            field: The advertiser's field the query pages through, such as ``campaigns`` or ``adGroups``.
            **variables: The query's other variables, sent with each advertiser's id.

        Returns:
            Every advertiser's nodes, as the API returned them.
        """
        advertisers = await self.nodes(constants.ADVERTISER_IDS_QUERY, ("partner", "advertisers"), partnerId=partner_id)
        pages = await il.bounded_gather(
            (
                self.nodes(query, ("advertiser", field), **variables, advertiserId=advertiser["id"])
                for advertiser in advertisers
            ),
            limit=constants.ADVERTISER_CONCURRENCY,
        )
        return [node for page in pages for node in page]

    async def report(self, partner_id: str, template_id: str, day: dt.date, **params: Any) -> list[dict[str, str]]:
        """Read one day of a MyReports report for a partner.

        The report runs as a one-time schedule filtered to the partner, which
        is deleted once the report is read or has failed.

        Args:
            partner_id: The partner the report is filtered to and whose executions are polled.
            template_id: The MyReports template the report is built from; its columns are the rows' keys.
            day: The report's only day.
            **params: The schedule's other settings, such as ``ReportDateFormat``,
                ``ReportNumericFormat`` and ``TimeZone``.

        Returns:
            The delivered CSV's rows keyed by its header, every value the string delivered.

        Raises:
            RuntimeError: If the execution completes without a delivery to download.
        """
        schedule_id = await self._schedule(partner_id, template_id, day, params)
        logger.info(f"Report schedule id: {schedule_id} (template {template_id})")
        try:
            execution = await self._execution(partner_id, schedule_id)
            deliveries = execution.get("ReportDeliveries") or []
            if not deliveries:
                raise RuntimeError(f"Report schedule {schedule_id} completed without a delivery")
            if len(deliveries) > 1:
                logger.warning(f"Schedule {schedule_id} has multiple deliveries; using the first")
            return await self._download(deliveries[0]["DownloadURL"])
        finally:
            await self._delete(schedule_id)

    async def _schedule(self, partner_id: str, template_id: str, day: dt.date, params: dict[str, Any]) -> str:
        """Create a one-time schedule of a report for one day.

        Args:
            partner_id: The partner the report is filtered to.
            template_id: The MyReports template the report is built from.
            day: The report's only day.
            params: The schedule's other settings, as the caller passed them.

        Returns:
            The schedule's id.
        """
        response = await self.client.post(
            "/myreports/reportschedule",
            json={
                "RequestedByUserName": "interloper",
                "IncludeHeaders": True,
                "PartnerFilters": [partner_id],
                "ReportDateRange": "Custom",
                "ReportStartDateInclusive": day.isoformat(),
                "ReportEndDateExclusive": (day + dt.timedelta(days=1)).isoformat(),
                "ReportFileFormat": "CSV",
                "ReportFrequency": "Once",
                "ReportScheduleName": f"interloper_{template_id}_{day.isoformat()}",
                "ReportTemplateId": template_id,
                **params,
            },
        )
        self._raise_refusal(response)
        return str(response.json()["ReportScheduleId"])

    async def _execution(self, partner_id: str, schedule_id: str) -> dict[str, Any]:
        """Poll a schedule's execution until it completes, waiting longer after each poll.

        Args:
            partner_id: The partner whose executions are queried.
            schedule_id: The schedule whose execution is awaited.

        Returns:
            The completed execution, carrying its ``ReportDeliveries``.

        Raises:
            RuntimeError: If the execution fails, or does not complete within ``REPORT_TIMEOUT`` seconds.
        """
        loop = get_running_loop()
        deadline = loop.time() + constants.REPORT_TIMEOUT
        interval = constants.POLL_START_INTERVAL
        while True:
            logger.info(f"Waiting for report schedule {schedule_id}...")
            executions = await self.query(
                "/myreports/reportexecution/query/partners", PartnerIds=[partner_id], ReportScheduleIds=[schedule_id]
            )
            if executions:
                if len(executions) > 1:
                    logger.warning(f"Schedule {schedule_id} has multiple executions; using the first")
                execution = executions[0]
                state = execution["ReportExecutionState"]
                if state == "Complete":
                    return execution
                if state in ("Failed", "Error"):
                    raise RuntimeError(f"Report schedule {schedule_id} failed with state {state}")

            if loop.time() >= deadline:
                raise RuntimeError(
                    f"Report schedule {schedule_id} did not complete within {constants.REPORT_TIMEOUT:.0f}s"
                )
            await sleep(interval)
            interval = min(interval * 2, constants.POLL_MAX_INTERVAL)

    async def _download(self, url: str) -> list[dict[str, str]]:
        """Download a report delivery and parse its CSV.

        Args:
            url: The delivery's ``DownloadURL``, which may redirect.

        Returns:
            The CSV's rows keyed by its header, every value a string.
        """
        response = await self.client.get(url, follow_redirects=True)
        response.raise_for_status()
        return list(csv.DictReader(response.text.splitlines()))

    async def _delete(self, schedule_id: str) -> None:
        """Delete a one-time schedule, logging a failure rather than raising it.

        A one-time schedule never runs again, so a leftover one only clutters
        the partner's schedules, and raising would hide the report's own outcome.

        Args:
            schedule_id: The schedule to delete.
        """
        try:
            response = await self.client.delete(f"/myreports/reportschedule/{schedule_id}")
            self._raise_refusal(response)
        except Exception as error:  # noqa: BLE001 - cleanup only, the report's outcome is already decided
            logger.warning(f"Failed to delete report schedule {schedule_id}: {error}")

    @staticmethod
    def _raise_refusal(response: httpx2.Response) -> None:
        """Raise a REST response's refusal, naming The Trade Desk's ``Message`` when it sends one.

        Args:
            response: A response from the REST API.

        Raises:
            httpx2.HTTPStatusError: If the response has an error status. Its message
                names the request, the status and the ``Message`` of the body;
                a body without one raises the status as httpx2 words it.
        """
        if response.is_success:
            return
        try:
            body = response.json()
        except ValueError:
            body = None
        message = body.get("Message") if isinstance(body, dict) else None
        if message:
            raise httpx2.HTTPStatusError(
                f"The Trade Desk refused {response.request.method} {response.request.url.path} "
                f"with HTTP {response.status_code}: {message}",
                request=response.request,
                response=response,
            )
        response.raise_for_status()

    @staticmethod
    def _listing(response: httpx2.Response, path: tuple[str, ...], variables: dict[str, Any]) -> dict[str, Any] | None:
        """Find the connection at a path of a GraphQL answer, raising the query's errors.

        Args:
            response: A page's response, with a success status.
            path: The fields leading from ``data`` to the connection.
            variables: The query's variables, named when the path's root is missing.

        Returns:
            The connection, or None when the root object has no such connection.

        Raises:
            RuntimeError: If the API answers with GraphQL errors, or with no
                object at the path's root: the key cannot see that id, and
                reading it as no nodes would snapshot an empty account.
        """
        payload = response.json()
        if payload.get("errors"):
            messages = "; ".join(error.get("message", str(error)) for error in payload["errors"])
            raise RuntimeError(f"The Trade Desk GraphQL query failed: {messages}")
        listing = (payload.get("data") or {}).get(path[0])
        if listing is None:
            raise RuntimeError(f"The Trade Desk returned no {path[0]} for {variables}: the API key cannot see it")
        for field in path[1:]:
            listing = listing.get(field) if listing is not None else None
        return listing
