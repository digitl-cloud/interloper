"""The Selling Partner API engine every Amazon Selling Partner request goes through."""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
from asyncio import get_running_loop, sleep
from typing import Any

import httpx2
import interloper as il

from interloper_assets.amazon_selling_partner import constants

logger = logging.getLogger(__name__)

_REPORTS = f"/reports/{constants.REPORT_API_VERSION}"
_DATA_KIOSK = f"/dataKiosk/{constants.DATAKIOSK_API_VERSION}"
_FINISHED = ("DONE", "CANCELLED", "FATAL")
_TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})


class AmazonSellingPartnerApi:
    """The Selling Partner API: its report jobs and its Data Kiosk queries.

    Both are jobs: created by a POST, polled every ``POLL_INTERVAL`` seconds
    until Amazon reports them ``DONE``, ``CANCELLED`` or ``FATAL``, then read
    from a document Amazon serves at a signed URL outside the API, which is
    fetched without the client's credentials but within its timeout and
    transient retries. A report's document is JSON, gzipped when Amazon names
    its ``compressionAlgorithm``; a query's is JSON Lines, one record per line.

    Amazon refuses a request with an HTTP error status, raised as is, except
    while polling: a poll that fails transiently is polled again until the
    job's deadline. No call is scoped by a header: what a job reads, its
    marketplaces included, is named in its own specification, which the asset
    passes.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every API request goes through, carrying the
                connection's Login with Amazon access token.
        """
        self.client = client

    async def report(
        self,
        *,
        key: str,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        **specification: Any,
    ) -> list[dict[str, Any]]:
        """Run a report job and read one list of its document.

        A failed job may still attach a document naming the cause. A cause that
        says the data is not yet available reads as no rows, so a day Amazon
        has yet to compute is written empty rather than failed.

        Args:
            key: The document's list the rows are read from, such as ``salesByAsin``;
                a report document holds its lists at its top level.
            start_date: The first day reported on, from its start; ``None`` sends no start.
            end_date: The last day reported on, to its end; ``None`` sends no end.
            **specification: What the report is, in Amazon's names: its ``reportType``,
                ``marketplaceIds`` and ``reportOptions``.

        Returns:
            The list's records as Amazon wrote them, or none when the document has no such list.

        Raises:
            RuntimeError: If the job is unfinished after ``REPORT_TIMEOUT`` seconds,
                ends without a document, or fails for any other cause than data
                not yet available.
        """
        window = {}
        if start_date is not None:
            window["dataStartTime"] = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        if end_date is not None:
            window["dataEndTime"] = end_date.strftime("%Y-%m-%dT23:59:59.999Z")
        response = await self.client.post(f"{_REPORTS}/reports", json=dict(**specification, **window))
        response.raise_for_status()
        report_id = response.json()["reportId"]
        logger.info(f"Report id: {report_id} ({specification.get('reportType')})")

        job = await self._wait(f"{_REPORTS}/reports/{report_id}", "Report", report_id, constants.REPORT_TIMEOUT)
        status = job["processingStatus"]
        document_id = job.get("reportDocumentId")
        if not document_id:
            raise RuntimeError(f"Report {report_id} failed with status {status}")

        response = await self.client.get(f"{_REPORTS}/documents/{document_id}")
        response.raise_for_status()
        reference = response.json()
        content = (await self._download(reference["url"])).content
        if reference.get("compressionAlgorithm") == "GZIP":
            content = gzip.decompress(content)
        document = json.loads(content)

        if status != "DONE":
            details = document.get("errorDetails", "") if isinstance(document, dict) else ""
            if isinstance(details, str) and "not yet available" in details:
                logger.warning(f"Report {report_id}: {details}")
                return []
            raise RuntimeError(f"Report {report_id} failed: {details or status}")
        return document.get(key, []) if document else []

    async def query(self, **specification: Any) -> list[dict[str, Any]]:
        """Run a Data Kiosk query and read every record of every page of its result.

        Amazon writes a result as JSON Lines, one record per line, and splits a
        large one into pages: each next page is a query of its own, created from
        the same specification and the previous page's ``nextToken``. A token
        already sent ends the paging, as it would read the same pages again.

        Args:
            **specification: The query in Amazon's names: its GraphQL ``query``;
                each next page's ``paginationToken`` is added.

        Returns:
            Every record as Amazon wrote it, page after page; none when no page has a document.

        Raises:
            RuntimeError: If a page's query is unfinished after ``QUERY_TIMEOUT`` seconds,
                or is cancelled or fails, with its error document's message when it names one.
        """
        records: list[dict[str, Any]] = []
        page = specification
        sent: set[str] = set()
        while True:
            response = await self.client.post(f"{_DATA_KIOSK}/queries", json=page)
            response.raise_for_status()
            query_id = response.json()["queryId"]
            logger.info(f"Query id: {query_id}")

            job = await self._wait(f"{_DATA_KIOSK}/queries/{query_id}", "Query", query_id, constants.QUERY_TIMEOUT)
            status = job["processingStatus"]
            if status != "DONE":
                message = f"Query {query_id} failed with status {status}"
                if job.get("errorDocumentId"):
                    message = f"{message}: {await self._error(job['errorDocumentId'])}"
                raise RuntimeError(message)
            if job.get("dataDocumentId"):
                download = await self._document(job["dataDocumentId"])
                records.extend(json.loads(line) for line in download.content.splitlines() if line.strip())

            token = job.get("pagination", {}).get("nextToken")
            if not token:
                return records
            if token in sent:
                logger.warning(f"Query {query_id} returned the page token {token} again; reading no further")
                return records
            sent.add(token)
            page = {**specification, "paginationToken": token}

    async def _document(self, document_id: str) -> httpx2.Response:
        """Download a Data Kiosk document, its data or its error, by its id.

        Args:
            document_id: The document's id, a query's ``dataDocumentId`` or ``errorDocumentId``.

        Returns:
            The document's response, its content read and decompressed as its ``Content-Encoding`` says.
        """
        response = await self.client.get(f"{_DATA_KIOSK}/documents/{document_id}")
        response.raise_for_status()
        return await self._download(response.json()["documentUrl"])

    async def _error(self, document_id: str) -> Any:
        """Read why a query failed from its error document.

        Args:
            document_id: The failed query's ``errorDocumentId``.

        Returns:
            The document's ``errorMessage``, or the whole document when it names none.
        """
        error = (await self._document(document_id)).json()
        return error.get("errorMessage", error) if isinstance(error, dict) else error

    async def _wait(self, path: str, noun: str, job_id: str, timeout: float) -> dict[str, Any]:
        """Poll a job until it is finished, polling again after a transient failure.

        Each poll depends on the previous status, so this is a sequential loop,
        one poll every ``POLL_INTERVAL`` seconds.

        Args:
            path: The job's status call, relative to the region's API URL.
            noun: What the job is in messages, ``Report`` or ``Query``.
            job_id: The job's id.
            timeout: Seconds after which a job still unfinished fails.

        Returns:
            The job as Amazon describes it, its ``processingStatus`` one of ``DONE``,
            ``CANCELLED`` or ``FATAL``.

        Raises:
            HTTPError: If a poll fails for a reason that is not transient, such as a 403.
            RuntimeError: If the job is still unfinished, or its last poll failed, past the deadline.
        """
        loop = get_running_loop()
        deadline = loop.time() + timeout
        while True:
            logger.info(f"Waiting for {noun.lower()} {job_id}...")
            failure: httpx2.HTTPError | None = None
            try:
                response = await self.client.get(path)
                response.raise_for_status()
                job = response.json()
            except httpx2.HTTPError as error:
                if not self._transient(error):
                    raise
                logger.debug(f"Transient error polling {noun.lower()} {job_id}: {error}")
                failure = error
            else:
                if job["processingStatus"] in _FINISHED:
                    return job
            if loop.time() >= deadline:
                raise RuntimeError(f"{noun} {job_id} did not complete within {timeout:.0f}s") from failure
            await sleep(constants.POLL_INTERVAL)

    @staticmethod
    def _transient(error: httpx2.HTTPError) -> bool:
        """Whether a failed poll may succeed if polled again.

        A dropped connection, a throttled poll and a server error are
        transient; any other refusal, such as a 403 or 404 for a job the
        credentials cannot read, would be refused again.

        Args:
            error: What the poll raised.

        Returns:
            True for a transport error or a status in ``_TRANSIENT_STATUSES``.
        """
        if isinstance(error, httpx2.HTTPStatusError):
            return error.response.status_code in _TRANSIENT_STATUSES
        return isinstance(error, httpx2.TransportError)

    async def _download(self, url: str) -> httpx2.Response:
        """Download a finished job's document from its signed URL.

        The URL carries its own signature, so the document is fetched without
        the API's credentials, through the client for its timeout and its
        repeats of a transient failure, and a refusal is raised without the URL.

        Args:
            url: The document's signed URL.

        Returns:
            The document's response, its content read.

        Raises:
            HTTPStatusError: If the document's host refuses the download.
        """
        response = await self.client.get(url, auth=None)
        if response.is_error:
            message = f"Document download refused with HTTP {response.status_code}"
            raise httpx2.HTTPStatusError(message, request=response.request, response=response)
        return response
