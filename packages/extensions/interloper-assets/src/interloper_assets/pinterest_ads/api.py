"""The Pinterest Ads API engine every Pinterest request goes through."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import httpx2
import interloper as il

from interloper_assets.pinterest_ads.constants import DOWNLOAD_TIMEOUT, PAGE_SIZE

_POLL_INTERVAL = 10.0
_REPORT_TIMEOUT = 30 * 60
_REPORT_FAILED_STATUSES = frozenset({"DOES_NOT_EXIST", "EXPIRED", "FAILED", "CANCELLED"})


class PinterestAdsApiError(httpx2.HTTPStatusError):
    """Pinterest refused a request, with its own code and message.

    It is the HTTP error status Pinterest answered, so whatever reads an
    ``httpx2.HTTPStatusError`` reads it unchanged. Its message is Pinterest's
    reason rather than httpx's, which names the request URL.
    """

    def __init__(self, message: str, *, code: int | None, response: httpx2.Response) -> None:
        """Hold Pinterest's code beside the refused response.

        Args:
            message: What Pinterest refused and why.
            code: Pinterest's ``code``, or None when the body carries none.
            response: The refused response.
        """
        super().__init__(message, request=response.request, response=response)
        self.code = code

    @classmethod
    def from_response(cls, response: httpx2.Response) -> PinterestAdsApiError:
        """Read Pinterest's refusal from an error response.

        Args:
            response: A response with an error status.

        Returns:
            The refusal, with Pinterest's ``code`` and ``message``, or the
            status's reason when the body is not Pinterest's error object.
        """
        try:
            body = response.json()
        except ValueError:
            body = None
        error = body if isinstance(body, dict) else {}
        code = error.get("code")
        status = f"HTTP {response.status_code}" if code is None else f"HTTP {response.status_code}, code {code}"
        reason = error.get("message") or response.reason_phrase
        return cls(f"Pinterest refused the request ({status}): {reason}", code=code, response=response)


class PinterestAdsApi:
    """The Pinterest v5 API: its objects, its bookmarked lists and its report jobs.

    Pinterest refuses a request with an HTTP error status, raised as
    `PinterestAdsApiError` with Pinterest's code and message.
    A list pages by the ``bookmark`` its last response carries. A report is a
    job: created on the account's ``/reports`` endpoint, polled by its token
    until it finishes, then read from a signed file that maps each entity id to
    its rows.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every API request goes through, renewing the connection's access token.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint that does not page.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/ad_accounts/{ad_account_id}``.
            **params: The query parameters; a list value repeats its parameter.

        Returns:
            The response's object.
        """
        return self._body(await self.client.get(path, params=params))

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every item of a bookmarked list.

        Pages are read one after the other, ``PAGE_SIZE`` items at a time, each
        asking for the ``bookmark`` the page before carried. A response without
        one is the last page. A refused page raises as `PinterestAdsApiError`.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/ad_accounts/{ad_account_id}/campaigns``.
            **params: The query parameters, sent with every page; a list value repeats its parameter.

        Returns:
            The ``items`` of every page, in page order.
        """
        pages = self.client.paginate(
            path,
            il.JSONCursorPaginator(cursor_path="bookmark", cursor_param="bookmark"),
            params={**params, "page_size": PAGE_SIZE},
            data_selector="items",
        )
        try:
            return [row async for page in pages for row in page]
        except httpx2.HTTPStatusError as error:
            refusal = PinterestAdsApiError.from_response(error.response)
            raise refusal from error

    async def report(self, account_id: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of an account's analytics report, through a report job.

        The job is asked for in JSON, the one format the engine reads, and
        polled every ``_POLL_INTERVAL`` seconds for at most ``_REPORT_TIMEOUT``.

        Args:
            account_id: The ad account the report covers.
            day: The report's only day.
            **params: What the report is: its ``granularity``, ``level`` and ``columns``,
                and its ``targeting_types`` for a targeting level.

        Returns:
            The rows of every entity in the report's file, empty when the account had no delivery that day.
        """
        path = f"/ad_accounts/{account_id}/reports"
        window = {"start_date": day.isoformat(), "end_date": day.isoformat()}
        job = self._body(await self.client.post(path, json=dict(**window, **params, report_format="JSON")))
        url = await self._wait(path, job["token"])
        if not url:
            return []
        return await self._download(url)

    async def _wait(self, path: str, token: str) -> str | None:
        """Poll a report job until it finishes.

        Args:
            path: The account's ``/reports`` endpoint the job was created on.
            token: The job's token.

        Returns:
            The signed URL of the job's file, or ``None`` when the report has no file.

        Raises:
            RuntimeError: If the job ends without finishing, or does not finish within ``_REPORT_TIMEOUT``.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _REPORT_TIMEOUT
        while True:
            status = self._body(await self.client.get(path, params={"token": token}))
            if status["report_status"] == "FINISHED":
                return status.get("url")
            if status["report_status"] in _REPORT_FAILED_STATUSES:
                raise RuntimeError(f"Pinterest report {token} ended with status {status['report_status']}")
            if loop.time() >= deadline:
                raise RuntimeError(f"Pinterest report {token} was not ready within {_REPORT_TIMEOUT}s")
            await asyncio.sleep(_POLL_INTERVAL)

    @staticmethod
    def _body(response: httpx2.Response) -> Any:
        """Read the body of an API response.

        A response with an error status raises as `PinterestAdsApiError`.

        Args:
            response: The API's answer to one request.

        Returns:
            The response's JSON body.
        """
        if response.is_error:
            refusal = PinterestAdsApiError.from_response(response)
            raise refusal
        return response.json()

    @staticmethod
    async def _download(url: str) -> list[dict[str, Any]]:
        """Read a finished report's file.

        The signed URL rejects the API's bearer token, so the file is read
        without the client's authentication. Its refusals are the storage's,
        not Pinterest's, so they raise as httpx reads them.

        Args:
            url: The signed URL of the file.

        Returns:
            The rows of every entity in the file, in file order; none for an empty file.
        """
        async with httpx2.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
            download = await client.get(url)
        download.raise_for_status()
        if not download.content:
            return []
        return [row for rows in download.json().values() for row in rows]
