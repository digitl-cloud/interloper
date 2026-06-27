"""REST clients (sync + async) extending httpx with pagination support."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx

from interloper.rest.paginator import BasePaginator, DataSelector, RangePaginator, select
from interloper.utils.concurrency import bounded_gather

logger = logging.getLogger(__name__)


class RESTClient(httpx.Client):
    """A REST client that extends httpx.Client with pagination support."""

    def __init__(
        self,
        base_url: str,
        auth: httpx.Auth | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        """Initialize the REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method (httpx.Auth instance).
            timeout: The timeout for requests.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            **kwargs: Additional keyword arguments to pass to httpx.Client.
        """
        super().__init__(
            base_url=base_url,
            auth=auth,
            timeout=timeout,
            headers=headers,
            params=params,
            **kwargs,
        )

    def paginate(
        self,
        path: str,
        paginator: BasePaginator,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data_selector: DataSelector = None,
    ) -> Iterator[Any]:
        """Walk a paginated resource, yielding one page of selected data at a time.

        Sequential by nature; each next request is derived from the previous
        response per ``paginator``. (The async client additionally fans out
        :class:`~interloper.rest.paginator.RangePaginator` pages concurrently.)

        Args:
            path: The resource path.
            paginator: The pagination strategy.
            params: Static query params applied to every page request.
            headers: Extra headers applied to every page request.
            data_selector: How to pull each page's records from the response.

        Yields:
            Each page's selected data.
        """
        request = self.build_request("GET", path, params=params, headers=headers)
        paginator.init_request(request)
        response = self.send(request)
        response.raise_for_status()
        yield select(response, data_selector)

        paginator.update_state(response)
        while paginator.has_next:
            paginator.update_request(request)
            response = self.send(request)
            response.raise_for_status()
            yield select(response, data_selector)
            paginator.update_state(response)


class AsyncRESTClient(httpx.AsyncClient):
    """Async counterpart to :class:`RESTClient` for IO-bound extraction.

    Same construction surface as :class:`RESTClient`; a connection exposes it as
    ``client`` and uses it from an ``async def data()`` to overlap independent
    requests (paginated pages via :meth:`paginate`, per-entity calls via
    :func:`interloper.utils.bounded_gather`).
    """

    def __init__(
        self,
        base_url: str,
        auth: httpx.Auth | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        """Initialize the async REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method (httpx.Auth instance).
            timeout: The timeout for requests.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            **kwargs: Additional keyword arguments to pass to httpx.AsyncClient.
        """
        super().__init__(
            base_url=base_url,
            auth=auth,
            timeout=timeout,
            headers=headers,
            params=params,
            **kwargs,
        )

    async def _send_checked(self, request: httpx.Request) -> httpx.Response:
        response = await self.send(request)
        response.raise_for_status()
        return response

    async def paginate(
        self,
        path: str,
        paginator: BasePaginator,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data_selector: DataSelector = None,
        concurrency: int = 8,
    ) -> AsyncIterator[Any]:
        """Walk a paginated resource, yielding one page of selected data at a time.

        For a :class:`~interloper.rest.paginator.RangePaginator` whose total is
        known after the first response, pages 2..N are fetched concurrently
        (bounded by ``concurrency``). Otherwise — cursor/link paginators, or an
        unknown total — it walks sequentially like the sync client.

        Args:
            path: The resource path.
            paginator: The pagination strategy.
            params: Static query params applied to every page request.
            headers: Extra headers applied to every page request.
            data_selector: How to pull each page's records from the response.
            concurrency: Max concurrent page requests for range paginators.

        Yields:
            Each page's selected data (first page first; the rest in page order).
        """
        request = self.build_request("GET", path, params=params, headers=headers)
        paginator.init_request(request)
        first = await self._send_checked(request)
        yield select(first, data_selector)

        # Fast path: known page set → fetch the rest concurrently.
        if isinstance(paginator, RangePaginator):
            rest = paginator.remaining_requests(request, first)
            if rest is not None:
                responses = await bounded_gather((self._send_checked(r) for r in rest), limit=concurrency)
                for response in responses:
                    yield select(response, data_selector)
                return

        # Sequential fallback: cursor / link, or unknown total.
        paginator.update_state(first)
        while paginator.has_next:
            paginator.update_request(request)
            response = await self._send_checked(request)
            yield select(response, data_selector)
            paginator.update_state(response)
