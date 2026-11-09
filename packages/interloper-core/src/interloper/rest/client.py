"""REST clients (sync + async) extending httpx2 with pagination support."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar

import httpx2

from interloper.rest.paginator import BasePaginator, DataSelector, RangePaginator, select
from interloper.retry import RetryPolicy
from interloper.utils.concurrency import bounded_gather

logger = logging.getLogger(__name__)

# long enough for a vendor's slow report endpoints, short enough that a stalled read fails the run
DEFAULT_TIMEOUT = 300.0


class TransientRetry:
    """Which refused requests a REST client repeats, and how long it waits first.

    A throttled request (429) was refused before it did anything, so it is
    repeated whatever its method. A server error (5xx) or a dropped
    connection may have left the request half done, so only an idempotent
    request is repeated. The wait is the server's ``Retry-After`` when it
    gives one in seconds, else the policy's.
    """

    THROTTLED: ClassVar[int] = 429
    SERVER_ERRORS: ClassVar[frozenset[int]] = frozenset({500, 502, 503, 504})
    IDEMPOTENT: ClassVar[frozenset[str]] = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
    DEFAULT: ClassVar[RetryPolicy] = RetryPolicy(max_attempts=5, delay=2, max_delay=60)

    def __init__(self, policy: RetryPolicy) -> None:
        """Hold the attempt budget the client retries within.

        Args:
            policy: How many attempts a request gets and how long between them.
        """
        self.policy = policy

    def wait(self, request: httpx2.Request, attempt: int, response: httpx2.Response | None) -> float | None:
        """The seconds to wait before repeating a request, or None when it is not repeated.

        Args:
            request: The request sent.
            attempt: The attempt about to be made, the first repeat being 2.
            response: The server's answer, or None when the connection failed.

        Returns:
            The wait, or None for a success, a non-transient failure, or a spent budget.
        """
        if not self.policy.allows(attempt):
            return None
        idempotent = request.method in self.IDEMPOTENT
        if response is None:
            return self.policy.delay_before(attempt) if idempotent else None
        if response.status_code == self.THROTTLED:
            announced = response.headers.get("Retry-After", "")
            if announced.isdigit():
                return min(float(announced), self.policy.max_delay)
            return self.policy.delay_before(attempt)
        if response.status_code in self.SERVER_ERRORS and idempotent:
            return self.policy.delay_before(attempt)
        return None


class RESTClient(httpx2.Client):
    """A REST client that extends httpx2.Client with pagination support and transient retries."""

    def __init__(
        self,
        base_url: str,
        auth: httpx2.Auth | None = None,
        timeout: float | None = DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ):
        """Initialize the REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method (httpx2.Auth instance).
            timeout: Seconds a request may wait to connect or between bytes read; ``None``
                waits forever.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            retry: The budget for repeating throttled and transient failures (see
                `TransientRetry`); ``None`` for its default of 5 attempts.
            **kwargs: Additional keyword arguments to pass to httpx2.Client.
        """
        super().__init__(
            base_url=base_url,
            auth=auth,
            timeout=timeout,
            headers=headers,
            params=params,
            **kwargs,
        )
        self.retry = TransientRetry(retry or TransientRetry.DEFAULT)

    def send(self, request: httpx2.Request, **kwargs: Any) -> httpx2.Response:
        """Send a request, repeating it while it fails for a transient reason.

        Args:
            request: The request to send.
            **kwargs: The send options of httpx2.Client.send.

        Returns:
            The first response that is not repeated: a success, a lasting
            failure, or the last attempt's failure once the budget is spent.

        Raises:
            TransportError: When the connection fails and the request is not
                repeated: not idempotent, or the budget is spent.
        """
        attempt = 1
        while True:
            try:
                response = super().send(request, **kwargs)
            except httpx2.TransportError:
                wait = self.retry.wait(request, attempt + 1, None)
                if wait is None:
                    raise
            else:
                wait = self.retry.wait(request, attempt + 1, response)
                if wait is None:
                    return response
                response.close()
            logger.warning(f"Retrying {request.method} {request.url.path} in {wait:.1f}s (attempt {attempt + 1})")
            time.sleep(wait)
            attempt += 1

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
        `RangePaginator` pages concurrently.)

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


class AsyncRESTClient(httpx2.AsyncClient):
    """Async counterpart to `RESTClient` for IO-bound extraction.

    Same construction surface as `RESTClient`; a connection exposes it as
    ``client`` and uses it from an ``async def data()`` to overlap independent
    requests (paginated pages via `paginate`, per-entity calls via
    `interloper.utils.bounded_gather`).
    """

    def __init__(
        self,
        base_url: str,
        auth: httpx2.Auth | None = None,
        timeout: float | None = DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ):
        """Initialize the async REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method (httpx2.Auth instance).
            timeout: Seconds a request may wait to connect or between bytes read; ``None``
                waits forever.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            retry: The budget for repeating throttled and transient failures (see
                `TransientRetry`); ``None`` for its default of 5 attempts.
            **kwargs: Additional keyword arguments to pass to httpx2.AsyncClient.
        """
        super().__init__(
            base_url=base_url,
            auth=auth,
            timeout=timeout,
            headers=headers,
            params=params,
            **kwargs,
        )
        self.retry = TransientRetry(retry or TransientRetry.DEFAULT)

    async def send(self, request: httpx2.Request, **kwargs: Any) -> httpx2.Response:
        """Send a request, repeating it while it fails for a transient reason.

        Args:
            request: The request to send.
            **kwargs: The send options of httpx2.AsyncClient.send.

        Returns:
            The first response that is not repeated: a success, a lasting
            failure, or the last attempt's failure once the budget is spent.

        Raises:
            TransportError: When the connection fails and the request is not
                repeated: not idempotent, or the budget is spent.
        """
        attempt = 1
        while True:
            try:
                response = await super().send(request, **kwargs)
            except httpx2.TransportError:
                wait = self.retry.wait(request, attempt + 1, None)
                if wait is None:
                    raise
            else:
                wait = self.retry.wait(request, attempt + 1, response)
                if wait is None:
                    return response
                await response.aclose()
            logger.warning(f"Retrying {request.method} {request.url.path} in {wait:.1f}s (attempt {attempt + 1})")
            await asyncio.sleep(wait)
            attempt += 1

    async def _send_checked(self, request: httpx2.Request) -> httpx2.Response:
        """Send a request and raise on a non-success status.

        Args:
            request: The page request to send.

        Returns:
            The response, guaranteed to carry a success status.
        """
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

        For a `RangePaginator` whose total is
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
