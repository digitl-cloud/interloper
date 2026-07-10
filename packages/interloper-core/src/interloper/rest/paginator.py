"""Pagination strategies for the REST clients.

Pagination is sequential by nature — each next request is derived from the
previous response — so the base protocol (:class:`BasePaginator`) advances a
request page-by-page and works on both :class:`~interloper.rest.client.RESTClient`
and :class:`~interloper.rest.client.AsyncRESTClient`.

Where the full page set is knowable after the first response (page-number or
offset bounded by a total), :class:`RangePaginator` exposes every remaining
request at once, and :meth:`AsyncRESTClient.paginate` fetches them concurrently
via :func:`interloper.utils.bounded_gather`. The same paginator runs sequentially
on the sync client; concurrency is an opt-in capability, not a requirement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import httpx

# -- Data selection ------------------------------------------------------------


#: How a page's records are pulled from a response: a dotted JSON path
#: (``"data.list"``), a callable taking the response, or ``None`` (raw ``.json()``).
DataSelector = str | Callable[[httpx.Response], Any] | None

_MISSING = object()


def _extract(data: Any, path: str, default: Any = _MISSING) -> Any:
    """Walk a dot-separated key path through nested dicts.

    Keys may contain any characters except ``.`` (so ``@numpages`` works).

    Returns:
        The value at ``path``, or ``default`` if a key is missing.

    Raises:
        KeyError: If a key is missing and no ``default`` is given.
    """
    current = data
    for key in path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif default is _MISSING:
            raise KeyError(f"path {path!r} not found in response")
        else:
            return default
    return current


def select(response: httpx.Response, selector: DataSelector) -> Any:
    """Pull a page's records out of a response per ``selector``.

    Returns:
        The selected page data (callable result, dotted-path value, or raw json).
    """
    if selector is None:
        return response.json()
    if isinstance(selector, str):
        return _extract(response.json(), selector)
    return selector(response)


# -- Request helpers -----------------------------------------------------------


def _with_param(request: httpx.Request, key: str, value: Any) -> None:
    """Override a single query param on ``request`` in place."""
    request.url = request.url.copy_set_param(key, str(value))


def _clone_with_param(request: httpx.Request, key: str, value: Any) -> httpx.Request:
    """Copy ``request`` with one query param overridden (for concurrent fan-out).

    Returns:
        A new request identical to ``request`` but with ``key=value``.
    """
    return httpx.Request(request.method, request.url.copy_set_param(key, str(value)), headers=request.headers)


# -- Paginator protocols -------------------------------------------------------


class BasePaginator(ABC):
    """Sequential paginator: advance one request per page off each response.

    The client calls :meth:`init_request` once, then loops:
    send → :meth:`update_state` → (:attr:`has_next`?) → :meth:`update_request`.
    """

    def init_request(self, request: httpx.Request) -> None:
        """Set first-page params on the initial request (default: no-op)."""

    @abstractmethod
    def update_state(self, response: httpx.Response) -> None:
        """Read next-page info (cursor / link / total) from ``response`` and set state."""

    @abstractmethod
    def update_request(self, request: httpx.Request) -> None:
        """Mutate ``request`` in place to fetch the next page."""

    @property
    @abstractmethod
    def has_next(self) -> bool:
        """Whether another page should be fetched after the last response."""


class RangePaginator(BasePaginator):
    """A paginator whose complete page set is known after the first response.

    Adds the seam the async client uses to fetch pages 2..N concurrently. When
    the total can't be determined up front, :meth:`remaining_requests` returns
    ``None`` and the client falls back to the sequential walk.
    """

    @abstractmethod
    def remaining_requests(
        self, base_request: httpx.Request, first_response: httpx.Response
    ) -> list[httpx.Request] | None:
        """Requests for every page after the first, or ``None`` if the total is unknown."""


# -- Paginators ----------------------------------------------------------------


class SinglePagePaginator(BasePaginator):
    """No pagination — a single request is the whole result."""

    def update_state(self, response: httpx.Response) -> None:
        """No-op; there is never a next page."""

    def update_request(self, request: httpx.Request) -> None:
        """No-op; there is never a next page."""

    @property
    def has_next(self) -> bool:
        """Always ``False``."""
        return False


class PageNumberPaginator(RangePaginator):
    """Page-number pagination (``?page=1``, ``?page=2``, …).

    When the response carries a total (``total_path``) or a ``maximum_page`` is
    set, the page set is bounded and the async client fans out pages 2..N
    concurrently; otherwise it walks sequentially until an empty page.
    """

    def __init__(
        self,
        *,
        page_param: str = "page",
        base_page: int = 1,
        total_path: str | None = None,
        maximum_page: int | None = None,
        stop_on_empty: bool = True,
        data_selector: DataSelector = None,
    ) -> None:
        """Configure page-number pagination."""
        self.page_param = page_param
        self.base_page = base_page
        self.total_path = total_path
        self.maximum_page = maximum_page
        self.stop_on_empty = stop_on_empty
        self.data_selector = data_selector
        self._page = base_page
        self._has_next = True

    def init_request(self, request: httpx.Request) -> None:
        """Reset state and set the first page param."""
        self._page = self.base_page
        self._has_next = True
        _with_param(request, self.page_param, self.base_page)

    def _last_page(self, response: httpx.Response) -> int | None:
        total = _extract(response.json(), self.total_path, None) if self.total_path else None
        if total is not None:
            return int(total)
        return self.maximum_page

    def update_state(self, response: httpx.Response) -> None:
        """Stop at the known last page, or on an empty page when no total is known."""
        last = self._last_page(response)
        reached_last = last is not None and self._page >= last
        empty = self.stop_on_empty and not select(response, self.data_selector)
        self._has_next = not (reached_last or empty)

    def update_request(self, request: httpx.Request) -> None:
        """Advance to the next page number."""
        self._page += 1
        _with_param(request, self.page_param, self._page)

    @property
    def has_next(self) -> bool:
        """Whether another page remains."""
        return self._has_next

    def remaining_requests(
        self, base_request: httpx.Request, first_response: httpx.Response
    ) -> list[httpx.Request] | None:
        """Build requests for pages ``base_page+1..last`` when the total is known.

        Returns:
            One request per remaining page, or ``None`` if the total is unknown.
        """
        last = self._last_page(first_response)
        if last is None:
            return None  # unknown total → sequential fallback
        return [_clone_with_param(base_request, self.page_param, p) for p in range(self.base_page + 1, last + 1)]


class OffsetPaginator(RangePaginator):
    """Offset/limit pagination (``?offset=0&limit=100``, ``?offset=100&limit=100``, …).

    When the response carries a total record count (``total_path``) or a
    ``maximum_offset`` is set, the async client fans out the remaining offsets
    concurrently; otherwise it walks sequentially until an empty page.
    """

    def __init__(
        self,
        *,
        limit: int,
        offset_param: str = "offset",
        limit_param: str = "limit",
        base_offset: int = 0,
        total_path: str | None = None,
        maximum_offset: int | None = None,
        stop_on_empty: bool = True,
        data_selector: DataSelector = None,
    ) -> None:
        """Configure offset/limit pagination."""
        self.limit = limit
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.base_offset = base_offset
        self.total_path = total_path
        self.maximum_offset = maximum_offset
        self.stop_on_empty = stop_on_empty
        self.data_selector = data_selector
        self._offset = base_offset
        self._has_next = True

    def init_request(self, request: httpx.Request) -> None:
        """Reset state and set the first offset + limit params."""
        self._offset = self.base_offset
        self._has_next = True
        _with_param(request, self.limit_param, self.limit)
        _with_param(request, self.offset_param, self.base_offset)

    def _last_offset(self, response: httpx.Response) -> int | None:
        total = _extract(response.json(), self.total_path, None) if self.total_path else None
        if total is not None:
            return int(total)
        return self.maximum_offset

    def update_state(self, response: httpx.Response) -> None:
        """Stop once the next offset would reach the total, or on an empty page."""
        last = self._last_offset(response)
        reached_last = last is not None and self._offset + self.limit >= last
        empty = self.stop_on_empty and not select(response, self.data_selector)
        self._has_next = not (reached_last or empty)

    def update_request(self, request: httpx.Request) -> None:
        """Advance to the next offset."""
        self._offset += self.limit
        _with_param(request, self.offset_param, self._offset)

    @property
    def has_next(self) -> bool:
        """Whether another page remains."""
        return self._has_next

    def remaining_requests(
        self, base_request: httpx.Request, first_response: httpx.Response
    ) -> list[httpx.Request] | None:
        """Build requests for offsets ``limit, 2*limit, … < total`` when the total is known.

        Returns:
            One request per remaining offset, or ``None`` if the total is unknown.
        """
        last = self._last_offset(first_response)
        if last is None:
            return None
        offsets = range(self.base_offset + self.limit, last, self.limit)
        return [_clone_with_param(base_request, self.offset_param, o) for o in offsets]


class HeaderLinkPaginator(BasePaginator):
    """Follow the ``Link`` header's ``rel=next`` URL (RFC 5988). Sequential."""

    def __init__(self, *, rel: str = "next") -> None:
        """Follow the Link header rel."""
        self.rel = rel
        self._next_url: httpx.URL | None = None

    def update_state(self, response: httpx.Response) -> None:
        """Read the ``rel=next`` link from the response ``Link`` header."""
        link = response.links.get(self.rel)
        self._next_url = httpx.URL(link["url"]) if link else None

    def update_request(self, request: httpx.Request) -> None:
        """Point the request at the next link URL."""
        assert self._next_url is not None
        request.url = self._next_url

    @property
    def has_next(self) -> bool:
        """Whether the response carried a next link."""
        return self._next_url is not None


class JSONLinkPaginator(BasePaginator):
    """Follow a next-page URL found at a JSON path in the response. Sequential."""

    def __init__(self, *, next_url_path: str) -> None:
        """Follow a next-page URL at a JSON path."""
        self.next_url_path = next_url_path
        self._next_url: str | None = None

    def update_state(self, response: httpx.Response) -> None:
        """Read the next-page URL from the response JSON."""
        self._next_url = _extract(response.json(), self.next_url_path, None)

    def update_request(self, request: httpx.Request) -> None:
        """Point the request at the next URL (absolute or base-relative)."""
        assert self._next_url is not None
        request.url = request.url.join(self._next_url)

    @property
    def has_next(self) -> bool:
        """Whether the response carried a next URL."""
        return bool(self._next_url)


class JSONCursorPaginator(BasePaginator):
    """Carry a cursor value from the response JSON into the next request param. Sequential."""

    def __init__(self, *, cursor_path: str, cursor_param: str) -> None:
        """Carry a JSON cursor into the next request."""
        self.cursor_path = cursor_path
        self.cursor_param = cursor_param
        self._cursor: Any = None

    def update_state(self, response: httpx.Response) -> None:
        """Read the next cursor from the response JSON."""
        self._cursor = _extract(response.json(), self.cursor_path, None)

    def update_request(self, request: httpx.Request) -> None:
        """Set the cursor query param for the next page."""
        assert self._cursor is not None
        _with_param(request, self.cursor_param, self._cursor)

    @property
    def has_next(self) -> bool:
        """Whether the response carried a next cursor."""
        return bool(self._cursor)
