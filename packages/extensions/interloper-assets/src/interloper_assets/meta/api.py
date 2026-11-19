"""Meta's Graph API: the rate limits every Meta engine waits out, and the engine the Insights sources read through."""

from __future__ import annotations

import datetime as dt
import json
import logging
from asyncio import sleep
from dataclasses import dataclass, field
from typing import Any, ClassVar

import httpx2
import interloper as il

logger = logging.getLogger(__name__)


@dataclass
class Attempts:
    """What one Graph request has spent waiting out throttling and repeating transient failures.

    Graph refuses a throttled request with one of ``THROTTLE_CODES``, under a
    400 or 403 rather than a 429, and may announce how long the caller is
    locked out. Throttling is waited out within a total budget, since Meta can
    lock an account for up to an hour; a transient failure is repeated a few
    times only, since one that persists is not transient.
    """

    THROTTLE_CODES: ClassVar[frozenset[int]] = frozenset({4, 17, 32, 613, *range(80000, 80015)})
    THROTTLE_BUDGET: ClassVar[dt.timedelta] = dt.timedelta(hours=1)
    FIRST_BACKOFF: ClassVar[dt.timedelta] = dt.timedelta(minutes=1)
    LONGEST_WAIT: ClassVar[dt.timedelta] = dt.timedelta(minutes=10)
    TRANSIENT_ATTEMPTS: ClassVar[int] = 5
    TRANSIENT_BACKOFF: ClassVar[dt.timedelta] = dt.timedelta(seconds=5)

    throttled: dt.timedelta = field(default_factory=dt.timedelta)
    throttles: int = 0
    failures: int = 0

    def after_throttle(self, announced: dt.timedelta) -> dt.timedelta | None:
        """The wait before repeating a throttled request, or None once the budget is spent.

        Args:
            announced: How long Meta said to wait; zero when it said nothing,
                in which case the wait doubles from a minute on each throttle.

        Returns:
            The wait, at most ten minutes, or None when it would overrun the budget.
        """
        wait = min(announced or self.FIRST_BACKOFF * 2**self.throttles, self.LONGEST_WAIT)
        if self.throttled + wait > self.THROTTLE_BUDGET:
            return None
        self.throttles += 1
        self.throttled += wait
        return wait

    def after_failure(self) -> dt.timedelta | None:
        """The wait before repeating a transient failure, or None after the last attempt.

        Returns:
            The wait, doubling from five seconds, or None once the attempts are spent.
        """
        self.failures += 1
        if self.failures >= self.TRANSIENT_ATTEMPTS:
            return None
        return self.TRANSIENT_BACKOFF * 2 ** (self.failures - 1)

    @staticmethod
    def announced_lockout(headers: Any) -> dt.timedelta:
        """How long Meta says the caller is locked out, read from a response's or an error's headers.

        ``x-business-use-case-usage`` holds one entry per rate-limit type
        (ads management, insights, Pages, Instagram...) per business object,
        each with the minutes until access returns, zero while it is not
        locked out.

        Args:
            headers: The headers; names are case-insensitive.

        Returns:
            The longest announced lockout, zero when none is.
        """
        lockouts = [dt.timedelta()]
        if business := headers.get("x-business-use-case-usage"):
            for limits in json.loads(business).values():
                for limit in limits:
                    lockouts.append(dt.timedelta(minutes=float(limit.get("estimated_time_to_regain_access", 0))))
        return max(lockouts)


class GraphApi:
    """The Graph API over HTTP: its objects, its paged edges, its insights and its refusals.

    Facebook Insights and Instagram Insights both read it through this engine;
    Facebook Ads reads it through the Facebook SDK, under the same `Attempts`.

    Every request, each page included, goes through `_read`: a throttled
    refusal is waited out and repeated alone, any other refusal raises its
    HTTP status as ``httpx2.HTTPStatusError``, and `error_code` reads Graph's
    own code from its response. List parameters (``fields``, ``metric``,
    ``breakdown``) travel comma-separated; every method joins them, so a
    caller passes plain lists.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, pinned to a Graph version and carrying a user or
                Page access token as its bearer.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read one object.

        Args:
            path: The object, relative to the versioned base URL, such as ``/{page_id}``.
            **params: The query parameters; a list value is sent comma-separated.

        Returns:
            The object as Graph sends it.
        """
        return await self._read(path, self._query(params))

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of an edge, following ``paging.next``.

        The ``next`` link is absolute and carries the first request's
        parameters on. Graph may copy the access token into it, so it is
        followed without one: the client sends the token as its bearer, and a
        URL reaches the HTTP client's request log and a refusal's message. A
        response without a link is the last page, so an edge that does not
        page is read once.

        Args:
            path: The edge, relative to the versioned base URL, such as ``/{page_id}/published_posts``.
            **params: The first request's query parameters; a list value is sent comma-separated.

        Returns:
            The ``data`` rows of every page, in page order.
        """
        body = await self._read(path, self._query(params))
        rows = list(body["data"])
        while next_link := (body.get("paging") or {}).get("next"):
            body = await self._read(httpx2.URL(next_link).copy_remove_param("access_token"))
            rows.extend(body["data"])
        return rows

    async def report(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of an insights edge, as one response.

        An insights edge pages by time window, not by cursor: its
        ``paging.next`` asks the same query for the days after, so following
        it would read past *day*.

        Args:
            path: The insights edge, relative to the versioned base URL, such as ``/{page_id}/insights``.
            day: The report's only day, sent as ``since`` and, the day after, as ``until``.
            **params: What the report is: its ``metric``, ``period``, ``metric_type``, ``breakdown``; a list
                value is sent comma-separated.

        Returns:
            The insight objects, each one metric as Graph sends it (``{name, period, values | total_value, ...}``).
        """
        window = {"since": day.isoformat(), "until": (day + dt.timedelta(days=1)).isoformat()}
        return (await self.get(path, **params, **window))["data"]

    @staticmethod
    def error_code(response: httpx2.Response) -> int | None:
        """Read Graph's own code from a refused request's response.

        Args:
            response: The response, such as an ``httpx2.HTTPStatusError``'s.

        Returns:
            The ``error.code`` of its body, or None when the body is not JSON or carries no code.
        """
        try:
            return response.json().get("error", {}).get("code")
        except ValueError:
            return None

    async def _read(self, url: str | httpx2.URL, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Send one GET and return its body, waiting out Graph's throttling.

        Args:
            url: A path relative to the versioned base URL, or an absolute next link.
            params: The query parameters; None for a next link, which carries its own.

        Returns:
            The response's body.
        """
        attempts = Attempts()
        while True:
            response = await self.client.get(url, params=params)
            code = self.error_code(response) if response.is_error else None
            wait = None
            if code in Attempts.THROTTLE_CODES:
                wait = attempts.after_throttle(Attempts.announced_lockout(response.headers))
            if wait is None:
                response.raise_for_status()
                return response.json()
            logger.warning(f"Graph refused with code {code}; waiting {wait}")
            await sleep(wait.total_seconds())

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Join list parameters into comma-separated names, as Graph reads them.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            The parameters, each list value comma-joined and every other value unchanged.
        """
        return {key: ",".join(value) if isinstance(value, list) else value for key, value in params.items()}
