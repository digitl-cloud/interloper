"""The Graph API client every Facebook Ads request goes through."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from time import sleep
from typing import Any, ClassVar

import requests
from facebook_business.api import FacebookAdsApi, FacebookResponse
from facebook_business.exceptions import FacebookRequestError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Usage:
    """How much of its rate limits a Graph API caller has used, as Meta reports it on every response.

    Attributes:
        percent: The highest utilization among the reported limits, 0 to 100.
        wait: How long Meta says the caller must wait before its access returns; zero when it says nothing.
    """

    percent: float
    wait: timedelta

    @classmethod
    def from_headers(cls, headers: Any) -> Usage:
        """Read the usage headers Meta attaches to responses and errors alike.

        ``x-app-usage`` is the app's share, ``x-ad-account-usage`` the ad
        account's, ``x-business-use-case-usage`` one entry per rate-limit
        type (ads management, insights...) per business object, each with the
        minutes until access returns, and ``x-fb-ads-insights-throttle`` the
        async insights share. The worst of them is the caller's usage. Each
        is read by its measures alone: Meta adds labels beside them, such as
        the access tier (``ads_api_access_tier``).

        Args:
            headers: A response's or an error's headers; header names are case-insensitive.

        Returns:
            The worst utilization and the longest announced wait.
        """
        percents = [0.0]
        waits = [timedelta()]
        if app := headers.get("x-app-usage"):
            app = json.loads(app)
            percents.extend(float(app.get(key, 0)) for key in ("call_count", "total_cputime", "total_time"))
        if account := headers.get("x-ad-account-usage"):
            account = json.loads(account)
            percents.append(float(account.get("acc_id_util_pct", 0)))
            waits.append(timedelta(seconds=float(account.get("reset_time_duration", 0))))
        if business := headers.get("x-business-use-case-usage"):
            for limits in json.loads(business).values():
                for limit in limits:
                    percents.extend(float(limit.get(key, 0)) for key in ("call_count", "total_cputime", "total_time"))
                    waits.append(timedelta(minutes=float(limit.get("estimated_time_to_regain_access", 0))))
        if insights := headers.get("x-fb-ads-insights-throttle"):
            insights = json.loads(insights)
            percents.extend(float(insights.get(key, 0)) for key in ("app_id_util_pct", "acc_id_util_pct"))
        return cls(percent=max(percents), wait=max(waits))


@dataclass
class Attempts:
    """What one request has spent waiting out throttling and repeating transient failures.

    Throttling is waited out within a total budget, since Meta can lock an
    account for up to an hour; a transient failure is repeated a few times
    only, since one that persists is not transient.
    """

    THROTTLE_BUDGET: ClassVar[timedelta] = timedelta(hours=1)
    FIRST_BACKOFF: ClassVar[timedelta] = timedelta(minutes=1)
    LONGEST_WAIT: ClassVar[timedelta] = timedelta(minutes=10)
    TRANSIENT_ATTEMPTS: ClassVar[int] = 5
    TRANSIENT_BACKOFF: ClassVar[timedelta] = timedelta(seconds=5)

    throttled: timedelta = field(default_factory=timedelta)
    throttles: int = 0
    failures: int = 0

    def after_throttle(self, announced: timedelta) -> timedelta | None:
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

    def after_failure(self) -> timedelta | None:
        """The wait before repeating a transient failure, or None after the last attempt.

        Returns:
            The wait, doubling from five seconds, or None once the attempts are spent.
        """
        self.failures += 1
        if self.failures >= self.TRANSIENT_ATTEMPTS:
            return None
        return self.TRANSIENT_BACKOFF * 2 ** (self.failures - 1)


class FacebookGraphApi(FacebookAdsApi):
    """The Facebook SDK's API with one policy for every request it sends.

    Every SDK request, an object read, each page of an edge, an async
    insights job and its polling, goes through ``call``, so this is the one
    place that decides what a refusal means:

    - **Throttled** (the rate-limit codes): wait as long as Meta's usage
      headers say, else back off, and repeat, within a total budget.
    - **Transient** (``is_transient``, code 2, a 5xx, a dropped connection):
      repeat a few times after a short backoff.
    - **Too much data** (code 1, "reduce the amount of data"): ask for less in
      the same request. A page halves its ``limit`` in the cursor's own
      parameters, so the following pages keep the size that fits; an object
      is read again in two halves of its fields and the answers merged.
    - Anything else is raised at once.

    Every response is also read for usage, and a caller close to its limits
    pauses before its next request rather than being refused.
    """

    THROTTLE_CODES: ClassVar[frozenset[int]] = frozenset({4, 17, 32, 613, *range(80000, 80015)})
    NETWORK_ERRORS: ClassVar[tuple[type[Exception], ...]] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.Timeout,
    )
    PACE_ABOVE: ClassVar[float] = 90
    DEFAULT_PAGE: ClassVar[int] = 25

    def call(
        self,
        method: str,
        path: Any,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        files: dict[str, Any] | None = None,
        url_override: str | None = None,
        api_version: str | None = None,
    ) -> FacebookResponse:
        """Send a request, waiting out throttling, repeating transient failures and asking for less.

        Args:
            method: The HTTP method, such as ``GET``.
            path: The Graph path as a tuple of tokens (``("act_1", "ads")``), or a full URL.
            params: The request's parameters. A page refused for size has its
                ``limit`` halved in this very mapping, which an SDK cursor
                reuses for its next pages.
            headers: Extra request headers.
            files: Files to upload.
            url_override: A Graph host replacing the session's.
            api_version: An API version replacing the client's.

        Returns:
            The response, merged from field halves when an object was read in parts.

        Raises:
            FacebookRequestError: When the refusal is of no kind above, or
                still stands once its budget is spent or nothing is left to halve.
        """
        params = {} if params is None else params
        attempts = Attempts()
        while True:
            try:
                response = super().call(method, path, params, headers, files, url_override, api_version)
            except self.NETWORK_ERRORS as error:
                wait = attempts.after_failure()
                if wait is None:
                    raise
                self._wait(wait, type(error).__name__)
            except FacebookRequestError as error:
                if self._asks_for_less(error):
                    if self._shrinks_page(method, path, params):
                        continue
                    if method == "GET" and len(str(params.get("fields", "")).split(",")) > 1:
                        return self._read_halves(path, params, headers, url_override, api_version)
                    raise
                if error.api_error_code() in self.THROTTLE_CODES:
                    wait = attempts.after_throttle(Usage.from_headers(error.http_headers() or {}).wait)
                elif self._is_transient(error):
                    wait = attempts.after_failure()
                else:
                    raise
                if wait is None:
                    raise
                self._wait(wait, f"refused with code {error.api_error_code()}")
            else:
                self._pace(Usage.from_headers(response.headers()))
                return response

    def _shrinks_page(self, method: str, path: Any, params: dict[str, Any]) -> bool:
        """Halve the page of an edge read refused for size, in the cursor's own parameters.

        A request without a ``limit`` reads Meta's default page of 25.

        Args:
            method: The refused request's HTTP method.
            path: The refused request's Graph path.
            params: The refused request's parameters, updated in place.

        Returns:
            True when the request reads pages of an edge and its page could be halved.
        """
        limit = int(params.get("limit", self.DEFAULT_PAGE))
        if method != "GET" or not self._is_edge(path) or limit == 1:
            return False
        params["limit"] = limit // 2
        logger.warning(f"Facebook asked for less data; reading pages of {params['limit']}")
        return True

    def _read_halves(
        self,
        path: Any,
        params: dict[str, Any],
        headers: dict[str, str] | None,
        url_override: str | None,
        api_version: str | None,
    ) -> FacebookResponse:
        """Read an object's fields in two halves and merge the answers into one response.

        Each half goes through ``call`` again, so it is split further when it
        is still too heavy, and fails when one field alone is.

        Args:
            path: The object's Graph path.
            params: The refused request's parameters, with more than one field; left untouched.
            headers: Extra request headers.
            url_override: A Graph host replacing the session's.
            api_version: An API version replacing the client's.

        Returns:
            One response carrying every field either half returned.
        """
        fields = str(params["fields"]).split(",")
        middle = len(fields) // 2
        logger.warning(f"Facebook asked for less data; reading {len(fields)} fields in two halves")
        halves = [
            self.call("GET", path, {**params, "fields": ",".join(part)}, headers, None, url_override, api_version)
            for part in (fields[:middle], fields[middle:])
        ]
        body = {**halves[0].json(), **halves[1].json()}
        return FacebookResponse(body=json.dumps(body), http_status=200, headers=halves[1].headers(), call=None)

    def _pace(self, usage: Usage) -> None:
        """Pause before the next request when Meta reports the caller close to its limits.

        Args:
            usage: The usage the last response reported.
        """
        if usage.percent >= self.PACE_ABOVE:
            wait = min(max(usage.wait, Attempts.FIRST_BACKOFF), Attempts.LONGEST_WAIT)
            self._wait(wait, f"usage at {usage.percent:.0f}%")

    @staticmethod
    def _wait(duration: timedelta, reason: str) -> None:
        """Sleep, saying why.

        Args:
            duration: How long to sleep.
            reason: What the wait is for, for the log.
        """
        logger.warning(f"Facebook {reason}; waiting {duration}")
        sleep(duration.total_seconds())

    @staticmethod
    def _asks_for_less(error: FacebookRequestError) -> bool:
        """Whether Facebook refused the request for the amount of data it asked for.

        Args:
            error: The refusal.

        Returns:
            True for code 1 "Please reduce the amount of data you're asking for".
        """
        return error.api_error_code() == 1 and "reduce the amount of data" in (error.api_error_message() or "")

    @staticmethod
    def _is_transient(error: FacebookRequestError) -> bool:
        """Whether a refusal is worth repeating as it is.

        Args:
            error: The refusal.

        Returns:
            True when Meta marks it transient, for its temporary-error code 2, or for any 5xx.
        """
        return bool(error.api_transient_error()) or error.api_error_code() == 2 or (error.http_status() or 0) >= 500

    @staticmethod
    def _is_edge(path: Any) -> bool:
        """Whether a Graph path reads an edge (a list, paged) rather than one object.

        ``act_1/ads`` is an edge of ``act_1``; ``act_1`` alone (the SDK sends
        ``("act_1", "")``) is the object itself. A full URL is read the same
        way after its host and version.

        Args:
            path: The Graph path as a tuple of tokens, or a full URL.

        Returns:
            True when the path names an edge below a node.
        """
        tokens = path.split("?")[0].split("/")[4:] if isinstance(path, str) else [str(token) for token in path]
        return len([token for token in tokens if token]) > 1
