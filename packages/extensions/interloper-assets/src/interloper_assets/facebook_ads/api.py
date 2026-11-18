"""The Graph API client and the Marketing API engine every Facebook Ads asset reads through."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from time import monotonic, sleep
from typing import Any, ClassVar

import requests
from facebook_business.api import FacebookAdsApi, FacebookResponse
from facebook_business.exceptions import FacebookError, FacebookRequestError

logger = logging.getLogger(__name__)


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

    - **Throttled** (the rate-limit codes): wait as long as Meta says the
      caller is locked out, else back off, and repeat, within a total budget.
    - **Transient** (``is_transient``, code 2, a 5xx, a dropped connection):
      repeat a few times after a short backoff.
    - **Too much data** (code 1, "reduce the amount of data"): ask for less in
      the same request. A page halves its ``limit`` in the cursor's own
      parameters, so the following pages keep the size that fits; an object
      is read again in two halves of its fields and the answers merged. A
      page of one row still too heavy is raised: merging two pages keeps the
      second page's rows, so its halves cannot be merged the same way.
    - Anything else is raised at once.

    A response announcing a lockout also pauses the caller that long before
    its next request. The usage percentages Meta reports beside it are not
    acted on: they cover a rolling window, so pausing on them slows every
    request while Meta still serves them all.
    """

    THROTTLE_CODES: ClassVar[frozenset[int]] = frozenset({4, 17, 32, 613, *range(80000, 80015)})
    NETWORK_ERRORS: ClassVar[tuple[type[Exception], ...]] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.Timeout,
    )
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
                    fields = str(params.get("fields", "")).split(",")
                    if method == "GET" and not self._is_edge(path) and len(fields) > 1:
                        return self._read_halves(path, params, headers, url_override, api_version)
                    raise
                if error.api_error_code() in self.THROTTLE_CODES:
                    wait = attempts.after_throttle(self._announced_lockout(error.http_headers() or {}))
                elif self._is_transient(error):
                    wait = attempts.after_failure()
                else:
                    raise
                if wait is None:
                    raise
                self._wait(wait, f"refused with code {error.api_error_code()}")
            else:
                self._pace(self._announced_lockout(response.headers()))
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

    def _pace(self, lockout: timedelta) -> None:
        """Pause before the next request for as long as Meta says the caller is locked out.

        Args:
            lockout: The lockout the last response announced; zero for none.
        """
        if lockout:
            self._wait(min(lockout, Attempts.LONGEST_WAIT), "announced a lockout")

    @staticmethod
    def _announced_lockout(headers: Any) -> timedelta:
        """How long Meta says the caller is locked out, read from a response's or an error's headers.

        ``x-business-use-case-usage`` holds one entry per rate-limit type
        (ads management, insights...) per business object, each with the
        minutes until access returns, zero while it is not locked out.

        Args:
            headers: The headers; names are case-insensitive.

        Returns:
            The longest announced lockout, zero when none is.
        """
        lockouts = [timedelta()]
        if business := headers.get("x-business-use-case-usage"):
            for limits in json.loads(business).values():
                for limit in limits:
                    lockouts.append(timedelta(minutes=float(limit.get("estimated_time_to_regain_access", 0))))
        return max(lockouts)

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


class FacebookMarketingApi:
    """The Marketing API over the Graph client: its nodes, its paged edges and its insights jobs.

    Every request goes through the client's ``call``, which waits out
    throttling, repeats transient failures and asks for less when Facebook
    refuses a request for its size; any refusal it does not absorb is raised
    as the SDK's ``FacebookRequestError``. A path names a node and, below it,
    an edge: ``act_1`` is an ad account, ``act_1/ads`` its ads.
    """

    REPORT_TIMEOUT: ClassVar[timedelta] = timedelta(hours=1)
    REPORT_POLL: ClassVar[timedelta] = timedelta(seconds=10)

    def __init__(self, client: FacebookGraphApi) -> None:
        """Build the engine on the Graph client.

        Args:
            client: The client every request goes through, carrying the connection's credentials.
        """
        self.client = client

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read one node.

        Args:
            path: The node, such as ``act_1``.
            **params: The query parameters; ``fields`` is a list.

        Returns:
            The node as Graph sends it.
        """
        return self.client.call("GET", self._path(path), self._query(params)).json()

    def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of an edge, page after page.

        Pages follow the ``after`` cursor while Graph announces a next page
        and the last one held rows. Every page is asked for with the same
        mapping, in which the client halves ``limit`` when a page is refused
        for size, so the pages after it keep the size that fits.

        Args:
            path: The edge, such as ``act_1/ads``.
            **params: The query parameters; ``fields`` is a list.

        Returns:
            The rows of every page, in page order, as Graph sends them.
        """
        query = self._query(params)
        rows: list[dict[str, Any]] = []
        while True:
            body = self.client.call("GET", self._path(path), query).json()
            rows.extend(body["data"])
            paging = body.get("paging", {})
            after = paging.get("cursors", {}).get("after")
            if not body["data"] or "next" not in paging or after is None:
                return rows
            query["after"] = after

    def report(self, path: str, day: date, **params: Any) -> list[dict[str, Any]]:
        """Run one day of an insights report as a job, wait for it and read its rows.

        Args:
            path: The insights edge of the node the report covers, such as ``act_1/insights``.
            day: The report's only day.
            **params: What the report is: its ``fields``, ``level``, ``breakdowns``
                and the like; a list or dict value is sent as JSON.

        Returns:
            The report's rows, as Graph sends them.
        """
        window = {"time_range": {"since": day.isoformat(), "until": day.isoformat()}}
        job = self.client.call("POST", self._path(path), {**params, **window}).json()
        self._poll(job["report_run_id"])
        return self.rows(f"{job['report_run_id']}/insights")

    def _poll(self, job_id: str) -> None:
        """Wait for a report job to complete.

        A job often sits at "Job Running 99%" for minutes before completing,
        so it is polled for up to ``REPORT_TIMEOUT``.

        Args:
            job_id: The job's ``report_run_id``.

        Raises:
            FacebookError: If the job failed.
            TimeoutError: If the job has not completed within ``REPORT_TIMEOUT``.
        """
        deadline = monotonic() + self.REPORT_TIMEOUT.total_seconds()
        while True:
            job = self.get(job_id)
            status, completion = job["async_status"], job["async_percent_completion"]
            logger.info(f"Waiting for report... ({status} {completion}%)")
            if status == "Job Failed":
                raise FacebookError("Facebook insights report failed")
            if status == "Job Completed":
                return
            if monotonic() >= deadline:
                raise TimeoutError(
                    f"Facebook insights report did not complete within {self.REPORT_TIMEOUT} "
                    f"(last status: {status} {completion}%)"
                )
            sleep(self.REPORT_POLL.total_seconds())

    @staticmethod
    def _path(path: str) -> tuple[str, str]:
        """Split a Graph path into the node and the edge the client addresses.

        Args:
            path: A node (``act_1``) or an edge below one (``act_1/ads``).

        Returns:
            The node and the edge, the edge empty when the path is the node itself.
        """
        node, _, edge = path.partition("/")
        return node, edge

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Join a ``fields`` list with commas, the form the client splits to read an object in halves.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            A new mapping of the parameters, a ``fields`` list comma-separated and every other value unchanged.
        """
        return {
            key: ",".join(value) if key == "fields" and isinstance(value, list) else value
            for key, value in params.items()
        }
