"""The policy a REST client sends a failed request again under."""

from __future__ import annotations

import httpx2
from pydantic import Field

from interloper.retry import RetryPolicy


class HttpRetryPolicy(RetryPolicy):
    """When a REST client sends a failed request again, and how long it waits first.

    A client repeats requests only under the policy it is built with; without
    one it sends each request once. The numbers are a `RetryPolicy`'s, at
    request scale. Which failures are worth another attempt follows HTTP: a
    throttled request was refused before it did anything, so it is repeated
    whatever its method, after the server's ``Retry-After`` when it gives one in
    seconds. A server error or a dropped connection may have left the request
    half done, so only a request whose method is idempotent is repeated. A
    platform that signals a passing failure some other way overrides
    `retryable`.
    """

    max_attempts: int = Field(
        default=5,
        ge=1,
        title="Max attempts",
        description="Total attempts, including the first",
    )
    delay: float = Field(
        default=2.0,
        ge=0,
        title="Delay",
        description="Seconds to wait before the second attempt",
    )
    max_delay: float = Field(
        default=60.0,
        ge=0,
        title="Max delay",
        description="Upper bound on any single delay, in seconds, the server's Retry-After included",
    )
    throttle_statuses: frozenset[int] = Field(
        default=frozenset({429}),
        title="Throttle statuses",
        description="Statuses refusing a request before it does anything, repeated whatever the method",
    )
    error_statuses: frozenset[int] = Field(
        default=frozenset({500, 502, 503, 504}),
        title="Error statuses",
        description="Server errors, repeated only for an idempotent method",
    )
    idempotent_methods: frozenset[str] = Field(
        default=frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}),
        title="Idempotent methods",
        description="Methods safe to send again after a server error or a dropped connection",
    )

    def retryable(self, request: httpx2.Request, response: httpx2.Response | None) -> bool:
        """Whether a failed request is worth sending again.

        Args:
            request: The request sent.
            response: The server's answer, or None when the connection failed.

        Returns:
            True for a throttle, or for a server error or dropped connection on an idempotent method.
        """
        if response is not None and response.status_code in self.throttle_statuses:
            return True
        failed = response is None or response.status_code in self.error_statuses
        return failed and request.method in self.idempotent_methods

    def wait(self, request: httpx2.Request, attempt: int, response: httpx2.Response | None) -> float | None:
        """The seconds to wait before sending a request again, or None when it is not sent again.

        Args:
            request: The request sent.
            attempt: The attempt about to be made, the first repeat being 2.
            response: The server's answer, or None when the connection failed.

        Returns:
            The server's ``Retry-After`` for a throttle that gives one, capped at
            ``max_delay``, else the policy's delay; None for a failure not worth
            repeating, or once the budget is spent.
        """
        if not self.allows(attempt) or not self.retryable(request, response):
            return None
        if response is not None and response.status_code in self.throttle_statuses:
            announced = response.headers.get("Retry-After", "")
            if announced.isdigit():
                return min(float(announced), self.max_delay)
        return self.delay_before(attempt)
