"""Retry: an attempt budget for a unit of work."""

from __future__ import annotations

import random

from pydantic import BaseModel, Field


class RetryPolicy(BaseModel):
    """How many times a unit of work is attempted, and how long between attempts.

    Declared on the component whose unit it governs: an operation's policy
    retries that operation's execution, a source's is the default for its
    assets, a job's retries that job's runs. A component declaring none is
    attempted once; there is no instance-wide default.

    The policy carries numbers only. Whether a given error is worth another
    attempt is behaviour rather than configuration, and lives on
    :meth:`~interloper.operation.base.Operation.retryable`.
    """

    max_attempts: int = Field(
        default=3,
        ge=1,
        title="Max attempts",
        description="Total attempts, including the first",
    )
    delay: float = Field(
        default=5.0,
        ge=0,
        title="Delay",
        description="Seconds to wait before the second attempt",
    )
    backoff: float = Field(
        default=2.0,
        ge=1,
        title="Backoff",
        description="Multiplier applied to the delay for each further attempt",
    )
    max_delay: float = Field(
        default=3600.0,
        ge=0,
        title="Max delay",
        description="Upper bound on any single delay, in seconds",
    )
    jitter: float = Field(
        default=0.1,
        ge=0,
        le=1,
        title="Jitter",
        description="Fraction of the delay spread randomly around it, so attempts do not align",
    )

    def allows(self, attempt: int) -> bool:
        """Whether the budget covers an attempt.

        Args:
            attempt: The attempt number, counting the first execution as 1.

        Returns:
            ``True`` while the attempt is within the budget.
        """
        return attempt <= self.max_attempts

    def delay_before(self, attempt: int) -> float:
        """How long to wait before an attempt.

        The growth is geometric from the second attempt (the first retry),
        capped at ``max_delay``, then spread by ``jitter`` so that operations
        failing together do not retry in lockstep.

        Args:
            attempt: The attempt number, counting the first execution as 1.

        Returns:
            The delay in seconds, never negative; ``0.0`` for the first attempt.
        """
        if attempt <= 1:
            return 0.0
        delay = min(self.delay * self.backoff ** (attempt - 2), self.max_delay)
        if not self.jitter:
            return delay
        spread = delay * self.jitter
        return max(0.0, random.uniform(delay - spread, delay + spread))
