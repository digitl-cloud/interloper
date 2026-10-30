"""RunSpec: what a platform run executes, as the API hands it to the process that executes it."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx2
from pydantic import BaseModel, Field

from interloper.dag.base import DAGSpec
from interloper.errors import RunnerError
from interloper.partitioning import PartitionWindow, TimePartition

logger = logging.getLogger(__name__)

# A refused token, a run no longer live, or a run that cannot be built: loading again cannot help.
_FINAL_STATUSES = frozenset({401, 409, 422})


class RunSpec(BaseModel):
    """A platform run's DAG, its connections resolved, the partition it covers and the metadata its events carry.

    The API's ``GET /api/runs/{run_id}/spec`` returns it and
    ``interloper launch`` loads it, so both sides read one type.
    """

    dag: DAGSpec
    partition_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def window(self) -> PartitionWindow | None:
        """The window the run covers: its partition key read as a time partition, ``None`` when unscoped."""
        return PartitionWindow.of(TimePartition.from_key(self.partition_key)) if self.partition_key else None

    @classmethod
    def from_api(cls, client: httpx2.Client, run_id: str, *, timeout: float, backoff: float = 1.0) -> RunSpec:
        """Load a run from the API, as its token, retrying while the API cannot answer.

        A refused token, a run that is no longer live or one whose target can
        no longer be built is final. Anything else (the API unreachable,
        rolling or overloaded) is retried until *timeout*, the same bound the
        run's event delivery follows.

        Args:
            client: A client of the API, authenticated with the run's token.
            run_id: The run to load.
            timeout: Seconds to keep retrying before giving up.
            backoff: Seconds before the first retry; doubles up to ten times that.

        Returns:
            The run's spec.

        Raises:
            RunnerError: If the API refused the run, or could not be reached
                within *timeout*.
        """
        deadline = time.monotonic() + timeout
        delay = backoff
        while True:
            try:
                response = client.get(f"/api/runs/{run_id}/spec")
            except httpx2.HTTPError as error:
                failure = f"{type(error).__name__}: {error}"
            else:
                if response.is_success:
                    return cls.model_validate(response.json())
                failure = f"HTTP {response.status_code}: {response.text}"
                if response.status_code in _FINAL_STATUSES:
                    raise RunnerError(f"Loading run {run_id} failed: {failure}")
            if time.monotonic() + delay > deadline:
                raise RunnerError(f"Loading run {run_id} failed: {failure}")
            logger.warning("Loading run %s failed; retrying: %s", run_id, failure)
            time.sleep(delay)
            delay = min(delay * 2, backoff * 10)
