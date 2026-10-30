"""Tests for ``interloper.runner.spec``."""

from __future__ import annotations

from typing import Any

import httpx2
import pytest

from interloper.dag import DAGSpec
from interloper.errors import RunnerError
from interloper.partitioning import PartitionWindow, TimePartition
from interloper.runner.spec import RunSpec


def _client(handler: Any) -> httpx2.Client:
    """A client of the API over a scripted transport, as a run's process makes one.

    Args:
        handler: Answers every request.

    Returns:
        The client.
    """
    return httpx2.Client(
        base_url="http://api.internal",
        headers={"Authorization": "Bearer ilr_secret"},
        transport=httpx2.MockTransport(handler),
    )


_SPEC = RunSpec(dag=DAGSpec(), partition_key="2026-10-25", metadata={"run_id": "run-1", "org_id": "org-1"})


class TestWindow:
    def test_the_partition_key_is_read_as_a_time_partition(self) -> None:
        assert _SPEC.window == PartitionWindow.of(TimePartition.from_key("2026-10-25"))

    def test_an_unscoped_run_has_no_window(self) -> None:
        assert RunSpec(dag=DAGSpec()).window is None


class TestFromApi:
    """The run's spec, loaded as its token, retried only while retrying can help."""

    def test_loads_the_run_from_its_route_with_the_bearer(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen["url"] = str(request.url)
            seen["auth"] = request.headers["authorization"]
            return httpx2.Response(200, json=_SPEC.model_dump(mode="json"))

        assert RunSpec.from_api(_client(handler), "run-1", timeout=1) == _SPEC
        assert seen == {"url": "http://api.internal/api/runs/run-1/spec", "auth": "Bearer ilr_secret"}

    @pytest.mark.parametrize("status", [401, 409, 422])
    def test_a_refused_run_is_final(self, status: int) -> None:
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(status)
            return httpx2.Response(status, json={"detail": "no"})

        with pytest.raises(RunnerError, match=f"HTTP {status}"):
            RunSpec.from_api(_client(handler), "run-1", timeout=5, backoff=0.01)
        assert calls == [status]

    def test_an_api_that_cannot_answer_yet_is_retried(self) -> None:
        answers = iter([httpx2.Response(503), httpx2.Response(404)])

        def handler(request: httpx2.Request) -> httpx2.Response:
            if (answer := next(answers, None)) is None:
                return httpx2.Response(200, json=_SPEC.model_dump(mode="json"))
            return answer

        assert RunSpec.from_api(_client(handler), "run-1", timeout=5, backoff=0.01) == _SPEC

    def test_an_unreachable_api_gives_up_after_the_timeout(self) -> None:
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("refused")

        with pytest.raises(RunnerError, match="ConnectError"):
            RunSpec.from_api(_client(handler), "run-1", timeout=0.05, backoff=0.01)
