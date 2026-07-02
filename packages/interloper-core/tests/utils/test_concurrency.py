"""Tests for ``interloper.utils.concurrency``."""

from __future__ import annotations

import asyncio
import threading

import pytest

from interloper.utils.concurrency import bounded_gather, invoke, run


class TestBoundedGather:
    """Order-preserving concurrent await with a concurrency cap."""

    async def test_preserves_input_order(self):
        async def val(x: int) -> int:
            await asyncio.sleep(0.01 * (5 - x))  # later inputs finish sooner
            return x

        assert await bounded_gather((val(i) for i in range(5)), limit=5) == [0, 1, 2, 3, 4]

    async def test_caps_concurrency_at_limit(self):
        inflight = 0
        peak = 0

        async def task() -> None:
            nonlocal inflight, peak
            inflight += 1
            peak = max(peak, inflight)
            await asyncio.sleep(0.01)
            inflight -= 1

        await bounded_gather((task() for _ in range(10)), limit=3)
        assert peak <= 3

    async def test_propagates_exceptions(self):
        async def ok() -> int:
            return 1

        async def boom() -> int:
            raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            await bounded_gather([ok(), boom(), ok()], limit=2)

    async def test_rejects_non_positive_limit(self):
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await bounded_gather([], limit=0)


class TestInvoke:
    """Uniform sync/async dispatch for an arbitrary callable."""

    async def test_async_callable_is_awaited_natively(self):
        ran_in: dict[str, int] = {}

        async def fn(x: int) -> int:
            ran_in["thread"] = threading.get_ident()
            return x + 1

        result = await invoke(fn, 1)
        assert result == 2
        # Awaited on the calling (event-loop) thread, not offloaded.
        assert ran_in["thread"] == threading.get_ident()

    async def test_sync_callable_is_offloaded_to_a_worker_thread(self):
        ran_in: dict[str, int] = {}

        def fn(x: int) -> int:
            ran_in["thread"] = threading.get_ident()
            return x + 1

        result = await invoke(fn, 1)
        assert result == 2
        # Offloaded via asyncio.to_thread → runs off the event-loop thread.
        assert ran_in["thread"] != threading.get_ident()

    async def test_forwards_args_and_kwargs(self):
        def fn(a: int, b: int, *, c: int) -> tuple[int, int, int]:
            return (a, b, c)

        assert await invoke(fn, 1, 2, c=3) == (1, 2, 3)


class TestRun:
    """Sync bridge to the async engine (``il.run``)."""

    def test_returns_result_from_sync_context(self):
        async def add(a: int, b: int) -> int:
            return a + b

        assert run(add(1, 2)) == 3

    def test_propagates_exceptions(self):
        async def boom() -> None:
            raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            run(boom())

    def test_reuses_one_persistent_loop_across_calls(self):
        # Loop-bound state (e.g. an AsyncRESTClient's pool) created by one
        # call must remain valid for the next — the loop is process-lived.
        async def current_loop() -> asyncio.AbstractEventLoop:
            return asyncio.get_running_loop()

        assert run(current_loop()) is run(current_loop())

    def test_runs_off_the_calling_thread(self):
        async def loop_thread() -> int:
            return threading.get_ident()

        assert run(loop_thread()) != threading.get_ident()

    async def test_works_while_a_loop_is_running_in_the_calling_thread(self):
        # The Jupyter scenario: the caller's thread already runs a loop
        # (here: pytest-asyncio's), where asyncio.run would raise.
        async def add(a: int, b: int) -> int:
            return a + b

        assert run(add(1, 2)) == 3

    def test_calling_run_on_its_own_loop_raises(self):
        async def nested() -> None:
            async def noop() -> None:
                pass

            run(noop())  # sync call from the bridge's own loop thread

        with pytest.raises(RuntimeError, match="use 'await'"):
            run(nested())
