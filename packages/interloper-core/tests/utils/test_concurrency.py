"""Tests for ``interloper.utils.concurrency``."""

from __future__ import annotations

import threading

from interloper.utils.concurrency import invoke


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
