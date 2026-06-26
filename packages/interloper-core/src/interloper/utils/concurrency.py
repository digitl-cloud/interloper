"""Concurrency utility helpers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Coroutine, Iterable
from typing import Any, TypeVar

_T = TypeVar("_T")


async def bounded_gather(coros: Iterable[Coroutine[Any, Any, _T]], *, limit: int) -> list[_T]:
    """Await coroutines concurrently, capped at ``limit`` in flight at once.

    Results are returned in the order the coroutines were given (like
    ``asyncio.gather``), but at most ``limit`` run concurrently — the bound is
    what keeps fan-out (paginated pages, per-entity requests) from stampeding an
    API into rate limits. If any coroutine raises, the exception propagates and
    the rest are cancelled.

    Args:
        coros: The coroutines to run.
        limit: Maximum number of coroutines in flight at once (must be >= 1).

    Returns:
        The results, ordered to match ``coros``.

    Raises:
        ValueError: If ``limit`` is less than 1.
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")

    semaphore = asyncio.Semaphore(limit)

    async def _guarded(coro: Coroutine[Any, Any, _T]) -> _T:
        async with semaphore:
            return await coro

    return await asyncio.gather(*(_guarded(c) for c in coros))


async def invoke(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a sync or async callable uniformly on the event loop.

    Async callables are awaited natively; sync ones are offloaded to a
    worker thread via ``asyncio.to_thread`` so they never block the loop.
    This is how the framework lets ``data()``, destination ``read``/``write``
    and fetch-field providers be written as either sync or ``async`` while the
    engine stays async-native.

    Args:
        fn: The callable to invoke.
        *args: Positional arguments forwarded to ``fn``.
        **kwargs: Keyword arguments forwarded to ``fn``.

    Returns:
        The callable's result.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)
