"""Concurrency utility helpers."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any


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
