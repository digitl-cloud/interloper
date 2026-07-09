"""Concurrency utility helpers."""

from __future__ import annotations

import asyncio
import atexit
import inspect
import threading
from collections.abc import Coroutine, Iterable
from typing import Any, TypeVar

_T = TypeVar("_T")


# ------------------------------------------------------------------
# Sync bridge
# ------------------------------------------------------------------


# The background event loop backing ``run()``. Started lazily on first use and
# kept for the lifetime of the process so that loop-bound state created by one
# ``run()`` call — an ``AsyncRESTClient``'s connection pool, most notably —
# remains valid for the next one. A per-call ``asyncio.run`` would bind that
# state to a loop that no longer exists.
_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_loop_lock = threading.Lock()


def _shutdown_loop() -> None:
    """Stop the background loop at interpreter exit (best-effort)."""
    global _loop, _loop_thread
    if _loop is not None and _loop.is_running():
        _loop.call_soon_threadsafe(_loop.stop)
    if _loop_thread is not None and _loop_thread.is_alive():
        _loop_thread.join(timeout=2.0)
    _loop = None
    _loop_thread = None


def _ensure_loop() -> asyncio.AbstractEventLoop:
    """Return the background event loop, starting its daemon thread on first use."""
    global _loop, _loop_thread
    with _loop_lock:
        if _loop is None or not _loop.is_running():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=loop.run_forever, name="interloper-run", daemon=True)
            thread.start()
            _loop = loop
            _loop_thread = thread
            atexit.register(_shutdown_loop)
        return _loop


def run(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run a coroutine to completion from synchronous code.

    The sync bridge to the async-native engine. It backs the sync
    entrypoints — ``asset.run()``, ``asset.materialize()``,
    ``dag.materialize()`` — and can be called directly to drive any other
    framework coroutine (e.g. a configured runner) from a script, a REPL,
    or a notebook cell without touching ``asyncio``::

        import interloper as il

        result = il.run(il.AsyncRunner(max_workers=8).run(dag))

    Unlike ``asyncio.run``, this works where an event loop is already
    running (Jupyter) and reuses one persistent background loop across
    calls, so loop-bound clients cached on connections stay valid from one
    invocation to the next. Async code should ``await`` the coroutine
    directly instead.

    Ctrl-C cancels the coroutine before re-raising ``KeyboardInterrupt``.

    Args:
        coro: The coroutine to execute.

    Returns:
        The coroutine's result.

    Raises:
        RuntimeError: If called from code already running on the bridge's
            own loop (``await`` instead — blocking would deadlock).
        KeyboardInterrupt: Re-raised after cancelling the coroutine.
    """
    loop = _ensure_loop()
    if threading.current_thread() is _loop_thread:
        coro.close()
        raise RuntimeError(
            "il.run() called from code already running on its own event loop; use 'await' instead."
        )

    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result()
    except KeyboardInterrupt:
        future.cancel()
        raise


# ------------------------------------------------------------------
# Async helpers
# ------------------------------------------------------------------


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
