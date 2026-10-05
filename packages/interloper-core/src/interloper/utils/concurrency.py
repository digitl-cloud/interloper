"""Concurrency utility helpers."""

from __future__ import annotations

import asyncio
import atexit
import inspect
import threading
from collections.abc import Coroutine, Iterable
from types import TracebackType
from typing import Any, TypeVar

from opentelemetry import context as otel_context
from typing_extensions import Self

_T = TypeVar("_T")


# -- Thread primitives ---------------------------------------------------------


class RLock:
    """A re-entrant lock that copies and pickles as a fresh, unheld lock.

    A drop-in for ``threading.RLock`` on models that get deep-copied, as a
    destination does with the source it is bound to: a bare lock cannot be
    copied. A copy never shares the original's lock, so it starts unheld.
    """

    def __init__(self) -> None:
        """Create the underlying lock."""
        self._lock = threading.RLock()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock, re-entrantly for the thread already holding it.

        Args:
            blocking: Whether to wait for the lock when another thread holds it.
            timeout: Seconds to wait at most when blocking; ``-1`` waits forever.

        Returns:
            Whether the lock was acquired.
        """
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        """Release one level of the lock held by the calling thread."""
        self._lock.release()

    def __enter__(self) -> Self:
        """Acquire the lock, waiting for it if needed.

        Returns:
            The lock.
        """
        self._lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the lock.

        Args:
            exc_type: The exception type, if the block raised; unused.
            exc: The exception, if the block raised; unused.
            tb: The traceback, if the block raised; unused.
        """
        self._lock.release()

    def __reduce__(self) -> tuple[type[RLock], tuple[()]]:
        """Reduce to a fresh lock, which ``copy`` and ``pickle`` both rebuild.

        Returns:
            The class and its empty constructor arguments.
        """
        return type(self), ()


class ThreadLocal(threading.local):
    """Per-thread attributes that copy and pickle as a fresh, empty namespace.

    A drop-in for ``threading.local`` on models that get deep-copied: a bare
    one cannot be copied, and a copy has no claim on the original's threads.
    """

    def __reduce__(self) -> tuple[type[ThreadLocal], tuple[()]]:
        """Reduce to a fresh namespace, which ``copy`` and ``pickle`` both rebuild.

        Returns:
            The class and its empty constructor arguments.
        """
        return type(self), ()


# -- Sync bridge ---------------------------------------------------------------


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
    or a notebook cell without touching ``asyncio``:

    ```py
    import interloper as il

    result = il.run(il.AsyncRunner(max_workers=8).run(dag))
    ```

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
        raise RuntimeError("il.run() called from code already running on its own event loop; use 'await' instead.")

    # run_coroutine_threadsafe binds the task to the loop thread's context,
    # not the caller's — carry it across or spans go parentless.
    caller_context = otel_context.get_current()

    async def _bridged() -> _T:
        token = otel_context.attach(caller_context)
        try:
            return await coro
        finally:
            otel_context.detach(token)

    future = asyncio.run_coroutine_threadsafe(_bridged(), loop)
    try:
        return future.result()
    except KeyboardInterrupt:
        future.cancel()
        raise


# -- Async helpers -------------------------------------------------------------


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
