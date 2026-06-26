"""Serial runner — executes assets one at a time in dependency order."""

from __future__ import annotations

from interloper.runner.async_runner import AsyncRunner


class SerialRunner(AsyncRunner):
    """Execute assets one at a time in dependency order.

    The simplest runner — deterministic, easy to debug. It is the async
    engine with a single concurrency slot, so there is never more than one
    asset in flight::

        result = await SerialRunner(on_event=log_event).run(dag)
        # or, from a sync edge: asyncio.run(SerialRunner().run(dag))
    """

    max_workers: int = 1
