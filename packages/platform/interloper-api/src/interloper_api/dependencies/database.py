"""The database session a read request's store calls share.

A store call that opens its own session pays a connection checkout, a
``BEGIN`` and a ``ROLLBACK`` around its reads, and a request makes several:
its credential and organisation, then whatever the route reads. A ``GET`` or
``HEAD`` request shares one session among them instead (see
:meth:`~interloper_db.Store.shared_session`), from its first dependency until
its handler has returned, before the response is sent: a streamed body and
background tasks open their own, as they always did. Every other method keeps
a session per store call, and so does a websocket, which lives for minutes.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from starlette.concurrency import run_in_threadpool
from starlette.requests import HTTPConnection

from interloper_api.dependencies.state import StoreDep

READ_METHODS = frozenset({"GET", "HEAD"})


async def share_read_session(connection: HTTPConnection, store: StoreDep) -> AsyncGenerator[None, None]:
    """Share one database session among the store calls a read request makes.

    Async so the session is bound in the request's own context, which every
    sync dependency and handler then runs a copy of in its worker thread.

    Args:
        connection: The request or websocket being served.
        store: The store the request reads through.

    Yields:
        ``None``, while the request is handled.
    """
    if connection.scope["type"] != "http" or connection.scope["method"] not in READ_METHODS:
        yield
        return
    with store.shared_session() as session:
        try:
            yield
        finally:
            await run_in_threadpool(session.close)
