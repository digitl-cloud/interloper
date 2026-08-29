"""How the store scopes its work to a session, and who owns the commit."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from sqlalchemy import Engine
from sqlmodel import Session

#: The session an enclosing ``Store.transaction`` owns, if any. Store methods
#: join it instead of opening their own, so a caller can make several calls
#: atomic. Context-local, so concurrent requests never share one.
_open_transaction: ContextVar[Session | None] = ContextVar("interloper_db_transaction", default=None)

#: The session currently in scope, and how many nested scopes are sharing it.
#: One store method calling another must join rather than open a second
#: session, and only the outermost scope may commit.
_open_session: ContextVar[Session | None] = ContextVar("interloper_db_session", default=None)
_scope_depth: ContextVar[int] = ContextVar("interloper_db_scope_depth", default=0)


def commit(session: Session) -> None:
    """End a store method's unit of work.

    Commits when the method owns the session, and flushes when something
    outside it does — an enclosing ``Store.transaction``, or an outer store
    method that this one was called from. The writes stay visible to the rest
    of that scope without being made durable early.

    Args:
        session: The session the calling method is working in.
    """
    if _open_transaction.get() is not None or _scope_depth.get() > 1:
        session.flush()
    else:
        session.commit()


@contextmanager
def transaction(engine: Engine) -> Iterator[Session]:
    """Run several store calls as one atomic unit of work.

    Store methods normally own a session each, which makes them individually
    atomic but never atomic *together*. Inside this block they all join one
    session and one commit::

        with store.transaction():
            source = store.components.create(...)
            store.relations.add(source.id, type="destination", dst_id=...)

    Nesting is a no-op: an inner block joins the outer one, so the outermost
    block alone decides commit or rollback.

    Args:
        engine: The engine to open the transaction on.

    Yields:
        The session backing the transaction.
    """
    joined = _open_transaction.get()
    if joined is not None:
        yield joined
        return

    session = Session(engine, expire_on_commit=False)
    transaction_token = _open_transaction.set(session)
    open_token = _open_session.set(session)
    depth = _scope_depth.set(1)
    try:
        with session:
            yield session
            session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        _scope_depth.reset(depth)
        _open_session.reset(open_token)
        _open_transaction.reset(transaction_token)


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    """Work in a session on *engine*.

    Joins the session already in scope — an enclosing :func:`transaction`, or
    an outer store method that called this one — so nested store calls share
    one session. Opens its own only when nothing is in scope.

    Args:
        engine: The engine to open a session on when not already in one.

    Yields:
        The session to work in.
    """
    joined = _open_session.get()
    if joined is not None:
        depth = _scope_depth.set(_scope_depth.get() + 1)
        try:
            yield joined
        finally:
            _scope_depth.reset(depth)
        return

    session = Session(engine, expire_on_commit=False)
    open_token = _open_session.set(session)
    depth = _scope_depth.set(1)
    try:
        with session:
            yield session
    finally:
        _scope_depth.reset(depth)
        _open_session.reset(open_token)
