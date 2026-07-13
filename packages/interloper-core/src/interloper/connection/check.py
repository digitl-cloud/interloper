"""Connection-check core: static validation plus categorised live check.

Shared by every caller that checks a connection config — the API's
``/components/check`` endpoint (candidate config from the wizard) and the
agent's ``check_connection`` tool (stored config decoded from the store) —
so the failure categorisation cannot drift between surfaces.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from pydantic import ValidationError

from interloper.connection.base import Connection
from interloper.errors import ConnectionCheckError
from interloper.utils.concurrency import invoke

logger = logging.getLogger(__name__)

#: Upper bound on a live check — no caller may hang on a dead host.
CHECK_TIMEOUT = 15.0


@dataclass(frozen=True)
class CheckFieldError:
    """One static-validation error, addressed to a config field."""

    field: str
    message: str


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a connection check.

    ``live`` distinguishes a full check from a static-only one (the class
    implements no ``check()`` hook). ``category`` classifies failures so the
    caller can hint at a fix: bad ``config`` values, rejected ``auth``,
    unreachable ``network``, or an uncategorised ``error``.
    """

    ok: bool
    live: bool
    message: str | None = None
    category: Literal["config", "auth", "network", "error"] | None = None
    errors: list[CheckFieldError] = field(default_factory=list)


def _failure(exc: Exception, key: str) -> CheckResult:
    """Map a live-check exception to its result.

    Full details are logged server-side only — provider errors may carry
    URLs with tokens.

    Returns:
        The categorised failure result.
    """
    logger.error("Connection check failed for '%s': %s", key, exc)

    if isinstance(exc, ConnectionCheckError):
        return CheckResult(ok=False, live=True, category="error", message=str(exc))
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (401, 403):
            return CheckResult(ok=False, live=True, category="auth", message="The provider rejected the credentials.")
        return CheckResult(ok=False, live=True, category="error", message=f"The provider responded with HTTP {status}.")
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return CheckResult(ok=False, live=True, category="network", message="The provider did not respond in time.")
    if isinstance(exc, httpx.TransportError):
        return CheckResult(ok=False, live=True, category="network", message="The provider could not be reached.")
    return CheckResult(ok=False, live=True, category="error", message="The connection check failed unexpectedly.")


async def check_connection_config(
    connection_cls: type[Connection],
    config: dict[str, Any],
    *,
    key: str | None = None,
    timeout: float = CHECK_TIMEOUT,
) -> CheckResult:
    """Check a connection config, statically and (when supported) live.

    The static tier instantiates ``connection_cls`` from ``config`` — pydantic
    validation surfaces per-field errors. The live tier calls the class's
    ``check()`` hook (when implemented), a lightweight authenticated call
    against the provider. A failed check is this function's *expected*
    output: failures come back as ``ok=False`` results, never as exceptions.

    Returns:
        The check result.
    """
    # Only pass through fields the connection actually declares — the caller
    # may hold extra markers (e.g. an internal id) that the model would reject.
    config = {k: v for k, v in config.items() if k in connection_cls.model_fields}
    try:
        conn = connection_cls(**config)
    except ValidationError as exc:
        errors = [
            CheckFieldError(field=".".join(str(loc) for loc in e["loc"]), message=e["msg"]) for e in exc.errors()
        ]
        return CheckResult(
            ok=False, live=False, category="config", message="The configuration is invalid.", errors=errors
        )

    if not connection_cls.checkable():
        return CheckResult(ok=True, live=False)

    try:
        ok = bool(await asyncio.wait_for(invoke(conn.check), timeout=timeout))
    except Exception as exc:  # noqa: BLE001 — any hook failure is a categorised result, never a raise
        return _failure(exc, key or connection_cls.__name__)
    if not ok:
        return CheckResult(ok=False, live=True, category="error", message="The connection check failed.")
    return CheckResult(ok=True, live=True)
