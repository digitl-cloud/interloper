"""External provider API routes.

FetchField options are resolved by a single generic endpoint (``resolve``);
there are no per-provider routes. Each connector exposes its lookups as
``@fetch_field_provider`` methods on its connection class, which the resolver calls.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter()


def handle_error(error: Exception, context: str) -> None:
    """Map external API errors to appropriate HTTP responses."""
    logger.error("Error %s: %s", context, error)

    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        if status in (401, 403):
            raise HTTPException(status_code=status, detail=f"Authorization failed while {context}.")
        if status == 404:
            raise HTTPException(status_code=404, detail=f"Resource not found while {context}.")

    if isinstance(error, HTTPException):
        raise error

    raise HTTPException(status_code=500, detail=f"Failed {context}.")


# Import and register the resolver after helpers are defined.
from interloper_api.routes.external.resolve import sub_router as resolve_router  # noqa: E402

router.include_router(resolve_router)
