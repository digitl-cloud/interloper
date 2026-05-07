"""External provider API routes for FetchField resolution.

Each submodule contains routes for a specific external provider.
The combined ``router`` is re-exported here so ``app.py`` can
import it unchanged.
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


# Import and register sub-routers after helpers are defined.
from interloper_api.routes.external.amazon_ads import sub_router as amazon_ads_router  # noqa: E402
from interloper_api.routes.external.facebook_ads import sub_router as facebook_ads_router  # noqa: E402
from interloper_api.routes.external.google_ads import sub_router as google_ads_router  # noqa: E402
from interloper_api.routes.external.pinterest_ads import sub_router as pinterest_ads_router  # noqa: E402
from interloper_api.routes.external.snapchat_ads import sub_router as snapchat_ads_router  # noqa: E402

router.include_router(amazon_ads_router)
router.include_router(facebook_ads_router)
router.include_router(google_ads_router)
router.include_router(pinterest_ads_router)
router.include_router(snapchat_ads_router)
