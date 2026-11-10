"""Liveness endpoint used by container and load-balancer probes."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    """Report that the API process is up.

    Returns:
        ``{"status": "ok"}``.
    """
    return {"status": "ok"}
