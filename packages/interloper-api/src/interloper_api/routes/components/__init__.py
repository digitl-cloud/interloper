"""Component API routes.

One surface for every component operation: store CRUD over persisted
instances (``crud``), plus the type-level operations that execute a
component class against a candidate, unsaved config — resolving a
FetchField's options (``resolve``) and checking a connection (``check``).
"""

from __future__ import annotations

from fastapi import APIRouter

from interloper_api.routes.components.check import sub_router as check_router
from interloper_api.routes.components.crud import router as crud_router
from interloper_api.routes.components.resolve import sub_router as resolve_router

router = APIRouter()
router.include_router(crud_router)
router.include_router(resolve_router)
router.include_router(check_router)
