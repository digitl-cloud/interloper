"""Internal (machine-to-machine) API: reliable event ingest.

Per-asset worker processes run in slim, DB-less pods, so instead of routing
their events through stdout/log-scraping (lossy), they POST them here.  The
endpoint is authenticated with a shared service token (not the user session)
and writes each event with :meth:`Store.save_event`, which is idempotent — so
the worker can retry a batch freely without creating duplicates.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import interloper as il
from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Store
from pydantic import BaseModel

from interloper_api.dependencies import get_store, require_ingest_token

logger = logging.getLogger(__name__)

router = APIRouter()


class EventIngestRequest(BaseModel):
    """A batch of serialized events for a single run.

    Each entry is the flat dict produced by ``Event.to_dict()`` (``event_id``,
    ``type``, ``timestamp`` plus inlined metadata).
    """

    events: list[dict[str, Any]]


class EventIngestResponse(BaseModel):
    """Result of an ingest batch."""

    accepted: int
    rejected: int


@router.post("/runs/{run_id}/events")
def ingest_run_events(
    run_id: UUID,
    body: EventIngestRequest,
    _: None = Depends(require_ingest_token),
    store: Store = Depends(get_store),
) -> EventIngestResponse:
    """Persist a batch of events for *run_id*.

    The ``org_id`` is resolved server-side from the run, so callers only need
    the run id.  Malformed events are skipped (and counted in ``rejected``)
    rather than failing the batch; a persistence error is allowed to surface as
    a 5xx so the caller retries — idempotent writes make that safe.
    """
    try:
        run = store.get_run(run_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    accepted = 0
    rejected = 0
    for raw in body.events:
        try:
            event = il.Event.from_dict(raw)
        except Exception:  # noqa: BLE001 - malformed event: skip, don't retry
            rejected += 1
            logger.warning("Rejected malformed event for run %s", run_id)
            continue
        store.save_event(event, org_id=run.org_id, run_id=run_id)
        accepted += 1

    return EventIngestResponse(accepted=accepted, rejected=rejected)
