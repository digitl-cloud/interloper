"""Executions API: operation verdicts across the organisation's runs.

One run's executions are read through the run (``/runs/{id}/executions``);
this router reads across runs, such as each component's newest execution.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from interloper_db import ExecutionQuery, Page
from interloper_db.models import Execution
from pydantic import BaseModel

from interloper_api.dependencies import OrgIdDep, StoreDep, ViewerDep

router = APIRouter(prefix="/executions", tags=["executions"])


class ExecutionResponse(BaseModel):
    """Response body for an operation execution."""

    run_id: UUID
    org_id: UUID
    component_id: UUID | None = None
    component_key: str
    status: str
    started_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None
    created_at: dt.datetime | None = None

    @classmethod
    def from_row(cls, row: Execution) -> ExecutionResponse:
        """Convert an execution row to its response model.

        Args:
            row: The execution row.

        Returns:
            The response model.
        """
        return cls(
            run_id=row.run_id,
            org_id=row.org_id,
            component_id=row.component_id,
            component_key=row.component_key or "",
            status=row.status,
            started_at=row.started_at,
            completed_at=row.completed_at,
            created_at=row.created_at,
        )


@router.get("")
def list_executions(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    query: Annotated[ExecutionQuery, Query()],
) -> Page[ExecutionResponse]:
    """List the organisation's operation executions; ``latest=true`` keeps each component's newest.

    Args:
        user: The authenticated user, required to hold at least the viewer role.
        org_id: The active organisation UUID.
        store: The Store instance.
        query: Whether to keep only each component's newest execution, and the
            window to read.

    Returns:
        The page of executions.
    """
    return store.executions.list(org_id, query).map(ExecutionResponse.from_row)
