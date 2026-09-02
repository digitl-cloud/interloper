"""Tests for the run models (``interloper_db.models.runs``)."""

from __future__ import annotations

from uuid import uuid4

from interloper_db.models import Component, Run


def test_event_metadata_carries_target_context() -> None:
    org = uuid4()
    target = Component(org_id=org, kind="job", key="nightly", name="Nightly sync")
    run = Run(id=uuid4(), org_id=org, component_id=target.id, backfill_id=uuid4())

    metadata = run.event_metadata(target)

    assert metadata == {
        "run_id": str(run.id),
        "backfill_id": str(run.backfill_id),
        "org_id": str(org),
        "target_id": str(target.id),
        "target_kind": "job",
        "target_key": "nightly",
        "target_name": "Nightly sync",
    }


def test_event_metadata_without_target() -> None:
    run = Run(id=uuid4(), org_id=uuid4())

    metadata = run.event_metadata(None)

    assert metadata == {"run_id": str(run.id), "backfill_id": None, "org_id": str(run.org_id)}
