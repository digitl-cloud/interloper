"""Tests for ``interloper_toolkit.models``."""

from __future__ import annotations

import datetime
from uuid import uuid4

from interloper_db import ComponentStatus
from interloper_db.models import Component
from interloper_db.store.insights import AssetKeyCoverage, Attention, ErrorCause, ErrorGroup, JobCoverage

from interloper_toolkit.models import AssetCoverage, AttentionRow

SINCE = datetime.datetime(2026, 10, 6, 9, tzinfo=datetime.timezone.utc)


class TestAttentionRow:
    def test_an_error_group_carries_its_cause_and_run_count(self):
        job = Component(org_id=uuid4(), kind="job", key="cron_job", name="Daily")
        runs = {uuid4(), uuid4()}
        group = ErrorGroup(
            job_id=job.id,
            asset_key=None,
            cause=ErrorCause(fingerprint="f", summary="HTTP 401 from graph.facebook.com"),
            first_seen=SINCE,
            last_seen=SINCE,
            sample_run_id=next(iter(runs)),
            sample="Traceback ...",
            runs=runs,
        )

        row = AttentionRow.from_attention(Attention(kind="error_group", since=SINCE, component=job, error_group=group))

        assert (row.kind, row.component_name, row.component_kind) == ("error_group", "Daily", "job")
        assert (row.error, row.runs) == ("HTTP 401 from graph.facebook.com", 2)

    def test_an_uncaused_group_falls_back_to_its_sample(self):
        group = ErrorGroup(
            job_id=None,
            asset_key="b",
            cause=None,
            first_seen=SINCE,
            last_seen=SINCE,
            sample_run_id=uuid4(),
            sample="boom",
        )

        row = AttentionRow.from_attention(Attention(kind="error_group", since=SINCE, error_group=group))

        assert (row.error, row.runs, row.component_id) == ("boom", 0, None)

    def test_a_drift_names_its_status_and_a_renewal_error_its_detail(self):
        source = Component(org_id=uuid4(), kind="source", key="old_source", name=None)

        drift = AttentionRow.from_attention(
            Attention(kind="drift", since=None, component=source, status=ComponentStatus.MISSING)
        )
        renewal = AttentionRow.from_attention(Attention(kind="renewal_error", since=SINCE, detail="invalid_grant"))

        assert (drift.error, drift.component_name, drift.runs) == ("missing", "old_source", None)
        assert renewal.error == "invalid_grant"


class TestAssetCoverage:
    def test_a_missed_snapshot_is_counted_but_never_offered_for_a_backfill(self):
        keys = ["2026-07-01", "2026-07-02", "2026-07-03"]
        snapshot = AssetKeyCoverage(
            uuid4(),
            "campaigns",
            expected=frozenset({"2026-07-03"}),
            covered=frozenset(),
            failed=frozenset({"2026-07-03"}),
            missed=frozenset({"2026-07-01"}),
        )

        coverage = AssetCoverage.from_coverage(JobCoverage(uuid4(), keys, [snapshot]), limit=10, offset=0)

        [row] = coverage.assets
        assert (row.covered, row.failed, row.missed, row.never_run) == (0, 1, 1, 0)
        assert [(r.start_key, r.end_key) for r in row.missing] == [("2026-07-03", "2026-07-03")]
        assert (coverage.all_covered, coverage.partly_covered, coverage.none_covered) == (0, 0, 1)
