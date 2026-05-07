"""Analytics tools — run statistics, partition coverage, and data freshness."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext

from interloper_agent.context import get_org_id, get_store, serialize


def run_history_summary(
    job_id: str | None = None,
    days: int = 7,
    tool_context: ToolContext = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """Summarize run statistics over a period.

    Args:
        job_id: Filter to a specific job UUID (optional, all jobs if omitted).
        days: Number of days to look back (default 7).

    Returns aggregate counts (total, success, failed, canceled),
    success rate, and average duration.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        runs = store.list_runs(
            org_id,
            job_id=UUID(job_id) if job_id else None,
            limit=500,
        )

        cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=days)
        recent = [r for r in runs if r.created_at and r.created_at >= cutoff]

        total = len(recent)
        by_status: dict[str, int] = {}
        durations: list[float] = []
        for r in recent:
            by_status[r.status] = by_status.get(r.status, 0) + 1
            if r.started_at and r.completed_at:
                durations.append((r.completed_at - r.started_at).total_seconds())

        success = by_status.get("success", 0)
        return {
            "status": "success",
            "period_days": days,
            "job_id": job_id,
            "total_runs": total,
            "by_status": by_status,
            "success_rate": round(success / total, 2) if total > 0 else None,
            "avg_duration_seconds": round(sum(durations) / len(durations), 1) if durations else None,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def partition_coverage(
    job_id: str,
    start_date: str,
    end_date: str,
    tool_context: ToolContext = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """Check partition coverage for a job over a date range.

    Args:
        job_id: UUID of the job.
        start_date: Start date in ISO format (YYYY-MM-DD).
        end_date: End date in ISO format (YYYY-MM-DD), inclusive.

    Returns which dates have successful runs and which are missing.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        jid = UUID(job_id)
        runs = store.list_runs(org_id, job_id=jid, limit=1000)

        start = datetime.date.fromisoformat(start_date)
        end = datetime.date.fromisoformat(end_date)

        # Collect dates with successful runs
        covered: set[datetime.date] = set()
        for r in runs:
            if r.status == "success" and r.partition_date:
                if start <= r.partition_date <= end:
                    covered.add(r.partition_date)

        # Build expected date range
        expected: list[datetime.date] = []
        current = start
        while current <= end:
            expected.append(current)
            current += datetime.timedelta(days=1)

        missing = sorted(set(expected) - covered)
        coverage_pct = round(len(covered) / len(expected) * 100, 1) if expected else 100.0

        return {
            "status": "success",
            "job_id": job_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_days": len(expected),
            "covered_days": len(covered),
            "missing_days": len(missing),
            "coverage_percent": coverage_pct,
            "missing_dates": [d.isoformat() for d in missing],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def freshness_check(tool_context: ToolContext) -> dict[str, Any]:
    """Check data freshness for all jobs.

    Returns the last successful run timestamp for each job and flags
    any that haven't succeeded in over 24 hours.
    """
    try:
        org_id = get_org_id(tool_context)
        store = get_store()
        jobs = store.list_jobs(org_id)
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        results = []
        for job in jobs:
            if not job.enabled:
                continue
            job_id = job.id  # type: ignore[assignment]
            runs = store.list_runs(org_id, job_id=job_id, status="success", limit=1)
            last_success = runs[0] if runs else None

            hours_since = None
            if last_success and last_success.completed_at:
                delta = now - last_success.completed_at
                hours_since = round(delta.total_seconds() / 3600, 1)

            results.append({
                "job": serialize(job),
                "last_success_at": serialize(last_success.completed_at) if last_success else None,
                "hours_since_success": hours_since,
                "stale": hours_since is None or hours_since > 24,
            })

        stale_count = sum(1 for r in results if r["stale"])
        return {
            "status": "success",
            "total_jobs": len(results),
            "stale_count": stale_count,
            "jobs": results,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
