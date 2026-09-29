"""Job tools: putting the collection's sources on a schedule."""

from __future__ import annotations

from uuid import UUID

from interloper.errors import CatalogKeyError, ConfigError

from interloper_toolkit.authz import requires_role
from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import ComponentRef, JobCreated, ToolError


@requires_role("editor")
def create_job(
    ctx: ToolkitContext,
    name: str,
    cron: str,
    target_source_ids: list[str],
    lookback: int | None = 1,
    offset: int = 1,
) -> JobCreated | ToolError:
    """Create a cron job that runs the given sources on a schedule.

    Use this after creating sources to put them on a cadence — one job can
    target several sources (e.g. every account of a source type). Recap the
    name, the schedule in words, and the targets, and get the user's
    explicit confirmation BEFORE calling this.

    Whether runs are partitioned is derived from the targets: a job over
    time-partitioned assets covers a trailing window of partitions each tick.

    Args:
        name: Display name for the job (e.g. 'Facebook Ads daily').
        cron: Standard cron expression (e.g. '0 6 * * *' for daily at 06:00 UTC).
        target_source_ids: UUIDs of the sources the job materializes.
        lookback: For partitioned targets, how many partitions each run covers.
        offset: For partitioned targets, how many partitions back from the
            current one the window ends. 1 (the default) means it ends on the
            last complete partition, i.e. yesterday for daily targets.
    """
    try:
        if not target_source_ids:
            return ToolError(error="target_source_ids must name at least one source")
        targets = [
            ctx.store.components.get(UUID(source_id), kind="source", org_id=ctx.org_id).id
            for source_id in target_source_ids
        ]
        try:
            row = ctx.store.components.create(
                ctx.org_id,
                kind="job",
                key="cron_job",
                name=name,
                config={"cron": cron, "enabled": True, "tags": [], "lookback": lookback, "offset": offset},
                relations={"targets": targets},
            )
        except (ConfigError, CatalogKeyError) as e:
            return ToolError(error=str(e))

        return JobCreated(
            message=f"Job '{name}' created",
            job=ComponentRef(id=row.id, kind=row.kind, key=row.key, name=row.name),
            cron=cron,
            enabled=True,
            target_count=len(targets),
        )
    except Exception as e:
        return ToolError(error=str(e))
