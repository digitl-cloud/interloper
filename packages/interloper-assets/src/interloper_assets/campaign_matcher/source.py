"""Campaign matcher: canonical campaign names across every advertising source of an organisation.

The matching logic below (lower-cased, stripped name equality) is a placeholder:
phase 3 ships it only to prove the fan-in of a many-valued wildcard upstream over
every ``campaigns`` asset in the DAG. A real fuzzy-matching strategy is future work.
"""

from __future__ import annotations

from typing import Any

import interloper as il
from interloper.representation import Representation

from interloper_assets.campaign_matcher import schemas


def _match(row: dict[str, Any]) -> dict[str, Any]:
    """Build the placeholder match fields for one upstream campaign row.

    Args:
        row: One upstream campaign record, expected to carry ``id`` and ``name``.

    Returns:
        The ``campaign_id``, ``campaign_name``, ``canonical_name``, and
        ``similarity`` fields for :class:`~interloper_assets.campaign_matcher.schemas.CampaignMatches`.
    """
    name = str(row.get("name", ""))
    return {
        "campaign_id": str(row.get("id", "")),
        "campaign_name": name,
        "canonical_name": name.strip().lower(),
        "similarity": 1.0,
    }


@il.source(tags=["Analytics"], icon="carbon:connect")
class CampaignMatcher(il.Source):
    """Matches campaigns across every advertising source into one canonical lookup table.

    The matching logic is a placeholder (see module docstring); this source
    exists to prove that a many-valued wildcard upstream (``*.campaigns``) fans
    every ``campaigns`` asset in the DAG into a single downstream asset.
    """

    @il.asset(
        schema=schemas.CampaignMatches,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)},
    )
    def campaign_matches(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
    ) -> list[dict[str, Any]]:
        """Canonical campaign name matches, one row per upstream campaign across every advertising source."""
        rows: list[dict[str, Any]] = []
        for leg in campaigns:
            if leg.data is None:
                context.logger.warning(
                    f"No data for upstream '{leg.asset.qualified_key}' in this partition; leg skipped"
                )
                continue
            source = leg.asset.source
            for row in Representation.of(leg.data).to_records(leg.data):
                rows.append(
                    {
                        "date": context.partition_date,
                        "source_key": source.key if source else "",
                        "source_id": source.id if source else "",
                        **_match(row),
                    }
                )
        return rows
