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


def _first(row: dict[str, Any], *keys: str) -> str:
    """Return the first non-empty value among *keys*, or an empty string.

    Args:
        row: The record to read from.
        *keys: Field names to try, in order.

    Returns:
        The first present, non-empty value as a string, or ``""`` when none match.
    """
    for key in keys:
        value = row.get(key)
        # Falls through on "" as well as None: the shipped connector schemas type these fields as str | None.
        if value:
            return str(value)
    return ""


def _match(row: dict[str, Any]) -> dict[str, Any]:
    """Build the placeholder match fields for one upstream campaign row.

    Different connector ``campaigns`` schemas name the same fields
    differently (Facebook: ``id``/``name``; TikTok: ``campaign_id``/
    ``campaign_name``), so both spellings are tried in turn.

    Args:
        row: One upstream campaign record, expected to carry a campaign id
            and name under either spelling.

    Returns:
        The ``campaign_id``, ``campaign_name``, ``canonical_name``, and
        ``similarity`` fields for :class:`~interloper_assets.campaign_matcher.schemas.CampaignMatches`.
    """
    name = _first(row, "campaign_name", "name")
    return {
        "campaign_id": _first(row, "campaign_id", "id"),
        "campaign_name": name,
        "canonical_name": name.strip().lower(),
        "similarity": 1.0,
    }


@il.source(tags=["Analytics"], icon="carbon:connect")
class CampaignMatcher(il.Source):
    """Matches campaigns across every advertising source in the organisation into one lookup table.

    Every connector's ``campaigns`` asset feeds it; each campaign is reduced to
    a canonical name so the same campaign can be recognised across platforms.
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
        """One row per campaign of every advertising source, with the canonical name it is matched on."""
        rows: list[dict[str, Any]] = []
        for leg in campaigns:
            if leg.data is None:
                context.logger.warning(
                    f"No data for upstream '{leg.asset.qualified_key}' in this partition; leg skipped"
                )
                continue
            for row in Representation.of(leg.data).to_records(leg.data):
                rows.append({"date": context.partition_date, **_match(row)})
        return rows
