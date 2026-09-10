"""Campaign matcher: one canonical campaign across every advertising source of an organisation."""

from __future__ import annotations

import difflib
import re
import unicodedata
import uuid
from typing import Any

import interloper as il

from interloper_assets.campaign_matcher import schemas

MATCH_NAMESPACE = uuid.UUID("6f1c0a2e-3b6d-4d55-9d1a-2c7a4e8b9f01")


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


def normalise(name: str, key_pattern: re.Pattern[str] | None = None) -> str:
    """Reduce a campaign name to the key it is matched on.

    Unicode is folded to its compatibility form, case and surrounding
    whitespace dropped, punctuation removed and inner whitespace collapsed,
    so spellings that differ only in how a platform or a person typed them
    match. When *key_pattern* is given and matches, its ``key`` group replaces
    the whole name first: that is how a naming convention
    (``client_market_objective_...``) picks the part that identifies the
    campaign.

    Args:
        name: The campaign name as the platform reports it.
        key_pattern: A compiled pattern with a ``key`` group, or ``None`` to
            match on the whole name.

    Returns:
        The normalised key, possibly empty for a name made of punctuation only.
    """
    if key_pattern is not None:
        found = key_pattern.search(name)
        if found is not None and found.group("key"):
            name = found.group("key")
    folded = unicodedata.normalize("NFKC", name).casefold()
    return " ".join(re.sub(r"[^\w\s]", " ", folded).split())


@il.source(tags=["Analytics"], icon="carbon:connect")
class CampaignMatcher(il.Source):
    """Matches campaigns across every advertising source in the organisation into one lookup table.

    Every connector's ``campaigns`` asset feeds it. Each campaign name is
    reduced to a normalised key, optionally through a naming-convention
    pattern, and campaigns sharing a key share a match. Keys that merely
    resemble each other can be merged too, above a similarity threshold.
    """

    key_pattern: str | None = il.InputField(
        default=None,
        description="Regular expression with a `key` group selecting the part of a campaign name that identifies it",
    )
    similarity_threshold: float = il.InputField(
        default=1.0,
        description="Merge campaigns whose normalised names are at least this similar (0 to 1); 1.0 merges equal keys",
    )

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
        """One row per campaign of every advertising source, with the canonical campaign it matches."""
        pattern = re.compile(self.key_pattern) if self.key_pattern else None
        rows: list[dict[str, Any]] = []
        for leg in campaigns:
            if leg.data is None:
                context.logger.warning(f"No data for upstream '{leg.asset.qualified_key}' in this partition; skipped")
                continue
            source = leg.asset.source
            for row in leg.records:
                name = _first(row, "campaign_name", "name", "campaign")
                rows.append(
                    {
                        "date": context.partition_date,
                        "platform": source.key if source else "",
                        "account": (source.discriminator or source.id) if source else "",
                        "campaign_id": _first(row, "campaign_id", "id"),
                        "campaign_name": name,
                        "canonical_name": normalise(name, pattern),
                    }
                )
        canonical = self._merge(sorted({row["canonical_name"] for row in rows}))
        for row in rows:
            key = canonical[row["canonical_name"]]
            row["similarity"] = round(difflib.SequenceMatcher(None, row["canonical_name"], key).ratio(), 4)
            row["canonical_name"] = key
            row["match_id"] = str(uuid.uuid5(MATCH_NAMESPACE, key))
        return rows

    def _merge(self, keys: list[str]) -> dict[str, str]:
        """Map every normalised key to the canonical key of its match.

        Each key is compared to the canonical keys already accepted, in
        sorted order, and joins the first one it resembles at least
        ``similarity_threshold``; otherwise it becomes a canonical key
        itself. At the default threshold of ``1.0`` only equal keys merge, so
        the map is the identity.

        Args:
            keys: The distinct normalised keys of this partition, sorted.

        Returns:
            Normalised key to the canonical key it belongs to.
        """
        if self.similarity_threshold >= 1.0:
            return {key: key for key in keys}
        canonical: dict[str, str] = {}
        accepted: list[str] = []
        for key in keys:
            match = next(
                (c for c in accepted if difflib.SequenceMatcher(None, key, c).ratio() >= self.similarity_threshold),
                None,
            )
            if match is None:
                accepted.append(key)
                match = key
            canonical[key] = match
        return canonical
