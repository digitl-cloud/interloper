"""Span/metric attribute names and helpers."""

from __future__ import annotations

from typing import Any

RUN_ID = "interloper.run.id"
BACKFILL_ID = "interloper.backfill.id"
ASSET_ID = "interloper.asset.id"
ASSET_KEY = "interloper.asset.key"
ASSET_QUALIFIED_KEY = "interloper.asset.qualified_key"
SOURCE_ID = "interloper.source.id"
PARTITION = "interloper.partition"
DESTINATION_KEY = "interloper.destination.key"
UPSTREAM_KEY = "interloper.upstream.key"
RESOURCE_NAME = "interloper.resource.name"
DAG_ASSET_COUNT = "interloper.dag.asset_count"
DAG_SPEC_ITEMS = "interloper.dag.spec_items"
RUNNER_TYPE = "interloper.runner.type"
LAUNCHER_TYPE = "interloper.launcher.type"
ROLE = "interloper.role"

# Event-metadata keys (see Asset._event_metadata / RunState) → attribute names.
_METADATA_KEYS = {
    "run_id": RUN_ID,
    "backfill_id": BACKFILL_ID,
    "asset_id": ASSET_ID,
    "asset_key": ASSET_KEY,
    "asset_qualified_key": ASSET_QUALIFIED_KEY,
    "partition_or_window": PARTITION,
    "source_id": SOURCE_ID,
    "destination_key": DESTINATION_KEY,
}


def from_metadata(metadata: dict[str, Any]) -> dict[str, str]:
    """Map event-style metadata onto span attributes.

    ``None`` values are dropped (OTel rejects them) and everything else is
    stringified (ids are UUIDs).

    Args:
        metadata: Event metadata (run-level metadata + asset identity fields).

    Returns:
        Attribute dict keyed by the ``interloper.*`` attribute names.
    """
    return {attr: str(metadata[key]) for key, attr in _METADATA_KEYS.items() if metadata.get(key) is not None}
