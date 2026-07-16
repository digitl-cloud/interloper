"""Every Entity asset in the catalog must be time-partitioned on a stamped ``date``.

Entity assets are dimension snapshots; partitioning them (even where the
partition seems redundant) keeps every asset uniformly partitioned for the
scheduling and destination machinery. The BigQuery destination requires the
partition column to exist on the schema, so each entity schema must carry the
stamped ``date`` field.
"""

from __future__ import annotations

import datetime as dt
import inspect

import interloper as il
from interloper.source.base import Source

import interloper_assets

# The demo source is a test fixture, exempt from asset conventions.
_EXEMPT_SOURCES = {"DemoSource"}


def _catalog_sources() -> list[type[Source]]:
    sources = []
    for name in dir(interloper_assets):
        obj = getattr(interloper_assets, name)
        if inspect.isclass(obj) and issubclass(obj, Source) and obj is not Source and name not in _EXEMPT_SOURCES:
            sources.append(obj)
    return sources


def test_every_entity_asset_is_partitioned_on_date():
    checked = 0
    for source in _catalog_sources():
        for asset_type in source.asset_types:
            if "Entity" not in (asset_type.tags or []):
                continue
            checked += 1
            partitioning = asset_type.partitioning
            assert isinstance(partitioning, il.TimePartitionConfig), (
                f"{source.__name__}.{asset_type.key} is an Entity asset without time partitioning"
            )
            assert partitioning.column == "date", (
                f"{source.__name__}.{asset_type.key} must partition on 'date', not {partitioning.column!r}"
            )
            schema = asset_type.schema
            assert schema is not None and "date" in schema.model_fields, (
                f"{source.__name__}.{asset_type.key}: schema {schema and schema.__name__} lacks the 'date' column"
            )
            assert schema.model_fields["date"].annotation == (dt.date | None), (
                f"{source.__name__}.{asset_type.key}: 'date' must be typed dt.date | None"
            )
    assert checked >= 18, f"expected at least 18 entity assets in the catalog, found {checked}"
