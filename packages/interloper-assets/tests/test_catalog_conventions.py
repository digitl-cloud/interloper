"""Conventions every asset in the catalog follows, checked over the whole catalog at once.

Entity assets are dimension snapshots; partitioning them (even where the
partition seems redundant) keeps every asset uniformly partitioned for the
scheduling and destination machinery. The BigQuery destination requires the
partition column to exist on the schema, as a date, so every time-partitioned
schema must declare it.

Field types follow rule 3 of ``.claude/rules/assets.md``: a field whose name
says what its value is (an identifier, an instant, a day, an amount) carries
the matching type. A field that deliberately does not is listed in
``EXCEPTIONS`` with the reason, and an exception nothing needs any more fails.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import inspect
import re
from collections.abc import Iterator

import interloper as il
from interloper.schema import FieldSpec
from interloper.source import Source

import interloper_assets

# The demo source is a test fixture, exempt from asset conventions.
_EXEMPT_SOURCES = {"DemoSource"}

_LOCAL_SCHEDULE = "local wall clock with the account's UTC offset; a timestamp would keep only the UTC instant"
_GA4_EXPORT = "mirrors GA4's own BigQuery export schema"

EXCEPTIONS: dict[str, str] = {
    "FacebookAds.adsets.start_time": _LOCAL_SCHEDULE,
    "FacebookAds.adsets.end_time": _LOCAL_SCHEDULE,
    "FacebookAds.campaigns.start_time": _LOCAL_SCHEDULE,
    "FacebookAds.campaigns.stop_time": _LOCAL_SCHEDULE,
    "FacebookAds.ads.ad_schedule_start_time": _LOCAL_SCHEDULE,
    "FacebookAds.ads.ad_schedule_end_time": _LOCAL_SCHEDULE,
    "GoogleAnalytics4Export.events.event_date": _GA4_EXPORT,
    "GoogleAnalytics4Export.events.event_timestamp": _GA4_EXPORT,
    "Adservice.conversions_stats_by_time_of_day.day": "the day of the week, not a date",
}

_IDENTIFIER = re.compile(r"(^|_)id$")
_INSTANT = re.compile(r"(_at|timestamp)$")
_TIME = re.compile(r"_time$")
_DAY = re.compile(r"^(date|day)$|_date$")
_AMOUNT = re.compile(r"(^|_)(amount|balance|budget|cap|cost|price|revenue|spend)$")


def _catalog_sources() -> list[type[Source]]:
    sources = []
    for name in dir(interloper_assets):
        obj = getattr(interloper_assets, name)
        if inspect.isclass(obj) and issubclass(obj, Source) and obj is not Source and name not in _EXEMPT_SOURCES:
            sources.append(obj)
    return sources


def _fields() -> Iterator[tuple[str, FieldSpec]]:
    for source in _catalog_sources():
        for asset_type in source.asset_types:
            if asset_type.schema is None:
                continue
            for spec in asset_type.schema.field_specs():
                yield f"{source.__name__}.{asset_type.key}.{spec.name}", spec


def _breaks_a_rule(spec: FieldSpec) -> bool:
    name, kind = spec.name, spec.type
    return (
        (_IDENTIFIER.search(name) is not None and kind is not str)
        or (_INSTANT.search(name) is not None and kind is not dt.datetime)
        or (_TIME.search(name) is not None and kind is str)
        or (_DAY.search(name) is not None and kind not in (dt.date, dt.datetime))
        or (_AMOUNT.search(name) is not None and kind is str)
    )


def _excepted(path: str) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in EXCEPTIONS)


def _describe(path: str, spec: FieldSpec) -> str:
    kind = spec.type.__name__ if isinstance(spec.type, type) else str(spec.type)
    return f"{path}: {kind}"


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


def test_every_partition_column_is_a_declared_date():
    misses = []
    for source in _catalog_sources():
        for asset_type in source.asset_types:
            partitioning = asset_type.partitioning
            if not isinstance(partitioning, il.TimePartitionConfig) or asset_type.schema is None:
                continue
            types = {spec.name: spec.type for spec in asset_type.schema.field_specs()}
            if types.get(partitioning.column) is not dt.date:
                misses.append(
                    f"{source.__name__}.{asset_type.key}.{partitioning.column}: {types.get(partitioning.column)}"
                )
    assert not misses, "partition columns not declared as dt.date:\n" + "\n".join(misses)


def test_field_types_match_what_their_names_say():
    misses = [_describe(path, spec) for path, spec in _fields() if _breaks_a_rule(spec) and not _excepted(path)]
    assert not misses, (
        "ids must be str, *_at/*timestamp dt.datetime, *_time never str, days dt.date, amounts never str "
        "(add a reasoned EXCEPTIONS entry only for a deliberate exception):\n" + "\n".join(misses)
    )


def test_every_exception_is_still_needed():
    breaking = [path for path, spec in _fields() if _breaks_a_rule(spec)]
    stale = [pattern for pattern in EXCEPTIONS if not any(fnmatch.fnmatchcase(path, pattern) for path in breaking)]
    assert not stale, "EXCEPTIONS entries no field needs any more:\n" + "\n".join(stale)
