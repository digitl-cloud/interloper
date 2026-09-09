# Campaign matcher skeleton, phase 3 Implementation Plan

> **SUPERSEDED 2026-09-07** by `2026-09-07-relation-model-phase-*.md` (design `2026-09-07-relation-model-design.md`). Kept for the record; phase 1 here was executed as PR #321 and is being reworked.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `CampaignMatcher`, an Analytics source owning one daily asset that fans in every `campaigns` asset of the organisation and emits placeholder rows, so the fan-in mechanism can be run and verified end to end before the matching logic is designed.

**Architecture:** One source class with one `@il.asset` declaring `depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)}`; a `CampaignMatches` schema; the asset emits one placeholder row per leg that has data, carrying the leg's identity, and skips legs handed over with `data=None`. No connection, no config fields. Replaces the nine-line stub with the wrong class name.

**Tech Stack:** interloper-core phase 1, pytest. Spec: `docs/superpowers/specs/2026-09-04-downstream-assets-design.md` section 2.7.

## Global Constraints

- Branch `feat/campaign-matcher-skeleton` from `main` after phase 1 merged (phase 2 is not required for the tests, but is required for the dev-instance verification).
- Conventional Commits ending with `By Digitl`; commit only when asked.
- `uv run --frozen ...`; ruff 120; `ty` clean; full Google docstrings.
- Asset naming rules from AGENTS.md: `campaign_matches` is one row per campaign per day, tagged `Entity`, time-partitioned on a stamped `date`, schema class `CampaignMatches` in `schemas/campaign_matches.py`.
- The assets-wide test `tests/test_entity_partitioning.py` must keep passing (Entity assets partition on `date`).
- No em-dashes anywhere.

---

### Task 1: Schema

**Files:**
- Create: `packages/interloper-assets/src/interloper_assets/campaign_matcher/schemas/__init__.py`
- Create: `packages/interloper-assets/src/interloper_assets/campaign_matcher/schemas/campaign_matches.py`
- Test: `packages/interloper-assets/tests/test_campaign_matcher.py` (create)

**Interfaces:**
- Produces: `interloper_assets.campaign_matcher.schemas.CampaignMatches` with fields `date, source_key, source_id, campaign_id, campaign_name, canonical_name, match_method`.

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the campaign matcher skeleton.

The matcher fans in every ``campaigns`` asset through one many-valued slot
and emits placeholder rows until the matching logic is designed. These tests
pin the slot declaration, the schema, and the end-to-end fan-in read.
"""

from __future__ import annotations

import datetime as dt

import interloper as il

from interloper_assets.campaign_matcher import schemas
from interloper_assets.campaign_matcher.source import CampaignMatcher


def test_schema_fields():
    assert list(schemas.CampaignMatches.model_fields) == [
        "date",
        "source_key",
        "source_id",
        "campaign_id",
        "campaign_name",
        "canonical_name",
        "match_method",
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest packages/interloper-assets/tests/test_campaign_matcher.py -q`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Create the schema**

`schemas/campaign_matches.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignMatches(Schema):
    """One row per campaign per day, grouped under a canonical name. Placeholder rows until matching logic lands."""

    date: dt.date | None = Field(default=None, description="The day the snapshot was taken (stamped from the partition).")
    source_key: str | None = Field(default=None, description="Catalog key of the provider source the campaign came from.")
    source_id: str | None = Field(default=None, description="Id of the provider source instance (one per ad account).")
    campaign_id: str | None = Field(default=None, description="Campaign id as reported by the provider.")
    campaign_name: str | None = Field(default=None, description="Campaign name as reported by the provider.")
    canonical_name: str | None = Field(default=None, description="Name the campaign is grouped under across providers.")
    match_method: str | None = Field(default=None, description="How the canonical name was assigned: placeholder, rule, override.")
```

`schemas/__init__.py`:

```python
from .campaign_matches import CampaignMatches

__all__ = ["CampaignMatches"]
```

- [ ] **Step 4: Run the test**

Run: `uv run --frozen pytest packages/interloper-assets/tests/test_campaign_matcher.py -q`
Expected: the schema test PASSES (the source import still fails until Task 2; write the import inside the test that needs it if the module import blocks collection, or complete Task 2 before running).

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-assets/src/interloper_assets/campaign_matcher/schemas packages/interloper-assets/tests/test_campaign_matcher.py
git commit -m "feat(assets): add the CampaignMatches schema

By Digitl"
```

---

### Task 2: Source and asset

**Files:**
- Modify: `packages/interloper-assets/src/interloper_assets/campaign_matcher/source.py` (replace the stub)
- Modify: `packages/interloper-assets/src/interloper_assets/__init__.py` (export)
- Modify: `dev/interloper.yaml` (add to the dev catalog)
- Test: `packages/interloper-assets/tests/test_campaign_matcher.py`

**Interfaces:**
- Produces: `CampaignMatcher(il.Source)` with asset `campaign_matches`; slot `campaigns` is many, key `*.campaigns`; legs with `data=None` produce no row.

- [ ] **Step 1: Write the failing tests**

Append to the test module:

```python
class FakeProviderOne(il.Source):
    """Provider with a daily ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""

        partitioning = il.TimePartitionConfig(column="date")
        tags = ["Entity"]

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "id": "c-1", "name": "DE_Brand"}]


class FakeProviderTwo(il.Source):
    """Second provider with a daily ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""

        partitioning = il.TimePartitionConfig(column="date")
        tags = ["Entity"]

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "id": "c-2", "name": "DE_Brand"}]


def test_slot_declaration():
    asset_type = next(a for a in CampaignMatcher.asset_types if a.key == "campaign_matches")
    assert asset_type.declared_upstreams()["campaigns"] == il.Dependency(key="*.campaigns", many=True)


def test_asset_conventions():
    asset_type = next(a for a in CampaignMatcher.asset_types if a.key == "campaign_matches")
    assert asset_type.tags == ["Entity"]
    assert isinstance(asset_type.partitioning, il.TimePartitionConfig)
    assert asset_type.partitioning.column == "date"
    assert asset_type.schema is schemas.CampaignMatches


def test_fan_in_emits_one_placeholder_row_per_leg():
    mem = il.MemoryDestination()
    one, two = FakeProviderOne(destinations=[mem]), FakeProviderTwo(destinations=[mem])
    matcher = CampaignMatcher(destinations=[mem])
    partition = il.TimePartition(dt.date(2026, 1, 1))

    result = il.DAG(one, two, matcher).materialize(partition)

    assert result.status.value == "completed"
    rows = mem.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert sorted(row["source_key"] for row in rows) == ["fake_provider_one", "fake_provider_two"]
    assert {row["match_method"] for row in rows} == {"placeholder"}
    assert {row["date"] for row in rows} == {dt.date(2026, 1, 1)}


def test_missing_leg_is_skipped():
    mem = il.MemoryDestination()
    one, two = FakeProviderOne(destinations=[mem]), FakeProviderTwo(destinations=[mem])
    partition = il.TimePartition(dt.date(2026, 1, 1))
    il.DAG(one).materialize(partition)
    matcher = CampaignMatcher(destinations=[mem])

    result = il.DAG(one(materializable=False), two(materializable=False), matcher).materialize(partition)

    assert result.status.value == "completed"
    rows = mem.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert [row["source_key"] for row in rows] == ["fake_provider_one"]


def test_exported_and_in_the_dev_catalog():
    import interloper_assets

    assert interloper_assets.CampaignMatcher is CampaignMatcher
    dev_catalog = (Path(__file__).parents[3] / "dev" / "interloper.yaml").read_text()
    assert "interloper_assets.campaign_matcher.source.CampaignMatcher" in dev_catalog
```

Add `from pathlib import Path` to the imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest packages/interloper-assets/tests/test_campaign_matcher.py -q`
Expected: FAIL (`CampaignMatcher` does not exist; the stub defines `CampaignPerformanceAnalysis`).

- [ ] **Step 3: Write the source**

Replace `campaign_matcher/source.py` entirely:

```python
"""Campaign matcher: one lookup table grouping every provider's campaigns under canonical names."""

from __future__ import annotations

from typing import Any

import interloper as il

from interloper_assets.campaign_matcher import schemas


@il.source(tags=["Analytics"], icon="carbon:connection-signal")
class CampaignMatcher(il.Source):
    """Groups campaigns from every configured ad source under canonical names.

    The matching logic is not designed yet: the asset reads every bound
    ``campaigns`` leg and emits one placeholder row per leg with data, so the fan-in
    can be scheduled, run and observed end to end.
    """

    @il.asset(
        schema=schemas.CampaignMatches,
        partitioning=il.TimePartitionConfig(column="date"),
        depends_on={"campaigns": il.Dependency(key="*.campaigns", many=True)},
        tags=["Entity"],
    )
    def campaign_matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict[str, Any]]:
        """One row per campaign per day with its canonical name. Placeholder rows until matching lands."""
        rows: list[dict[str, Any]] = []
        for leg in campaigns:
            if leg.data is None:
                continue
            source = leg.asset.source
            rows.append(
                {
                    "date": context.partition_date,
                    "source_key": source.key if source is not None else None,
                    "source_id": source.id if source is not None else None,
                    "campaign_id": "placeholder",
                    "campaign_name": "Placeholder campaign",
                    "canonical_name": "Placeholder campaign",
                    "match_method": "placeholder",
                }
            )
        return rows
```

`interloper_assets/__init__.py`: add `from interloper_assets.campaign_matcher.source import CampaignMatcher` in alphabetical position (after `campaign_manager_360`) and `"CampaignMatcher",` to `__all__` after `"CampaignManager360Connection",`.

`dev/interloper.yaml`: add `  - interloper_assets.campaign_matcher.source.CampaignMatcher` after the `campaign_manager_360` line.

Check the repo-root `interloper.yaml` catalog list too; if it enumerates sources one by one, add the same line there so the prod catalog can pick it up (leave a note in the PR that enabling it in prod is a chart values decision).

- [ ] **Step 4: Run the assets suite**

Run: `uv run --frozen pytest packages/interloper-assets -q`
Expected: PASS, including `test_entity_partitioning.py`.

- [ ] **Step 5: Stage (commit only if asked)**

```bash
git add packages/interloper-assets/src/interloper_assets/campaign_matcher packages/interloper-assets/src/interloper_assets/__init__.py dev/interloper.yaml packages/interloper-assets/tests/test_campaign_matcher.py
git commit -m "feat(assets): add the CampaignMatcher source with a placeholder fan-in asset

By Digitl"
```

---

### Task 3: Dev-instance verification and the trigger-hook experiment

Requires phases 1 and 2 merged and a dev instance. Never port 3000, never reset the shared dev database; create only components you delete afterwards (record their ids first, see the `verify` project skill).

- [ ] **Step 1: Stand up the instance**

```bash
INTERLOPER_SERVER_PORT=3100 make dev-up
```

Reuse the `:3000` session cookie.

- [ ] **Step 2: Create two provider sources with placeholder credentials**

`POST /components` with `kind=source`, `key=snapchat_ads` and `key=tiktok_ads`, each with a connection resource created first via `POST /components` (`kind=connection`, the provider's connection key, placeholder secret values). No run is triggered, so no credential is used. Note both `campaigns` child ids from the responses.

- [ ] **Step 3: Create the matcher and bind both legs**

`POST /components` `kind=source key=campaign_matcher`, then `POST /components/{campaign_matches_id}/relations` twice with `type=upstream`, `slot=campaigns`, the two `campaigns` ids. `GET /components/relations?type=upstream` shows two edges from the matcher.

- [ ] **Step 4: Run the matcher for one partition**

`POST /runs` (or the runs endpoint the API exposes; `GET /docs`) targeting the matcher source with `partition_key=2026-01-01`. Expected: the run completes; the run's events include two `LOG` warnings "found no data in upstream ... at 2026-01-01" (the providers never ran) and a `dest_write_completed` for `campaign_matches` is absent because zero rows were produced ("produced no data; skipping write"). This is the expected skeleton behaviour and proves the legs were joined and read.

- [ ] **Step 5: Trigger-hook experiment**

Create a job per provider source and one for the matcher (`kind=job key=cron_job`, `relations={"target": [...]}`). Create a `TriggerHook` (`kind=hook key=trigger_hook`, config `events=["run_completed"]`, relations `watch=[both provider jobs]`, `target=[matcher job]`). Trigger both provider jobs for `2026-01-01`. They fail (placeholder credentials), so switch the hook to `events=["run_failed"]` for the experiment and trigger again. Record in the spec appendix: how many matcher runs were created, their `partition_key`, and their timing relative to the provider runs. Expected from the code: one matcher run per finished provider run, same partition key.

- [ ] **Step 6: Clean up and write the appendix**

Delete the hook, the jobs, the matcher source, the provider sources and their connections (in that order; the delete guard blocks a provider source while the matcher's edges point at it, so remove the matcher first). Append "Appendix A: trigger-hook experiment" to `docs/superpowers/specs/2026-09-04-downstream-assets-design.md` with the observations and a one-paragraph recommendation (keep the cron, or design a quorum on hooks).

- [ ] **Step 7: Open the PR**

Title `feat(assets): campaign matcher skeleton with a placeholder fan-in asset`. Body links the spec, states that matching logic is deliberately absent, ends with `By Digitl`.
