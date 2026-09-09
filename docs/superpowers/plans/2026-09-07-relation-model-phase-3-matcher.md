# Relation model, phase 3 (campaign matcher) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the `campaign_matcher` source: one entity asset, `campaign_matches`, fanning in every `campaigns` asset of the organisation through a many-valued wildcard relation, producing fake match rows per leg. It is the north-star proof of the relation model end to end: declaration, DAG resolution, read-only upstream inclusion, `None` legs, platform binding and a manifest.

**Architecture:** The source lives in `packages/interloper-assets/src/interloper_assets/campaign_matcher/` (the empty placeholder already exists) with `schemas/campaign_matches.py`. Matching logic is a stub that copies the upstream row's name and stamps the leg's source. Tests run the DAG against two fake connector sources with `il.MemoryDestination`. Spec: `docs/superpowers/specs/2026-09-07-relation-model-design.md`, section 5.5 (matcher example) and 11 (phase 3).

**Tech Stack:** interloper-core, interloper-assets conventions (Entity naming, `date` partition column, `tags=["Entity"]`), pytest.

## Global Constraints

- Stacked PR: branch `feat/campaign-matcher` from `feat/relation-model-platform` (phase 2) once its final review is clean; PR base is the phase 2 branch, retargeted as the stack merges.
- Conventional Commits ending `By Digitl`; `feat(assets):`. Commit only when Guillaume has asked.
- `uv run --frozen`; ruff 120; ty clean; full Google docstrings; assets tests live in `packages/interloper-assets/tests/test_campaign_matcher.py` (one file per source is the existing layout there).
- Asset naming rules from `AGENTS.md`: Entity assets are bare plural nouns, time-partitioned on a stamped `date` column, schema class `CampaignMatches` in `schemas/campaign_matches.py`.
- No em-dashes anywhere.

---

### Task 1: Schema and source

**Files:**
- Create: `packages/interloper-assets/src/interloper_assets/campaign_matcher/schemas/__init__.py`, `schemas/campaign_matches.py`
- Rewrite: `packages/interloper-assets/src/interloper_assets/campaign_matcher/source.py`, `__init__.py`
- Modify: wherever `demo` is registered for the catalog (`packages/interloper-assets/src/interloper_assets/__init__.py` and the `interloper.components` or equivalent entry-point group in `packages/interloper-assets/pyproject.toml`; find it with `grep -rn "demo" packages/interloper-assets/pyproject.toml packages/interloper-assets/src/interloper_assets/__init__.py`)
- Test: `packages/interloper-assets/tests/test_campaign_matcher.py`

**Interfaces:**
- Produces:
  ```py
  class CampaignMatches(il.Schema):
      date: dt.date | None
      source_key: str
      source_id: str
      campaign_id: str
      campaign_name: str
      canonical_name: str
      similarity: float

  @il.source(tags=["Analytics"], icon="carbon:connect")
  class CampaignMatcher(il.Source):
      @il.asset(schema=CampaignMatches, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"],
                relations={"campaigns": il.Relation("asset", "*.campaigns", many=True)})
      def campaign_matches(self, context: il.ExecutionContext, campaigns: list[il.Upstream]) -> list[dict[str, Any]]
  ```
- Fake logic: for each leg with `data is not None`, for each row, emit `canonical_name = row["name"].strip().lower()` and `similarity = 1.0`; a leg with `data is None` is skipped and counted in a `context.logger.warning`. Upstream rows are read in the destination's read representation; normalise to a list of dicts with `il.Representation` helpers the other sources use (see `tiktok_ads` tests for the pattern), and read `id` and `name` from each row.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for the campaign matcher source: a many-valued wildcard upstream over every `campaigns` asset."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il
import pytest

from interloper_assets.campaign_matcher.source import CampaignMatcher

PARTITION = il.TimePartitionConfig(column="date")


class CampaignsSchema(il.Schema):
    date: dt.date | None
    id: str
    name: str


def _connector(source_key: str, names: list[str]) -> type[il.Source]:
    @il.source(key=source_key)
    class Connector(il.Source):
        @il.asset(schema=CampaignsSchema, partitioning=PARTITION, tags=["Entity"])
        def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"date": context.partition_date, "id": f"{source_key}-{i}", "name": name} for i, name in enumerate(names)]

    return Connector


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


def test_relation_is_a_many_valued_wildcard() -> None:
    relation = CampaignMatcher.campaign_matches.relations["campaigns"]
    assert (relation.kind, relation.key, relation.many, relation.optional) == ("asset", "*.campaigns", True, False)


def test_dag_binds_every_campaigns_asset_and_matches_each_leg() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["Summer Sale ", "brand"])(destinations=[memory])
    tt = _connector("tt_like", ["summer sale"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    dag = il.DAG(fb, tt, matcher)
    assert set(dag.get_predecessors(matcher.campaign_matches.id)) == {fb.campaigns.id, tt.campaigns.id}
    dag.materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
    rows = memory.read_rows(matcher.campaign_matches)          # use the MemoryDestination accessor the other tests use
    assert sorted(r["canonical_name"] for r in rows) == ["brand", "summer sale", "summer sale"]
    assert {r["source_key"] for r in rows} == {"fb_like", "tt_like"}


def test_leg_without_data_is_skipped_not_fatal() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["x"])(destinations=[memory])
    tt = _connector("tt_like", ["y"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    tt.campaigns.materializable = False                        # nothing written for tt: its leg reads as None
    il.DAG(fb, tt, matcher).materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
    rows = memory.read_rows(matcher.campaign_matches)
    assert [r["source_key"] for r in rows] == ["fb_like"]


def test_matcher_alone_reads_bound_upstreams_read_only() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["x"])(destinations=[memory])
    il.DAG(fb).materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
    matcher = CampaignMatcher(destinations=[memory])
    matcher.campaign_matches.bind("campaigns", fb.campaigns)
    dag = il.DAG(matcher)
    assert dag.operation_map[fb.campaigns.id].materializable is False
    dag.materialize(partition=il.TimePartition(dt.date(2026, 9, 1)))
    assert len(memory.read_rows(matcher.campaign_matches)) == 1


def test_matcher_with_no_campaigns_in_dag_is_a_build_error() -> None:
    with pytest.raises(il.ConfigError, match="campaigns"):
        il.DAG(CampaignMatcher(destinations=[il.MemoryDestination()]))
```

Use the `MemoryDestination` read helper the existing tests use (grep `MemoryDestination` in `packages/interloper-core/tests/destination`); the name `read_rows` above is a placeholder for that helper.

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen pytest packages/interloper-assets/tests/test_campaign_matcher.py -q`
Expected: FAIL (`CampaignMatcher` has no `campaign_matches`).

- [ ] **Step 3: Implement**

```python
# campaign_matcher/source.py
"""Campaign matcher: canonical campaign names across every advertising source of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

from interloper_assets.campaign_matcher import schemas


def _match(row: dict[str, Any]) -> dict[str, Any]:
    name = str(row.get("name", ""))
    return {"campaign_id": str(row.get("id", "")), "campaign_name": name, "canonical_name": name.strip().lower(), "similarity": 1.0}


@il.source(tags=["Analytics"], icon="carbon:connect")
class CampaignMatcher(il.Source):
    """Matches campaigns across sources into one canonical lookup table (matching logic is a placeholder)."""

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
        """One row per upstream campaign, stamped with the leg's source and the run's date."""
        rows: list[dict[str, Any]] = []
        for leg in campaigns:
            if leg.data is None:
                context.logger.warning("No campaigns for %s in this partition; leg skipped", leg.asset.qualified_key)
                continue
            source = leg.asset.parent
            for row in il.Representation.rows(leg.data):        # the helper that turns a frame or list into dicts
                rows.append({
                    "date": context.partition_date,
                    "source_key": source.key if source else "",
                    "source_id": source.id if source else "",
                    **_match(row),
                })
        return rows
```

Register the class next to `demo` in the assets package registry and entry points.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest packages/interloper-assets/tests/test_campaign_matcher.py packages/interloper-core/tests/catalog -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-assets
git commit -m "feat(assets): campaign matcher source with a many-valued wildcard upstream over every campaigns asset

By Digitl"
```

---

### Task 2: Manifest and docs

**Files:**
- Create: `examples/campaign_matcher.yaml` (the spec section 7 manifest, adapted to two connectors present in `interloper-assets` that have a `campaigns` asset, with `${...}` credentials and `materializable: false` on the connectors)
- Modify: `docs/guide/dependencies.md` (a "Fan-in across sources" subsection pointing at the matcher), `docs/catalog/` page for the source if the docs list sources per package
- Test: `uv run --frozen interloper run -f examples/campaign_matcher.yaml --dry-run` if a dry-run flag exists, else `python -c "import interloper as il; il.DAG.from_spec_file('examples/campaign_matcher.yaml')"` with fake credentials in the environment (the DAG builds; nothing is fetched)

- [ ] **Step 1: Write the manifest and the docs section**, **Step 2: Build the DAG from the file and assert two predecessors on `campaign_matches`**, **Step 3: Commit** `docs: campaign matcher manifest and fan-in guide section`.

---

### Task 3: Platform run (requires phase 2 merged)

- [ ] **Step 1: Stand up a dev instance on a non-3000 port** (`INTERLOPER_SERVER_PORT=3100 make dev-up`), create two `demo`-like sources that expose `campaigns` if the seed has none (or two `facebook_ads` sources with dummy credentials whose `campaigns` will fail to fetch: the matcher must still run and report skipped legs), create a `campaign_matcher` source, and check `GET /components/relations?src_kind=asset&dst_kind=asset` lists the `campaigns` rows after the collection wizard binds them.
- [ ] **Step 2: Run the matcher through a job** and confirm the run's events show one warning per empty leg and a successful materialisation with zero or more rows.
- [ ] **Step 3: Record the outcome in the ledger** and open the PR `feat: campaign matcher source` when Guillaume asks.
