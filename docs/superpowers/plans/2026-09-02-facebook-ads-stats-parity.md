# Facebook Ads Stats Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Facebook `AdsStats` schema declare the same action-type metrics as `CampaignsStats`, and stop both schemas from dropping three scalars their assets already request.

**Architecture:** Schema-only change in `interloper-assets`. Facebook insights arrive with `actions`-style lists that `FacebookActionsNormalizer` pivots into `<family>_<action_type>` columns; the pydantic `Schema` subclass is the allowlist the `RECONCILE` materialization keeps. Adding fields to the schema is therefore all that is needed for the already-fetched columns to reach the destination. Two regression tests pin the parity and guard requested-but-undeclared fields.

**Tech Stack:** Python ≥3.10, pydantic v2 (`Schema.model_fields`), pytest, ruff (line length 120), ty. Run everything from the repo root of this worktree with `uv run`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-09-02-facebook-ads-stats-parity-design.md`.
- Do NOT commit. Guillaume commits himself; finished work rests uncommitted in the working tree. Check `git status` after editing to confirm edits landed in this worktree (`.claude/worktrees/timeline-page-job-execution-43975c`), not the main checkout.
- Schema files keep fields in alphabetical order; every field is `<type> | None = Field(default=None, description="...")`.
- Types and descriptions of copied action columns are taken verbatim from `CampaignsStats`. No source, constants or normalizer edits.
- Do not change `account_id` types (str on ads, int on campaigns).
- Never `cd` into the main repository; all paths below are relative to the worktree root.

---

### Task 1: Rename the worktree branch

**Files:** none (git metadata only)

- [ ] **Step 1: Confirm the current branch and a clean tree**

Run: `git status --short && git branch --show-current`
Expected: only `?? docs/superpowers/` untracked; branch `claude/bigquery-migration-analysis-97b0cf`.

- [ ] **Step 2: Rename the branch (directory stays where it is)**

Run: `git branch -m claude/bigquery-migration-analysis-97b0cf feat/facebook-ads-stats-parity && git branch --show-current`
Expected: `feat/facebook-ads-stats-parity`.

---

### Task 2: Regression tests for schema parity and completeness

**Files:**
- Modify: `packages/interloper-assets/tests/test_facebook_ads.py` (imports at lines 22-23; new class appended after `TestSpecRoundtripAndReconcile`, before `test_isinstance_of_dataframe_normalizer`; docstring of `test_sparse_row_passes_validation` at lines 104-108)

**Interfaces:**
- Consumes: `interloper_assets.facebook_ads.schemas.AdsStats` / `.CampaignsStats` (pydantic models; `.model_fields` maps name → `FieldInfo` with `.annotation`), `interloper_assets.facebook_ads.constants.ADS_INSIGHT_FIELDS` (list[str]), `interloper_assets.facebook_ads.source.PIVOT_COLUMNS` (list[str]).
- Produces: `TestSchemaParity` with two tests that Task 3 must turn green.

- [ ] **Step 1: Extend the imports**

Replace lines 22-23:

```python
from interloper_assets.facebook_ads import schemas
from interloper_assets.facebook_ads.source import FacebookActionsNormalizer, FacebookAds
```

with:

```python
from interloper_assets.facebook_ads import constants, schemas
from interloper_assets.facebook_ads.source import PIVOT_COLUMNS, FacebookActionsNormalizer, FacebookAds
```

- [ ] **Step 2: Add the failing tests**

Insert this class directly above `def test_isinstance_of_dataframe_normalizer():`:

```python
class TestSchemaParity:
    """Both stats assets fetch the same action arrays; the schemas must not drop them unevenly."""

    _ACTION_FAMILIES = (
        "actions_",
        "action_values_",
        "cost_per_action_type_",
        "cost_per_unique_action_type_",
        "unique_actions_",
    )

    def test_ads_stats_declares_every_campaign_action_column(self):
        campaigns = schemas.CampaignsStats.model_fields
        ads = schemas.AdsStats.model_fields
        expected = {name: info for name, info in campaigns.items() if name.startswith(self._ACTION_FAMILIES)}

        missing = sorted(set(expected) - set(ads))
        assert not missing, f"AdsStats lacks {len(missing)} campaign action columns: {missing}"

        mismatched = {
            name: (str(info.annotation), str(ads[name].annotation))
            for name, info in expected.items()
            if info.annotation != ads[name].annotation
        }
        assert not mismatched, mismatched

    def test_ads_stats_declares_every_requested_scalar(self):
        # Breakdown dimensions come back as columns too, though they are not in the fields list.
        breakdowns = {"publisher_platform", "platform_position", "impression_device"}
        requested = {field for field in constants.ADS_INSIGHT_FIELDS if field not in PIVOT_COLUMNS} | breakdowns

        missing = sorted(requested - set(schemas.AdsStats.model_fields))
        assert not missing, f"requested but undeclared on AdsStats (silently dropped): {missing}"

    def test_campaigns_stats_declares_the_shared_scalars(self):
        for name in ("attribution_setting", "quality_ranking", "converted_product_quantity"):
            assert name in schemas.CampaignsStats.model_fields, name
```

- [ ] **Step 3: Update the stale count in the sparse-row docstring**

In `test_sparse_row_passes_validation`, change

```python
        Only a few of the 75 fields arrive, and every schema field is
```

to

```python
        Only a few of the 200 fields arrive, and every schema field is
```

- [ ] **Step 4: Run the new tests and confirm they fail for the right reason**

Run: `uv run pytest packages/interloper-assets/tests/test_facebook_ads.py::TestSchemaParity -v`
Expected: 3 FAILED. The first assertion message lists 122 missing columns (starting with `action_values_add_payment_info`); the second lists `['attribution_setting', 'converted_product_quantity', 'quality_ranking']`; the third fails on `attribution_setting`.

- [ ] **Step 5: Confirm the rest of the file still passes**

Run: `uv run pytest packages/interloper-assets/tests/test_facebook_ads.py -v -k "not TestSchemaParity"`
Expected: all PASS.

---

### Task 3: Extend `AdsStats` and `CampaignsStats`

**Files:**
- Modify: `packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/ads_stats.py` (75 → 200 fields)
- Modify: `packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/campaigns_stats.py` (193 → 196 fields)
- Scratch (not in repo): `$SCRATCH/extend_schemas.py`, where `$SCRATCH` is the session scratchpad directory

**Interfaces:**
- Consumes: the two schema files as they are; field lines match `^    <name>: <type> | None = Field(default=None, description="...")$` and may wrap onto continuation lines up to the closing `)`.
- Produces: the same two classes with additional fields, alphabetical, so `TestSchemaParity` passes.

- [ ] **Step 1: Write the one-off generator to the scratchpad**

The three scalars are hand-written here; every action column is copied from `CampaignsStats` verbatim.

```python
"""One-off: copy campaign action columns into AdsStats, add shared scalars to both. Not part of the repo."""

import pathlib
import re

ROOT = pathlib.Path("packages/interloper-assets/src/interloper_assets/facebook_ads/schemas")
FAMILIES = ("actions_", "action_values_", "cost_per_action_type_", "cost_per_unique_action_type_", "unique_actions_")
FIELD_START = re.compile(r"^    ([a-z0-9_]+): ")

SCALARS = {
    "attribution_setting": (
        '    attribution_setting: str | None = Field(\n'
        '        default=None, description="The attribution setting applied to the conversion metrics"\n'
        "    )"
    ),
    "converted_product_quantity": (
        '    converted_product_quantity: float | None = Field(\n'
        '        default=None, description="Number of products purchased as a result of the ad"\n'
        "    )"
    ),
    "quality_ranking": (
        '    quality_ranking: str | None = Field(\n'
        '        default=None, description="Ranking of the ad\'s perceived quality against ads competing for the same audience"\n'
        "    )"
    ),
}


def split(path: pathlib.Path) -> tuple[str, dict[str, str]]:
    """Return (header up to the first field, {field name: field block})."""
    lines = path.read_text().splitlines()
    first = next(i for i, line in enumerate(lines) if FIELD_START.match(line))
    header = "\n".join(lines[:first])
    blocks: dict[str, str] = {}
    name = None
    for line in lines[first:]:
        if m := FIELD_START.match(line):
            name = m.group(1)
            blocks[name] = line
        elif name is not None and line.strip():
            blocks[name] += "\n" + line
    return header, blocks


def write(path: pathlib.Path, header: str, blocks: dict[str, str]) -> None:
    body = "\n".join(blocks[name] for name in sorted(blocks))
    path.write_text(f"{header}\n{body}\n")


camp_header, camp = split(ROOT / "campaigns_stats.py")
ads_header, ads = split(ROOT / "ads_stats.py")

added = {name: block for name, block in camp.items() if name.startswith(FAMILIES) and name not in ads}
assert len(added) == 122, len(added)
ads.update(added)
ads.update(SCALARS)
write(ROOT / "ads_stats.py", ads_header, ads)

camp.update(SCALARS)
write(ROOT / "campaigns_stats.py", camp_header, camp)
print(f"AdsStats fields: {len(ads)}  CampaignsStats fields: {len(camp)}")
```

- [ ] **Step 2: Run the generator from the worktree root**

Run: `uv run python "$SCRATCH/extend_schemas.py"`
Expected: `AdsStats fields: 200  CampaignsStats fields: 196`.

- [ ] **Step 3: Let ruff normalise line wrapping, then lint**

Run: `uv run ruff format packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/ && uv run ruff check packages/interloper-assets`
Expected: two files reformatted (or left unchanged), `All checks passed!`.

- [ ] **Step 4: Eyeball the diff for shape, not content**

Run: `git diff --stat && git diff packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/ads_stats.py | grep '^[-+]    [a-z]' | grep -c '^-'`
Expected: `ads_stats.py` roughly +125 lines, `campaigns_stats.py` +3; the count of removed field lines is `0` (nothing existing was dropped or altered).

- [ ] **Step 5: Run the parity tests**

Run: `uv run pytest packages/interloper-assets/tests/test_facebook_ads.py -v`
Expected: every test PASS, including the three in `TestSchemaParity`.

- [ ] **Step 6: Confirm the edits landed in this worktree**

Run: `git status --short`
Expected:

```
 M packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/ads_stats.py
 M packages/interloper-assets/src/interloper_assets/facebook_ads/schemas/campaigns_stats.py
 M packages/interloper-assets/tests/test_facebook_ads.py
?? docs/superpowers/
```

If the schema files show as unmodified here, the edit went to the main checkout; re-run the generator from the worktree root.

---

### Task 4: Whole-package verification

**Files:** none new

- [ ] **Step 1: Type check the package**

Run: `uv run ty check packages/interloper-assets`
Expected: the same diagnostics count as on the untouched tree. Run the command once before starting Task 3 and note the count; it must not grow.

- [ ] **Step 2: Run the assets test suite**

Run: `uv run pytest packages/interloper-assets -q`
Expected: all pass; no test other than the ones added references the Facebook field count.

- [ ] **Step 3: Sanity-check the BigQuery field mapping of the new schema**

Run:

```bash
uv run python -c "
from interloper_google_cloud.bigquery.destination import _schema_to_bq_fields
from interloper_assets.facebook_ads import schemas
f = _schema_to_bq_fields(schemas.AdsStats)
print(len(f), sorted({x.field_type for x in f}))
print([x.name for x in f if x.name.startswith('actions_omni')])"
```

Expected: `200 ['DATE', 'FLOAT64', 'INT64', 'STRING']` (or `FLOAT`/`INTEGER` spellings, whichever the mapper emits) and the `actions_omni_*` names present.

- [ ] **Step 4: Report**

Leave everything uncommitted. Report the final `git status --short`, the pytest summary line, and the reminder from the spec: do not release before the BigQuery destination can add columns to existing tables.
