# BigQuery ALLOW_FIELD_ADDITION Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let BigQuery load jobs add declared columns that an existing table lacks, only under the `RECONCILE` strategy and only when a schema drives the load.

**Architecture:** One private helper on `BigQueryDestination` decides the `schema_update_options` for a load; both `_load_dataframe` and `_load_rows` apply it to their `LoadJobConfig`. No change to table creation or metadata sync.

**Tech Stack:** Python ≥3.10, google-cloud-bigquery (`bigquery.SchemaUpdateOption`), pytest with the file's `_make_destination` MagicMock helper, ruff, ty. Run from the worktree root with `uv run`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-09-02-bigquery-allow-field-addition-design.md`.
- Do NOT commit; work rests uncommitted. Confirm with `git status` that edits landed in this worktree.
- Docstrings: full Google sections on every function, including private ones.
- Comments only for a non-obvious why.

---

### Task 1: Failing tests

**Files:**
- Modify: `packages/interloper-google-cloud/tests/test_bigquery.py` (append a class after `TestInsertData`, i.e. before `class TestTimePartitioning`)

**Interfaces:**
- Consumes: `_make_destination(dataset=...)`, `_ctx(asset, schema)`, `_plain_asset()`, `_RowSchema` already defined in the file; `MaterializationStrategy` from `interloper.normalizer`.
- Produces: `TestSchemaUpdateOptions` that Task 2 turns green.

- [ ] **Step 1: Add the import** next to the other `interloper` imports at the top of the file:

```python
from interloper.normalizer import MaterializationStrategy
```

- [ ] **Step 2: Insert the tests** directly above `class TestTimePartitioning:`

```python
class TestSchemaUpdateOptions:
    """Load jobs may add declared columns only under RECONCILE and only with a schema."""

    def _strategy(self, dest: Any, strategy: MaterializationStrategy) -> None:
        object.__setattr__(dest, "materialization_strategy", strategy)

    def test_reconcile_dataframe_load_allows_field_addition(self):
        import pandas as pd

        dest, mock_client = _make_destination(dataset="ds")
        assert dest.materialization_strategy is MaterializationStrategy.RECONCILE  # the BigQuery default
        df = pd.DataFrame([{"id": 1, "cost": 1.0, "day": datetime.date(2024, 1, 1)}])

        dest._insert_data("tbl", "ds", df, _ctx(_plain_asset(), _RowSchema))

        job_config = mock_client.load_table_from_dataframe.call_args.kwargs["job_config"]
        assert job_config.schema_update_options == [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    def test_reconcile_rows_load_allows_field_addition(self):
        dest, mock_client = _make_destination(dataset="ds")

        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_plain_asset(), _RowSchema))

        job_config = mock_client.load_table_from_json.call_args.kwargs["job_config"]
        assert job_config.schema_update_options == [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    def test_auto_never_allows_field_addition(self):
        import pandas as pd

        dest, mock_client = _make_destination(dataset="ds")
        self._strategy(dest, MaterializationStrategy.AUTO)
        df = pd.DataFrame([{"id": 1, "cost": 1.0, "day": datetime.date(2024, 1, 1)}])

        dest._insert_data("tbl", "ds", df, _ctx(_plain_asset(), _RowSchema))
        dest._insert_data("tbl", "ds", [{"id": 1, "cost": 1.0, "day": None}], _ctx(_plain_asset(), _RowSchema))

        assert mock_client.load_table_from_dataframe.call_args.kwargs["job_config"].schema_update_options is None
        assert mock_client.load_table_from_json.call_args.kwargs["job_config"].schema_update_options is None

    def test_reconcile_without_schema_never_allows_field_addition(self):
        dest, mock_client = _make_destination(dataset="ds")

        dest._insert("tbl", "ds", [{"id": 1}])

        job_config = mock_client.load_table_from_json.call_args.kwargs["job_config"]
        assert job_config.schema_update_options is None
```

- [ ] **Step 3: Run and confirm the two RECONCILE tests fail, the two negative tests pass**

Run: `uv run pytest packages/interloper-google-cloud/tests/test_bigquery.py::TestSchemaUpdateOptions -v`
Expected: `test_reconcile_dataframe_load_allows_field_addition` and `test_reconcile_rows_load_allows_field_addition` FAIL with `assert None == [<SchemaUpdateOption.ALLOW_FIELD_ADDITION ...>]`; the other two PASS.

---

### Task 2: Implementation

**Files:**
- Modify: `packages/interloper-google-cloud/src/interloper_google_cloud/bigquery/destination.py` (`_load_dataframe` ~line 276, `_load_rows` ~line 317; new helper placed between `_insert_data` and `_load_dataframe`)

**Interfaces:**
- Produces: `BigQueryDestination._schema_update_options(self, bq_schema: list[bigquery.SchemaField] | None) -> list[bigquery.SchemaUpdateOption] | None`.

- [ ] **Step 1: Add the helper** right after `_insert_data`:

```python
    def _schema_update_options(
        self, bq_schema: list[bigquery.SchemaField] | None
    ) -> list[bigquery.SchemaUpdateOption] | None:
        """Decide whether a load job may add declared columns the table lacks.

        Only under ``RECONCILE``, and only when a declared schema drives the
        load: the conformer has already shaped the data to that schema, so a
        column BigQuery does not know is a schema evolution, not drift.
        Schema-less loads never let inferred columns grow a table.

        Args:
            bq_schema: The load's field definitions, or ``None`` for autodetect.

        Returns:
            ``[ALLOW_FIELD_ADDITION]`` when additions are allowed, else ``None``.
        """
        if bq_schema is None or self.materialization_strategy is not MaterializationStrategy.RECONCILE:
            return None
        return [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
```

- [ ] **Step 2: Apply it in `_load_dataframe`** — after `job_config = bigquery.LoadJobConfig(write_disposition=...)` add:

```python
        job_config.schema_update_options = self._schema_update_options(bq_schema)
```

and extend the docstring's first paragraph with: `Under ``RECONCILE`` the job may add declared columns the table lacks.`

- [ ] **Step 3: Apply it in `_load_rows`** — after the `LoadJobConfig(...)` construction add the same line, and the same docstring sentence.

- [ ] **Step 4: Run the whole BigQuery test file**

Run: `uv run pytest packages/interloper-google-cloud/tests/test_bigquery.py -q`
Expected: all pass, including the four new tests.

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check packages/interloper-google-cloud && uv run ty check packages/interloper-google-cloud`
Expected: `All checks passed!` twice.

- [ ] **Step 6: Confirm placement**

Run: `git status --short`
Expected: the destination file and its test file modified in this worktree, plus the docs.
