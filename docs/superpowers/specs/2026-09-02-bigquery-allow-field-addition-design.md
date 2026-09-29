# BigQuery destination: allow field addition under `RECONCILE`

Date: 2026-09-02
Package: `interloper-google-cloud` (`interloper_google_cloud.bigquery.destination`)

## Problem

`BigQueryDestination._insert_data` creates a missing table from the asset schema, syncs descriptions on
an existing one, then appends with a load job (`WRITE_APPEND`) whose `schema` is the asset schema.
Neither step can add a column to an existing table, so the first write after a schema gains a field
fails with BigQuery's "Provided Schema does not match Table … Cannot add fields". This blocks releasing
the widened Facebook `AdsStats` (75 → 200 fields) to organisations whose `ads_stats__*` tables exist.

## Change

Under the `RECONCILE` materialization strategy, and only when a declared schema drives the load, both
load jobs set `schema_update_options = [ALLOW_FIELD_ADDITION]`. BigQuery then appends the declared
fields the table lacks as part of the load.

- Gate: `self.materialization_strategy is MaterializationStrategy.RECONCILE` **and** `bq_schema is not
  None`. `AUTO` and `STRICT` keep today's behaviour (a missing column still fails loudly), and the
  schema-less paths (`_insert`, DataFrame autodetect) never let BigQuery grow a table from inferred
  columns.
- Applies to `_load_dataframe` (Parquet) and `_load_rows` (JSON), through one small private helper so
  the two paths cannot drift.
- Existing columns are never altered or dropped; `ALLOW_FIELD_RELAXATION` is not set.
- Under `RECONCILE` the conformer has already filled every schema column, so all missing columns are
  added on the first write rather than batch by batch.

## Tests (`tests/test_bigquery.py`)

1. `RECONCILE` (the destination default) + schema: the DataFrame load job carries
   `[ALLOW_FIELD_ADDITION]`; the JSON rows load job carries it too.
2. `AUTO` + schema: no `schema_update_options` on either load job.
3. `RECONCILE` without a schema (`_insert` path): no `schema_update_options`.

## Out of scope

`STRICT` semantics, dropping or relaxing columns, and adding columns through `update_table`
(considered as an alternative; not chosen).
