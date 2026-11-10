# interloper-google-cloud

Google Cloud integration for the Interloper framework. Provides BigQuery, Cloud Storage and Google Sheets destinations and the Google Cloud connection resource.

## Google Sheets

`GoogleSheetsDestination` writes assets into one spreadsheet, one tab per asset. It authenticates with the
`GoogleCloudConnection`'s service-account key, or with ambient credentials when the key is empty.

### Setup

1. Create the spreadsheet and copy its id from the URL
   (`https://docs.google.com/spreadsheets/d/<id>/edit`).
2. Share it with the service account's email address (`client_email` in the key) as an **editor**. The
   service account sees no spreadsheet it was not shared on.

### Usage

```python
from interloper_google_cloud import GoogleCloudConnection, GoogleSheetsDestination

destination = GoogleSheetsDestination(
    connection=GoogleCloudConnection(service_account_key="..."),
    spreadsheet_id="1AbC...",
)
```

### Layout

- Each asset is a tab titled after its table, or `{dataset}.{table}` when the asset has a dataset (at most 100
  characters, Sheets' limit).
- The first row is the header, written when the tab is created from the asset's schema. Later writes are
  aligned to it: columns missing from the data are left empty, columns missing from the header are dropped with
  a warning. The header is never altered.
- Rows carry the partition column. Replacing a partition deletes its rows, then appends the new ones; Sheets
  has no transactions, so the pair is not atomic.

### Types

Values are written raw: numbers and booleans as is, `None` as an empty cell, decimals as strings, dates and
datetimes as ISO 8601 strings, dicts and lists as JSON. Time partitions are matched by comparing those ISO
strings, which orders chronologically, so a time-partitioned asset's partition column must be a date or a
datetime (or an ISO string). On read, empty cells come back as `None` and rows are reconciled against the
asset's schema, restoring the declared types.

### Limits

Sheets caps a spreadsheet at 10 million cells, and replacing a partition reads the whole tab and rewrites it. The
destination suits small reporting tables that people open in Sheets, not a warehouse.
