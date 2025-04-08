<h1 align="center">Interloper Core</h1>
<h3 align="center">The ultra-portable data asset framework</h3>

## Installation
```sh
pip install interloper
```

## Quick start

```py
import interloper as itlp
from interloper_google_cloud.io import BigQueryIO
from interloper_pandas.normalizer import DataframeNormalizer
from interloper_sql.io import PostgresIO, SQLiteIO


@itlp.source
def my_source() -> tuple[itlp.Asset, ...]:
    @itlp.asset(normalizer=itlp.JSONNormalizer())
    def as_json() -> list:
        return [
            {"a": 1, "b": 2},
            {"b": 3, "c": "4"},
        ]

    @itlp.asset(normalizer=DataframeNormalizer())
    def as_dataframe() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"a": 1, "b": 2},
                {"b": 3, "c": "4"},
            ]
        )

    return (as_json, as_dataframe)


my_source = my_source(
    io={
        "file": itlp.FileIO("./data"),
        "sqlite": SQLiteIO("data/sqlite.db"),
        "postgres": PostgresIO(database="interloper", user="g", password="", host="localhost", port=5432),
        "bigquery": BigQueryIO(project="PROJECT", location="eu"),
    }
)


itlp.Pipeline(my_source).materialize()
```