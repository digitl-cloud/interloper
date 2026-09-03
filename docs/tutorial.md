# Tutorial

This tutorial builds a small but complete source against the public
[Open Meteo](https://open-meteo.com/) API. Each step adds one concept: an asset, a source with
configuration, a connection carrying an HTTP client, a dependency, a schema, partitioning, and
finally running from the CLI. The finished code is at the end.

## 1. An asset

An asset is a function returning data. Here it fetches yesterday's hourly forecast for Berlin:

```py
import datetime as dt

import httpx
import interloper as il

URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

@il.asset
def forecast() -> list[dict]:
    date = dt.date.today() - dt.timedelta(days=1)
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": date.isoformat(),
        "end_date": date.isoformat(),
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
    }
    payload = httpx.get(URL, params=params).json()["hourly"]
    return [dict(zip(payload, values)) for values in zip(*payload.values())]
```

Run it:

```py
rows = forecast().run()
```

`forecast` is a definition (a class); `forecast()` is an instance. The distinction matters as
soon as you configure instances differently, which the next steps do.

Sync functions run on a worker thread, so they never block the engine. An `async def` asset is
awaited natively:

```py
@il.asset
async def forecast() -> list[dict]:
    async with httpx.AsyncClient() as client:
        response = await client.get(URL, params=params)
    ...
```

## 2. A source with configuration

Coordinates are configuration, not code. Move the asset into a source and declare the
coordinates as fields:

```py
@il.source(tags=["Weather"])
class OpenMeteo(il.Source):
    """Hourly weather from Open Meteo."""

    latitude: float = il.InputField(default=52.52, description="Latitude of the location")
    longitude: float = il.InputField(default=13.41, description="Longitude of the location")

    @il.asset
    def forecast(self) -> list[dict]:
        date = dt.date.today() - dt.timedelta(days=1)
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
        }
        payload = httpx.get(URL, params=params).json()["hourly"]
        return [dict(zip(payload, values)) for values in zip(*payload.values())]
```

The asset is now a method: `self` is the source instance, so it reads the configured
coordinates. The `InputField` helper is a pydantic `Field` that also tells a UI how to render the
field; the default alone would work too.

```py
berlin = OpenMeteo()
paris = OpenMeteo(latitude=48.86, longitude=2.35)
paris.forecast.run()
```

## 3. Materialize

Give the source a destination and materialize. Every asset without its own destination inherits
the source's:

```py
source = OpenMeteo(destinations=il.CSVDestination(base_path="./data"))
source.forecast.materialize()
# ./data/open_meteo/forecast/data.csv
```

The path is `{base_path}/{dataset}/{table}/data.csv`. The dataset defaults to the source key
(`open_meteo`), the table to the asset key.

## 4. A connection with an HTTP client

Open Meteo needs no credentials, but most APIs do, and a client is worth sharing between
assets. That is what a **connection** is for: a resource holding credentials and exposing a
client. Here is one for a hypothetical authenticated weather API:

```py
from functools import cached_property

@il.connection(name="Weather API")
class WeatherConnection(il.Connection):
    api_key: str = il.SecretField(description="API key")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient("https://api.example.com", auth=il.HTTPBearerAuth(self.api_key))
```

Inject it into an asset by annotating a parameter with the connection type:

```py
@il.source
class Weather(il.Source):
    @il.asset
    def stations(self, connection: WeatherConnection) -> list[dict]:
        return connection.client.get("/stations").json()
```

Resolution is a cascade: an instance passed on the asset, then one passed on the source, then
one built from the environment (`API_KEY=...`). For paginated endpoints the client has a
`paginate()` method with pluggable paginators; see [Connections](guide/connections.md).

## 5. A dependency

Add an asset that consumes another. Naming a parameter after a sibling asset declares the
dependency:

```py
@il.source
class OpenMeteo(il.Source):
    ...

    @il.asset
    def forecast(self) -> list[dict]:
        ...

    @il.asset
    def daily_summary(self, forecast: list[dict]) -> list[dict]:
        temperatures = [row["temperature_2m"] for row in forecast]
        return [{"min": min(temperatures), "max": max(temperatures)}]
```

When the DAG runs, `forecast` is materialized first, then read back from its destination and
passed to `daily_summary`. Run the whole source:

```py
dag = il.DAG(source)
dag.materialize()
```

## 6. A schema

Declare what `forecast` returns. With a schema the data is reconciled to it on every
materialization: columns aligned, values coerced, mismatches reported.

```py
class ForecastRow(il.Schema):
    time: dt.datetime
    temperature_2m: float | None
    precipitation: float | None
    wind_speed_10m: float | None

@il.asset(schema=ForecastRow)
def forecast(self) -> list[dict]:
    ...
```

Without a schema, one is inferred from the data so that destinations still know the column
types. How strictly the schema is enforced is the asset's `materialization_strategy`; see
[Schemas and data contracts](guide/schema.md).

## 7. Partitioning

A daily fetch should be driven by the run, not by `dt.date.today()`. Declare time partitioning
and read the partition from the context:

```py
@il.asset(schema=ForecastRow, partitioning=il.TimePartitionConfig(column="date"))
def forecast(self, context: il.ExecutionContext) -> list[dict]:
    date = context.partition_date
    params = {
        "latitude": self.latitude,
        "longitude": self.longitude,
        "start_date": date.isoformat(),
        "end_date": date.isoformat(),
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
    }
    payload = httpx.get(URL, params=params).json()["hourly"]
    rows = [dict(zip(payload, values)) for values in zip(*payload.values())]
    for row in rows:
        row["date"] = date
    return rows
```

The schema gains a `date: dt.date` column, the partition column. A partitioned asset must be
run for a partition:

```py
dag.materialize(il.TimePartition(dt.date(2026, 1, 15)))
# ./data/open_meteo/forecast/date=2026-01-15/data.csv
```

Backfilling a range is a loop over a window, newest first:

```py
window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 7))
for partition in window:
    dag.materialize(partition)
```

`daily_summary` depends on a partitioned asset, so it must be partitioned too; the DAG rejects a
non-partitioned asset downstream of a partitioned one.

## 8. Runners

`dag.materialize()` uses an `AsyncRunner` with four concurrent slots. Runners are async-native;
from sync code, drive them with `il.run`:

```py
from interloper.events import ConsoleEventHandler

runner = il.AsyncRunner(max_workers=8, fail_fast=False, on_event=ConsoleEventHandler())
result = il.run(runner.run(dag, il.TimePartition(dt.date(2026, 1, 15))))
print(result.status, result.failed_ids)
```

`on_event` streams the run's lifecycle events through the logging stack. See
[Execution](guide/execution.md) and [Events and logging](guide/events.md).

## 9. The CLI

With the source saved in `weather.py`, inspect the plan without writing a driver script. The
module must be importable, so either install your package or put its directory on the path:

```sh
PYTHONPATH=. interloper run weather.OpenMeteo --date 2026-01-15 --dry-run
```

Import paths instantiate the class with its defaults, which here means no destinations, and
`daily_summary` needs `forecast` to have been written somewhere. Destinations and configuration
for a CLI run live in a **spec file**, a YAML document reconstructing a job, source or asset:

```yaml
# weather.yaml
path: weather.OpenMeteo
init:
  latitude: 48.86
  longitude: 2.35
  destinations:
    - path: interloper.destination.csv.CSVDestination
      init: { base_path: ./data }
```

```sh
PYTHONPATH=. interloper run -f weather.yaml --date 2026-01-15
```

The CLI also takes `--start-date` and `--end-date`, but that is a **single run** covering the
whole window, which only assets declaring `allow_window=True` accept. For one run per day, loop
as in step 7, or declare the window support described in [Partitioning](guide/partitioning.md#windowed-assets).

The runner comes from `interloper.yaml` or `INTERLOPER_RUNNER_*` variables. See
[CLI](guide/cli.md) and [Specs and serialization](guide/specs.md).

## The finished source

```py
import datetime as dt

import httpx
import interloper as il

URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
PARTITIONING = il.TimePartitionConfig(column="date")


class ForecastRow(il.Schema):
    date: dt.date
    time: dt.datetime
    temperature_2m: float | None
    precipitation: float | None
    wind_speed_10m: float | None


class DailySummary(il.Schema):
    date: dt.date
    min: float
    max: float


@il.source(tags=["Weather"])
class OpenMeteo(il.Source):
    """Hourly weather from Open Meteo."""

    latitude: float = il.InputField(default=52.52, description="Latitude of the location")
    longitude: float = il.InputField(default=13.41, description="Longitude of the location")

    @il.asset(schema=ForecastRow, partitioning=PARTITIONING, tags=["Report"])
    def forecast(self, context: il.ExecutionContext) -> list[dict]:
        date = context.partition_date
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
        }
        payload = httpx.get(URL, params=params).json()["hourly"]
        rows = [dict(zip(payload, values)) for values in zip(*payload.values())]
        for row in rows:
            row["date"] = date
        return rows

    @il.asset(schema=DailySummary, partitioning=PARTITIONING, tags=["Report"])
    def daily_summary(self, context: il.ExecutionContext, forecast: list[dict]) -> list[dict]:
        temperatures = [row["temperature_2m"] for row in forecast if row["temperature_2m"] is not None]
        return [{"date": context.partition_date, "min": min(temperatures), "max": max(temperatures)}]


if __name__ == "__main__":
    source = OpenMeteo(destinations=il.CSVDestination(base_path="./data"))
    result = il.DAG(source).materialize(il.TimePartition(dt.date(2026, 1, 15)))
    print(result)
```
