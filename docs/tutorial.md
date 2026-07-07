# Tutorial

## Your first asset

At the most basic level, an `asset` is simply a function that returns data.
Any kind of data, whether it's a string, a list of dictionaries, a Pandas dataframe, etc.

Let's create our first asset that pulls today's Berlin weather forecast data from [Open Meteo](https://open-meteo.com/):

```py
import datetime as dt
import httpx
import interloper as il

@il.asset
def forecast():
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": dt.date.today().isoformat(),
        "end_date": dt.date.today().isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = httpx.get(url, params=params)
    data = response.json()["hourly"]
    return data
```

We can execute our asset by instantiating it and calling `run()` — a plain synchronous call
that works in scripts, the REPL, and notebooks alike (the async-native engine runs on a
background event loop; async code awaits `run_async()` instead):

```py
result = forecast().run()
```

!!! note

    The `@il.asset` decorator returns an asset **definition** (a class). Calling it (e.g.
    `forecast()`) creates a runtime `Asset` instance. This separation between definition and
    instance is a core concept in Interloper.

Synchronous asset functions like the one above run on a worker thread automatically, so they
never block the event loop. If you prefer, you can write the asset as a coroutine and
Interloper will await it natively:

```py
@il.asset
async def forecast():
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
    return response.json()["hourly"]
```

## Adding context

Assets that need access to partition information or a logger can request an `ExecutionContext`:

```py
@il.asset
def forecast(context: il.ExecutionContext):
    context.logger.info("Fetching forecast data...")
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": dt.date.today().isoformat(),
        "end_date": dt.date.today().isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = httpx.get(url, params=params)
    return response.json()["hourly"]
```

## Materialization

Interloper starts to make sense when we introduce the concept of **materialization**.

An asset can be **materialized**, meaning that based on a `Destination` configuration, its result is **written to** that destination.

Interloper ships with built-in destinations and additional ones via extension packages.

Let's materialize our asset using `FileDestination`, which pickles the data on the filesystem:

```py
forecast_asset = forecast(destinations=il.FileDestination("./data"))
forecast_asset.materialize()
```

The materialization handles the execution of the asset and saves the data as a pickle file under `./data/forecast/data.pkl`.

## Defining a source

Pulling data from a data source isn't typically limited to a single asset. We might fetch data from several API endpoints while reusing common configuration or a shared HTTP client.

Interloper allows you to define `sources`, which group related assets together. A source is a
**function** decorated with `@il.source` that returns the list of assets it contains:

```py
@il.source
def open_meteo():
    @il.asset
    def forecast():
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 52.5244,
            "longitude": 13.4105,
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
        }
        response = httpx.get(url, params=params)
        return response.json()["hourly"]

    @il.asset
    def air_quality():
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": 52.5244,
            "longitude": 13.4105,
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["pm10", "pm2_5", "dust", "uv_index"],
        }
        response = httpx.get(url, params=params)
        return response.json()["hourly"]

    return [forecast, air_quality]
```

Instantiate the source and access individual assets by name:

```py
source = open_meteo(destinations=il.FileDestination("./data"))
source.forecast.run()
source.air_quality.materialize()
```

## DAG

When you want to materialize multiple assets together, respecting their dependencies, use a `DAG`:

```py
source = open_meteo(destinations=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize()
```

`dag.materialize()` runs the DAG with the default `AsyncRunner`. For more control, use a runner
directly — runners are async-native, so drive them with `il.run` from sync code:

```py
result = il.run(il.SerialRunner().run(dag))

print(result.status)           # ExecutionStatus.COMPLETED
print(result.execution_time)   # Total time in seconds
```

## Configuration

Sources often need external configuration like base URLs or feature flags. Define a `Config`,
which extends [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
for environment-based resolution, and **inject it into your asset functions** by type
annotation:

```py
class OpenMeteoConfig(il.Config):
    latitude: float = 52.5244
    longitude: float = 13.4105

@il.source
def open_meteo():
    @il.asset
    def forecast(config: OpenMeteoConfig):
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "latitude": config.latitude,
            "longitude": config.longitude,
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["temperature_2m"],
        }
        response = httpx.get(url, params=params)
        return response.json()["hourly"]

    return [forecast]
```

The config is automatically resolved from environment variables when the asset runs. You can
also provide it explicitly via the `resources` mapping:

```py
source = open_meteo()
configured = source.forecast(resources={"config": OpenMeteoConfig(latitude=48.8566, longitude=2.3522)})
```

For service **credentials** (API keys, OAuth tokens), use a [Connection](features/connections.md)
instead of a plain `Config` — same injection mechanism, plus secret handling and an OAuth
connect flow.

## Partitioning

Many data pipelines need to process data by date. Interloper supports time-based partitioning:

```py
@il.source
def open_meteo():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def forecast(context: il.ExecutionContext):
        date = context.partition_date
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 52.5244,
            "longitude": 13.4105,
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "hourly": ["temperature_2m"],
        }
        response = httpx.get(url, params=params)
        return response.json()["hourly"]

    return [forecast]
```

Materialize for a specific date:

```py
source = open_meteo(destinations=il.FileDestination("./data"))
dag = il.DAG(source)
dag.materialize(partition_or_window=il.TimePartition(dt.date.today()))
```

## Backfilling

To process a range of dates, pass a `TimePartitionWindow` to a runner. The runner iterates the
window and runs the DAG once per date:

```py
import datetime as dt

window = il.TimePartitionWindow(
    start=dt.date(2025, 1, 1),
    end=dt.date(2025, 1, 7),
)
il.run(il.AsyncRunner().run(dag, window))
```

There is no separate "backfiller" object — backfilling is just running a DAG over a window,
and every runner (serial, async, multi-process, Docker, Kubernetes) supports it.

## Next steps

- [Assets & Sources](features/assets-sources.md) -- Detailed definition patterns
- [Destinations](features/destinations.md) -- All available destinations
- [Runners](features/runners.md) -- Execution strategies
- [Schema & Normalizer](features/schema.md) -- Data normalization and validation
- [Connections](features/connections.md) -- Credentials and the OAuth connect flow
- [Partitioning](features/partitioning.md) -- Time-based partitioning
- [Backfilling](features/backfilling.md) -- Historical data processing
- [Configuration](features/config.md) -- Environment-based config
- [Events](features/events.md) -- Lifecycle event system
- [CLI & Manifests](features/cli.md) -- Command-line interface and declarative runs
