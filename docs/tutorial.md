## Your first asset

At the most basic level, an `asset` is simply a function that returns data.  
Any kind of data, whether it's a string, a list of dictionaries, a Pandas dataframe, etc.

Let's create our first asset that pulls today's Berlin weather forecast data from [Open Meteo](https://open-meteo.com/):

```py
import datetime as dt
import interloper as interloper

@interloper.asset
def forecast():
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": dt.date.today().isoformat(),
        "end_date": dt.date.today().isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = client.get(url, params=params)
    data = response.json()["hourly"]
    return data
```

We can execute our asset using:
```py
forecast.run()
```

!!! note

    Calling the asset with `forecast()` does not execute it but instead returns a *parameterized* copy. With Interloper, the execution of assets and resolution of parameters is deferred. This concept will be covered in the rest of this documentation.


## Adding parameters

An asset can be parameterized like a normal function.

Let's add a parameter to our `forecast` asset.

```py hl_lines="3"
@interloper.asset
def forecast(
    date: dt.date,
):
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": date.isoformat(),
        "end_date": date.isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = client.get(url, params=params)
    data = response.json()["hourly"]
    return data
```

To execute the asset, keyword arguments are required:

```py
forecast.run(date=dt.date.today())
```


## Materialization

So far, nothing too exciting. Interloper's starts to make sense when we introduce the concept of **materialization**.

An asset can **materialized**, meaning that, based on an `IO` configuration, it can be **written to**  and **read from** somewhere.

Interloper ships with many `IO` managers out of the box allowing assets to be materialized to a wide range of destinations.

Let's add an `IO` config to our asset using `FileIO`, which simply pickles the data on the filesystem:

```py hl_lines="5"
@interloper.asset
def forecast(date: dt.date):
    [...]

forecast.io = {"file": interloper.io.FileIO("./data")}
```

We can now materialize our asset:

```py
forecast.materialize()
```

The materialization will handle the execution of the asset and save the data as a pickle file under `./data/forecast`.


## Defining a source

Pulling data from a data source isn't typically limited to a single asset. We might for example fetch data from several API endpoints, while reusing common pieces of configuration or sharing an HTTP client.

Interloper allows you to define `sources`, which are essentially a collection of assets.

Let's now create an `open_meteo` source and add a second asset to it. We will take the opportunity to move at the source level the logic shared between assets

```py
@interloper.source
def open_meteo():
    client = httpx.Client(
        params={
            "latitude": 52.5244,
            "longitude": 13.4105,
        },
    )

    def to_records(data: dict):
        return [dict(zip(data.keys(), values)) for values in zip(*data.values())]

    @interloper.asset
    def forecast(date: dt.date):
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
        }
        response = client.get(url, params=params)
        data = response.json()["hourly"]
        return to_records(data)

    @interloper.asset
    def air_quality(date: dt.date):
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ["pm10", "pm2_5", "dust", "uv_index"],
        }
        response = client.get(url, params=params)
        data = response.json()["hourly"]
        return to_records(data)

    return (forecast, air_quality)
```

When a `source` is defined, its assets are exposed as attributes. Therefore, we can still run or materialize asset using:

```py
open_meteo.forecast.run(date=dt.date.today())
```
or
```py
open_meteo.air_quality.materialize(date=dt.date.today())
```

## Pipeline

!!! danger "TODO"

Any execution that runs and materializes more than one asset requires a `pipeline`

```py
pipeline = interloper.Pipeline(open_meteo)
pipeline.materialize()
```

## Binding parameters

Until now, we've been executing the original definition of assets directly. However, a basic requirement would be to reuse the same definition of a source or asset but with different configurations.

For example, we might want to use different account IDs or API keys, or here in the case of our `open_meteo` source, different coordinates.

Let's start by allowing the coordinates to be passed as parameters:

```py hl_lines="3-4"
@interloper.source
def open_meteo(
    lat: float = 52.5244,
    lon: float = 13.4105,
) -> Sequence[Asset]:
    ...
```

!!! warning
    A `source` parameter requires a default value.

Then, calling our source with different parameters values will return a copy with bound parameters.

```py
open_meteo_paris = open_meteo(lat=48.8566, lon=2.3522)
interloper.Pipeline(open_meteo_paris).materialize()
```

## Partitioning & Backfilling

!!! danger "TODO"