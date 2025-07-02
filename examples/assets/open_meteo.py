import datetime as dt
import logging
from collections.abc import Sequence

import httpx
import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def open_meteo(
    lat: float = 52.5244,
    lon: float = 13.4105,
) -> Sequence[itlp.Asset]:
    client = httpx.Client(
        params={
            "latitude": lat,
            "longitude": lon,
        },
    )

    def to_records(data: dict) -> list[dict]:
        return [dict(zip(data.keys(), values)) for values in zip(*data.values())]

    @itlp.asset
    def forecast() -> list[dict]:
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
        }
        response = client.get(url, params=params)
        data = response.json()["hourly"]
        return to_records(data)

    @itlp.asset
    def air_quality() -> list[dict]:
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["pm10", "pm2_5", "dust", "uv_index"],
        }
        response = client.get(url, params=params)
        data = response.json()["hourly"]
        return to_records(data)

    return (forecast, air_quality)


open_meteo.io = {"file": itlp.FileIO("data")}
open_meteo_paris = open_meteo(lat=48.8566, lon=2.3522)
