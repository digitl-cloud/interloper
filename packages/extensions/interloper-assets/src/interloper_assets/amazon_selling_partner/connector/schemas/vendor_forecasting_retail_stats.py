import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorForecastingRetailStats(Schema):
    """Forward-looking demand forecast per ASIN (GET_VENDOR_FORECASTING_REPORT, RETAIL).

    A snapshot of the latest generated forecast — mean and percentile forecast units
    over the forecast horizon — rather than a per-day historical report.
    """

    asin: str | None = Field(default=None, description="Amazon Standard Identification Number for the product.")
    start_date: dt.date | None = Field(default=None, description="The start date of the forecast period.")
    end_date: dt.date | None = Field(default=None, description="The end date of the forecast period.")
    forecast_generation_date: dt.date | None = Field(
        default=None, description="The date when the forecast was generated."
    )
    mean_forecast_units: float | None = Field(
        default=None, description="The average number of units forecasted to be sold."
    )
    p_70_forecast_units: float | None = Field(
        default=None, description="The number of units forecasted to be sold with 70% confidence."
    )
    p_80_forecast_units: float | None = Field(
        default=None, description="The number of units forecasted to be sold with 80% confidence."
    )
    p_90_forecast_units: float | None = Field(
        default=None, description="The number of units forecasted to be sold with 90% confidence."
    )
