from typing import Any, Literal

import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic import Field


class LinkedinOrganicNormalizer(DataFrameNormalizer):
    """Flatten LinkedIn organic statistics and type their epoch instants.

    LinkedIn serializes ``timeRange.start`` / ``timeRange.end`` as milliseconds
    since the epoch. The conformer would read a bare integer as nanoseconds, so
    the listed columns are converted to UTC datetimes here.

    Attributes:
        epoch_columns: Normalized column names holding epoch timestamps; a
            column absent from the frame is skipped.
        epoch_unit: The unit of the epoch timestamps, ``"ms"`` for LinkedIn.
    """

    epoch_columns: list[str] = Field(default_factory=list)
    epoch_unit: Literal["s", "ms"] = "ms"

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize the rows, then convert the epoch columns to UTC instants.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = super().normalize(data)
        for column in self.epoch_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], unit=self.epoch_unit, utc=True)
        return df
