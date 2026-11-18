from typing import Any

import pandas as pd
from interloper_pandas import DataFrameNormalizer


def _decimal_comma_to_float(value: Any) -> Any:
    """Convert a decimal-comma numeric string ("1,5") to a float; pass the rest through.

    Args:
        value: One cell of the report.

    Returns:
        The float, or the value unchanged when it is not a decimal-comma number.
    """
    if isinstance(value, str) and "," in value:
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return value
    return value


class TheTradeDeskNormalizer(DataFrameNormalizer):
    """Reshape MyReports CSV rows onto the schema columns.

    Reports are requested in "International" format, so metric values carry
    decimal commas ("1,5") and the date column is DD/MM/YYYY; convert both so
    the generic numeric/date casts succeed. Headers also spell quartiles with
    ``%`` ("Player 25% Complete" → ``player_25pct_complete``), which the generic
    snake-casing would drop.
    """

    def column_name(self, name: str) -> str:
        """Rewrite one vendor column name before the standard mapping.

        Args:
            name: The column name as the API returned it.

        Returns:
            The mapped column name.
        """
        return super().column_name(name.replace("%", "pct"))

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = super().normalize(data)
        if df.empty:
            return df
        object_columns = df.select_dtypes(include="object").columns
        df[object_columns] = df[object_columns].map(_decimal_comma_to_float)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y").dt.date
        return df
