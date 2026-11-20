from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

from interloper_pandas import DataFrameNormalizer

if TYPE_CHECKING:
    import pandas as pd


class LinkedinAdsStatsNormalizer(DataFrameNormalizer):
    """Type the ``dateRange`` of ad analytics rows as days.

    Each row carries its day as ``{"start": {"year", "month", "day"}, "end": {...}}``.
    Both bounds become ``dt.date`` values before flattening, so they land on
    ``date_range_start`` / ``date_range_end`` under the vendor's own names.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Convert the ``dateRange`` bounds, then run the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        import pandas as pd

        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = []
        for record in records:
            date_range = record.get("dateRange") or {}
            days = {bound: dt.date(**value) for bound, value in date_range.items() if isinstance(value, dict)}
            rows.append({**record, "dateRange": days} if days else record)
        return super().normalize(rows)
