from typing import Any

import pandas as pd
from interloper_pandas import DataFrameNormalizer


class TiktokStatsNormalizer(DataFrameNormalizer):
    """Flatten TikTok integrated-report rows into a flat frame.

    Each row nests its requested dimensions under ``dimensions`` and its metrics
    under ``metrics``; merge both into a single flat record (dropping the parent
    prefix) before the base ``DataFrameNormalizer`` runs. A metric that does not
    apply to the row comes back as ``"-"`` and becomes NULL. ``stat_time_day``
    comes back as a midnight datetime string (``2026-01-01 00:00:00``); coerce it
    to a plain date so it lands on the ``date``-typed schema column.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        flat: list[dict[str, Any]] = []
        for row in records:
            merged = {}
            merged.update(row.get("dimensions") or {})
            merged.update({key: None if value == "-" else value for key, value in (row.get("metrics") or {}).items()})
            for key, value in row.items():
                if key not in ("dimensions", "metrics"):
                    merged[key] = value
            flat.append(merged)

        df = super().normalize(flat)
        if "stat_time_day" in df.columns:
            df["stat_time_day"] = pd.to_datetime(df["stat_time_day"], errors="coerce").dt.date
        return df
