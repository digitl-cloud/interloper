from __future__ import annotations

from typing import TYPE_CHECKING, Any

from interloper_pandas import DataFrameNormalizer

if TYPE_CHECKING:
    import pandas as pd


class SnapchatStatsNormalizer(DataFrameNormalizer):
    """Reshape Snapchat stats rows into a flat frame.

    Each row nests its metrics under a ``stats`` dict (flattened to ``stats_*``
    then de-prefixed) or, for dimensioned reports, carries a ``dimension_stats``
    list that is exploded into one row per dimension value. Column-name
    normalization is then applied by the base ``DataFrameNormalizer``.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        import pandas as pd

        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        df = pd.json_normalize(records, sep="_")
        if df.empty:
            return df

        if "dimension_stats" in df.columns:
            df = df[df["dimension_stats"].map(lambda v: isinstance(v, list) and len(v) > 0)]
            df = df.explode("dimension_stats").reset_index(drop=True)
            expanded = df["dimension_stats"].apply(pd.Series)
            df = pd.concat([df.drop(columns=["dimension_stats"]), expanded], axis=1)
        else:
            df = df.rename(columns={col: col.replace("stats_", "") for col in df.columns})

        return super().normalize(df)
