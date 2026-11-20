from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from interloper_pandas import DataFrameNormalizer

if TYPE_CHECKING:
    import pandas as pd


class AmazonAdsNormalizer(DataFrameNormalizer):
    """The source's normalization, keeping a campaign's free-form ``tags`` whole.

    ``tags`` is an arbitrary key/value map the advertiser writes on a campaign.
    Flattened like the other nested fields, it would become one column per key
    an advertiser ever used, so it is kept as one JSON string under the
    vendor's name before the frame is built.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Encode each row's ``tags`` map as JSON, then normalize as configured.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame, ``tags`` as a JSON string where a row has it.
        """
        import pandas as pd

        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = [
            {**row, "tags": json.dumps(row["tags"])} if isinstance(row.get("tags"), dict) else row for row in records
        ]
        return super().normalize(rows)


_DISPLAY_IDS = frozenset({"campaignId", "adGroupId", "adId", "portfolioId"})


class SponsoredDisplayEntityNormalizer(AmazonAdsNormalizer):
    """The source's normalization, plus Sponsored Display's numeric ids and compact ``YYYYMMDD`` dates.

    Sponsored Display sends ids as numbers where the other ad products send
    strings. They become strings row by row, before the frame is built: some
    ids are beyond what a float holds exactly, and a column with a missing
    value (``portfolioId``) would otherwise turn float. It also sends
    ``startDate``/``endDate`` as ``20261001`` where the others send
    ``2026-10-01``; strict validation reads a digits-only string as a Unix
    timestamp, so the two columns are parsed with their explicit format, and a
    value in any other format fails.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Stringify the ids, normalize as the source does, then parse the schedule dates.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame, ids as strings and ``start_date``/``end_date`` as dates.
        """
        import pandas as pd

        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = [
            {key: str(value) if key in _DISPLAY_IDS and pd.notna(value) else value for key, value in row.items()}
            for row in records
        ]
        df = super().normalize(rows)
        for column in ("start_date", "end_date"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], format="%Y%m%d").dt.date
        return df
