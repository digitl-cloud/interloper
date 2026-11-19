import re
from typing import Any

import pandas as pd
from interloper_pandas import DataFrameNormalizer

# Insight columns whose cells are lists of ``{action_type, value}``; each is
# pivoted into one column per action type (``actions`` -> ``actions_link_click``).
PIVOT_COLUMNS = [
    "action_values",
    "actions",
    "conversions",
    "cost_per_action_type",
    "cost_per_conversion",
    "cost_per_outbound_click",
    "cost_per_unique_action_type",
    "cost_per_unique_outbound_click",
    "outbound_clicks_ctr",
    "outbound_clicks",
    "unique_actions",
    "unique_outbound_clicks_ctr",
    "unique_outbound_clicks",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "website_ctr",
]


def _sanitize(name: str) -> str:
    """Lower-case a column name and collapse non-alphanumeric runs to ``_``.

    Args:
        name: The column name, after the action pivot.

    Returns:
        The sanitized name, without leading or trailing ``_``.
    """
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()


def _pivot_cell(column: str, cell: Any) -> dict[str, float]:
    """Pivot a list of ``{action_type, value}`` into ``{column_actiontype: value}``.

    An action type listed more than once is summed, so each yields a single
    column.

    Args:
        column: The insight column the cell belongs to, which prefixes each key.
        cell: The cell's value; anything but a list yields no columns.

    Returns:
        One summed value per action type, keyed ``<column>_<action_type>``; an
        entry without an action type or a numeric value is skipped.
    """
    out: dict[str, float] = {}
    if isinstance(cell, list):
        for entry in cell:
            action_type = entry.get("action_type")
            if action_type is None:
                continue
            value = pd.to_numeric(entry.get("value"), errors="coerce")
            if pd.isna(value):
                continue
            key = f"{column}_{action_type}"
            out[key] = out.get(key, 0.0) + float(value)
    return out


class FacebookActionsNormalizer(DataFrameNormalizer):
    """Pivot Facebook insight action-lists into per-action-type columns.

    Each ``actions``/``action_values``/… cell is a list of ``{action_type,
    value}``; pivot it into one ``<column>_<action_type>`` column, sanitize the
    names, coerce custom-conversion columns to float, then defer to the base
    ``DataFrameNormalizer`` to flatten nested entity dicts (``creative``) and
    snake-case the rest.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            return df

        for column in PIVOT_COLUMNS:
            if column not in df.columns:
                continue
            cells = df[column].map(lambda cell, column=column: _pivot_cell(column, cell))
            pivoted = pd.DataFrame(list(cells), index=df.index)
            df = pd.concat([df.drop(columns=[column]), pivoted], axis=1)

        df.columns = [_sanitize(c) for c in df.columns]

        # Custom-conversion columns are not in the schema; coerce to float so
        # mixed int/NaN values don't surface as objects.
        for column in df.columns:
            if re.match(r".*_custom(_.+)?$", column):
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)

        return super().normalize(df)
