"""JSON serialization helpers shared by the Google Cloud destinations."""

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any


def replace_non_finite(obj: Any) -> Any:
    """Recursively replace non-finite floats (``NaN``, ``Infinity``) with ``None``.

    pandas represents missing numeric values as ``float('nan')``. Python's
    ``json.dumps`` serialises these to the bare tokens ``NaN`` / ``Infinity``,
    which are invalid JSON — BigQuery's load parser rejects them with "Parser
    terminated before end of string". Neither BigQuery nor JSONL files have a
    NaN concept, so map non-finite floats to ``None`` (JSON ``null``).
    """
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: replace_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_non_finite(v) for v in obj]
    return obj


def json_default(o: Any) -> Any:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
