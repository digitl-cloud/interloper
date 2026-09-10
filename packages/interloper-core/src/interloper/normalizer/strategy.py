"""Materialization strategy: how strictly the conform step enforces the schema."""

from __future__ import annotations

from enum import Enum
from typing import Any


class MaterializationStrategy(str, Enum):
    """How the conform step enforces an asset's schema.

    Both strategies hand destinations data in the schema's canonical types;
    they differ in what counts as an error.

    Attributes:
        RECONCILE: The default. Align columns to the schema (extras dropped
            with a warning, missing nullables filled) and coerce values. With
            no schema declared, infer one from the data for the destinations.
        STRICT: Schema required. Extra columns, missing required columns and
            values the schema rejects fail the materialization; values that
            pass are still coerced to the declared types.
    """

    STRICT = "strict"
    RECONCILE = "reconcile"

    @classmethod
    def _missing_(cls, value: Any) -> MaterializationStrategy | None:
        """Read the retired ``auto`` value, which stored configs and manifests still carry, as ``RECONCILE``.

        Args:
            value: The value no member matched.

        Returns:
            ``RECONCILE`` for ``"auto"``, else ``None`` so the enum raises as usual.
        """
        return cls.RECONCILE if value == "auto" else None
