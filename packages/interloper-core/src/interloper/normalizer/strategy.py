"""Materialization strategy for controlling schema enforcement and data reconciliation."""

from enum import Enum


class MaterializationStrategy(str, Enum):
    """Controls how data is validated and reconciled during materialization.

    Attributes:
        AUTO: Infer a schema if none is provided; reconcile against the
            schema when one is declared.
        STRICT: Schema is required.  Data is validated against the schema
            and materialization fails on any mismatch.
        RECONCILE: Schema is required.  Columns are aligned to match the
            schema (extras dropped, missing filled), and values are coerced
            to the schema's types.
    """

    AUTO = "auto"
    STRICT = "strict"
    RECONCILE = "reconcile"
