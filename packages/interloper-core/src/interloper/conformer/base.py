"""Conformer: schema enforcement resolved from the data's representation.

The conform stage runs on every materialization, between normalization
(optional reshaping) and the destination write. A :class:`Conformer` carries
the schema operations — validate, reconcile, infer — for exactly one data
representation; the matching conformer is reached through the data's
:class:`~interloper.representation.Representation`.

Conformers are pure mechanism: stateless, never serialized, and not
user-configurable. User-facing configuration lives on
:class:`~interloper.normalizer.Normalizer` (reshaping) and the asset's
``schema`` / ``materialization_strategy`` (the contract and how strictly to
enforce it).

Core ships the rows conformer; integration packages ship theirs alongside
their :class:`~interloper.representation.Representation`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from interloper.schema import Schema
from interloper.utils.data import coerce_to_records


class Conformer(ABC):
    """Schema operations for one data representation.

    ``prepare`` canonicalizes raw asset output into the representation the
    other operations expect; it is called once per materialization, before
    any schema operation.
    """

    @abstractmethod
    def prepare(self, data: Any) -> Any:
        """Canonicalize *data* into this conformer's representation.

        Raises:
            NormalizerError: If the data cannot be represented as a table.
        """

    @abstractmethod
    def validate(self, data: Any, schema: type[Schema], *, strict: bool = False) -> None:
        """Validate *data* against *schema*; raise :class:`SchemaError` on mismatch."""

    @abstractmethod
    def reconcile(self, data: Any, schema: type[Schema]) -> Any:
        """Align *data* to *schema* (drop extras, add missing) and coerce values."""

    @abstractmethod
    def infer(self, data: Any) -> type[Schema]:
        """Infer a Schema from *data*."""


class RowsConformer(Conformer):
    """Schema operations on ``list[dict]`` records (pydantic row-wise)."""

    def prepare(self, data: Any) -> list[dict[str, Any]]:
        """Coerce dict / model / generator shapes to records.

        Returns:
            Data as a list of row dicts.
        """
        return coerce_to_records(data)

    def validate(self, data: list[dict[str, Any]], schema: type[Schema], *, strict: bool = False) -> None:
        """Validate each row against the schema."""
        schema.validate_rows(data, strict=strict)

    def reconcile(self, data: list[dict[str, Any]], schema: type[Schema]) -> list[dict[str, Any]]:
        """Reconcile rows against the schema.

        Returns:
            Reconciled rows with columns aligned and values coerced.
        """
        return schema.reconcile(data)

    def infer(self, data: list[dict[str, Any]]) -> type[Schema]:
        """Infer a Schema by scanning row values.

        Returns:
            A dynamically created Schema subclass.
        """
        return Schema.infer(data)
