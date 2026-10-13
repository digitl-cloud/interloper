"""Output: what one run of an asset produced, its data and the schema it was checked against."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from interloper.errors import AssetError, RepresentationError
from interloper.normalizer import MaterializationStrategy
from interloper.representation import Representation, View
from interloper.telemetry.tracer import tracer
from interloper.utils.data import is_empty

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.schema import Schema


@dataclass(frozen=True)
class Output:
    """What one run of an asset produced.

    A value, so a run's schema travels with its data from conform to the write
    instead of resting on the asset between the two.

    Attributes:
        data: The rows, conformed to the schema when there is one, or an opaque
            object no representation matches.
        schema: The schema the data was checked against: the declared one, the
            inferred one, or ``None`` when there is neither.
    """

    data: Any
    schema: type[Schema] | None = None

    @classmethod
    def from_result(cls, result: Any, asset: Asset) -> Output:
        """Shape what ``data()`` returned into the asset's output: rows, normalized, then conformed.

        Normalization (when a normalizer is set) reshapes the data; conform
        then enforces the declared schema according to the materialization
        strategy, whether or not a normalizer ran, so a declared schema is
        always a checked contract.

        Args:
            result: The value ``data()`` returned.
            asset: The asset whose normalizer, schema and strategy apply.

        Returns:
            The output.
        """
        data = cls._rows(result)
        attributes = asset._span_attributes()
        if asset.normalizer is not None:
            with tracer().start_as_current_span("interloper.normalizer.normalize", attributes=attributes):
                data = asset.normalizer.normalize(data)
        with tracer().start_as_current_span("interloper.asset.conform", attributes=attributes):
            return cls._conform(data, asset)

    @staticmethod
    def _rows(result: Any) -> Any:
        """Turn the Python shapes ``data()`` may return besides a table into rows.

        The one place the framework is lenient about what an asset returns: a
        generator is consumed, a pydantic model or a list of them is dumped, a
        lone dict becomes one row, and ``None`` is no rows. Everything else,
        tables included, is returned untouched.

        Args:
            result: The raw value returned by ``data()``.

        Returns:
            The rows, or the result unchanged when it is not one of those shapes.
        """
        if result is None:
            return []
        if isinstance(result, Iterator):
            result = list(result)
        if isinstance(result, BaseModel):
            return [result.model_dump()]
        if isinstance(result, dict):
            return [result]
        if isinstance(result, list) and result and isinstance(result[0], BaseModel):
            return [item.model_dump() if isinstance(item, BaseModel) else item for item in result]
        return result

    @classmethod
    def _conform(cls, data: Any, asset: Asset) -> Output:
        """Enforce the asset's schema according to its materialization strategy.

        With a schema, ``RECONCILE`` aligns and coerces while ``STRICT``
        refuses extra, missing or invalid columns. Without one, ``RECONCILE``
        infers a schema and ``STRICT`` is an error. Data no representation
        matches passes through without a schema and is an error with one.

        Args:
            data: The rows, normalized when a normalizer is set.
            asset: The asset whose schema and strategy apply.

        Returns:
            The conformed output.

        Raises:
            AssetError: If ``STRICT`` is set without a schema, or if a schema
                is declared but the data is not tabular.
        """
        strategy = asset.materialization_strategy
        schema = asset.schema
        if schema is None and strategy is MaterializationStrategy.STRICT:
            raise AssetError(f"Asset '{asset.key}': strategy='strict' requires a schema.")
        try:
            view = Representation.of(data)
        except RepresentationError as error:
            if schema is None:
                return cls(data)
            raise AssetError(
                f"Asset '{asset.key}' declares a schema but returned data that cannot be checked against it: {error}"
            ) from error
        if schema is None:
            with tracer().start_as_current_span("interloper.asset.infer_schema", attributes=asset._span_attributes()):
                return cls(data, cls._infer(view))
        with tracer().start_as_current_span("interloper.representation.reconcile", attributes=asset._span_attributes()):
            return cls(view.reconcile(schema, strict=strategy is MaterializationStrategy.STRICT), schema)

    @staticmethod
    def _infer(view: View) -> type[Schema] | None:
        """Best-effort schema inference, metadata for destinations that must never fail a run.

        Args:
            view: The data bound to its representation.

        Returns:
            The inferred schema, or ``None`` when the data is empty or
            inference fails.
        """
        if is_empty(view.data):
            return None
        try:
            return view.infer()
        except Exception:  # noqa: BLE001 - inference is best-effort metadata
            return None
