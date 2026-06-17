"""Normalizer: data reshaping (coercion, flattening, column naming). Schema ops live in interloper.conformer."""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import Field

from interloper.component import Component
from interloper.utils.data import coerce_to_records
from interloper.utils.text import to_snake_case


class Normalizer(Component):
    """Type-native normalizer for ``list[dict]`` asset data.

    Accepts arbitrary return types (``dict``, ``list[dict]``, ``BaseModel``,
    ``list[BaseModel]``, ``Generator``), coerces to ``list[dict]``, then
    applies optional transformations (column-name normalization, nested-dict
    flattening, missing-column fill).

    Normalizer is a :class:`Component` so configured instances round-trip
    through ``ComponentSpec`` with their concrete subclass intact — e.g.
    across the host → child-pod DAG-spec boundary.

    Usage::

        @asset(normalizer=Normalizer())
        def my_asset(context):
            return [{"UserName": "alice", "Address": {"City": "NYC"}}]

    Attributes:
        normalize_columns_names: Convert column names to snake_case.
        flatten_max_level: Maximum nesting depth to flatten.  ``0`` disables
            flattening, ``None`` flattens without limit, a positive ``int``
            flattens up to that many levels.
        flatten_separator: Separator for flattened key names.
        fill_missing: Fill missing keys across rows with ``None`` so every row
            has the same columns.
        replace_empty_dicts: Replace ``{}`` values with ``None``.
        replace_empty_strings: Replace ``""`` values with ``None``.
        drop_na_columns: Drop columns where every value is ``None``.
        snake_case_digits: Treat digit groups as words when snake-casing
            column names (``acosClicks14d`` → ``acos_clicks_14d`` instead of
            ``acos_clicks14d``).  Off by default because the right convention
            depends on the upstream API's naming semantics.
        column_overrides: Explicit raw-name → normalized-name mapping, applied
            before the snake_case conversion.  For API quirk names no general
            rule can handle (``eCPAddToCart`` → ``ecp_add_to_cart``).
    """

    # Shadow Component.id — normalizers are pure configuration, not runtime
    # instances that need identity. (Same pattern as Schema.)
    id: ClassVar[str] = ""

    normalize_columns_names: bool = True
    flatten_max_level: int | None = 0
    flatten_separator: str = "_"
    fill_missing: bool = True
    drop_na_columns: bool = False
    snake_case_digits: bool = False
    column_overrides: dict[str, str] = Field(default_factory=dict)
    replace_empty_dicts: bool = False
    replace_empty_strings: bool = False

    # Override Component.model_post_init to avoid setting an instance id.
    def model_post_init(self, context: Any) -> None:
        """No-op: normalizers don't need instance identity."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalize(self, data: Any) -> list[dict[str, Any]]:
        """Normalize *data* to ``list[dict]`` with configured transformations.

        Coerces the input to ``list[dict]``, then applies flatten, column
        rename, and fill-missing in order.

        Args:
            data: Raw asset output (any supported type).

        Returns:
            Normalized list of row dicts.
        """
        rows = self._coerce(data)

        if self.replace_empty_dicts:
            rows = [{k: (None if isinstance(v, dict) and not v else v) for k, v in row.items()} for row in rows]

        if self.flatten_max_level is None or self.flatten_max_level > 0:
            rows = [self._flatten_dict(row) for row in rows]

        if self.replace_empty_strings:
            rows = [{k: (None if v == "" else v) for k, v in row.items()} for row in rows]

        if self.drop_na_columns and rows:
            rows = self._drop_na_columns(rows)

        if self.normalize_columns_names:
            rows = [{self.column_name(k): v for k, v in row.items()} for row in rows]

        if self.fill_missing:
            rows = self._fill_missing_keys(rows)

        return rows

    def column_name(self, name: str) -> str:
        """Transform a column name according to the normalizer's convention.

        Explicit ``column_overrides`` win; otherwise converts to
        ``snake_case``, optionally splitting letter→digit boundaries
        (see ``snake_case_digits``).

        Args:
            name: Original column name.

        Returns:
            Transformed column name.
        """
        if name in self.column_overrides:
            return self.column_overrides[name]
        name = to_snake_case(name)
        if self.snake_case_digits:
            name = re.sub(r"([a-z])(\d)", r"\1_\2", name)
        return name

    # ------------------------------------------------------------------
    # Type coercion
    # ------------------------------------------------------------------

    def _coerce(self, data: Any) -> list[dict[str, Any]]:
        """Coerce arbitrary data to ``list[dict]`` (raises ``NormalizerError`` when unsupported).

        Returns:
            The coerced list of row dicts.
        """
        return coerce_to_records(data)

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def _flatten_dict(
        self,
        d: dict[str, Any],
        parent_key: str = "",
        level: int = 0,
    ) -> dict[str, Any]:
        """Flatten nested dicts using separator-joined keys.

        Returns:
            A flat dict with separator-joined keys.
        """
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{self.flatten_separator}{k}" if parent_key else k
            if isinstance(v, dict) and (self.flatten_max_level is None or level < self.flatten_max_level):
                items.extend(self._flatten_dict(v, new_key, level + 1).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def _fill_missing_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Ensure every row has the same set of keys, filling gaps with ``None``.

        Returns:
            Rows with a uniform set of keys.
        """
        if not rows:
            return rows

        # Preserve insertion order of keys
        all_keys: dict[str, None] = {}
        for row in rows:
            for k in row:
                all_keys.setdefault(k, None)

        key_set = all_keys.keys()
        return [{k: row.get(k) for k in key_set} for row in rows]

    @staticmethod
    def _drop_na_columns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Drop columns where every value is ``None``.

        Returns:
            Rows without all-null columns.
        """
        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(row.keys())

        na_keys = {k for k in all_keys if all(row.get(k) is None for row in rows)}
        if not na_keys:
            return rows
        return [{k: v for k, v in row.items() if k not in na_keys} for row in rows]
