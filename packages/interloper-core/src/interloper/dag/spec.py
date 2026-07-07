"""Serializable DAG specification for cross-process reconstruction."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from interloper.component import Component, ComponentSpec

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.dag.base import DAG


class DAGSpec(BaseModel):
    """Serializable representation of a DAG.

    Holds a flat list of component specs which may be either
    :class:`~interloper.source.Source` specs (each carrying their
    asset-override map) or individual standalone
    :class:`~interloper.asset.Asset` specs.  The DAG constructor flattens
    sources back into their asset lists on reconstruction.
    """

    items: list[ComponentSpec] = []

    def reconstruct(self, catalog: Catalog | None = None) -> DAG:
        """Reconstruct the DAG from its spec.

        Each source spec materialises a live source (with its assets
        pre-bound through ``Source.model_post_init`` → ``_resolve``),
        and each standalone asset spec materialises a bare asset.  All
        reconstructed items are then handed to the :class:`DAG`
        constructor which re-infers the dependency graph from the
        preserved asset ids.

        Args:
            catalog: Catalog used to resolve ``key`` references, shared
                across all items. Defaults to the settings-configured
                catalog, built lazily.

        Returns:
            A new DAG instance with the same structure as the original.
        """
        from interloper.dag.base import DAG

        reconstructed = [Component.from_spec(spec, catalog) for spec in self.items]
        return DAG(*reconstructed)  # ty: ignore[invalid-argument-type]
