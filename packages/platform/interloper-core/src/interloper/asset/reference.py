"""Reference: an asset interloper reads but never computes, a table that exists or that people decide."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from interloper.asset.base import Asset, AssetDefinition

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning import Partition, PartitionWindow


class Reference(Asset):
    """An asset whose table no run computes: nothing is created or written by a run.

    A reference points at a table in a destination and knows how to read it.
    ``table_name`` names the table as written, ``None`` naming it as any
    asset's table, and the asset's ``dataset`` names where it is, the
    source's by default; either may name a field of the owning source in
    braces (``"analytics_{property_id}"``), filled by `Source.format`. A run
    does nothing. Downstream assets read it like any other asset, and views
    build on it like any other table.

    An ``editable`` reference is one whose rows the app rewrites whole
    through the asset's destinations. It declares a schema, the contract
    every write is checked against, and no partitioning, since it is only
    ever written whole.
    """

    reads_upstream_data: ClassVar[bool] = False
    table_name: ClassVar[str | None] = None
    editable: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Refuse an editable reference without a schema or with a partitioning.

        Args:
            **kwargs: Class-creation keyword arguments, passed through to ``super()``.

        Raises:
            TypeError: If the class is ``editable`` and declares no schema, or
                declares a partitioning.
        """
        super().__init_subclass__(**kwargs)
        if not cls.editable:
            return
        if cls.schema is None:
            raise TypeError(f"Editable reference '{cls.key}' declares no schema: its rows are checked against one")
        if cls.partitioning is not None:
            raise TypeError(f"Editable reference '{cls.key}' declares a partitioning: it is only ever written whole")

    @classmethod
    def definition(cls) -> AssetDefinition:
        """Produce the asset's definition, saying whether it is editable.

        Returns:
            The asset definition, ``editable`` set from the class.
        """
        return super().definition().model_copy(update={"editable": cls.editable})

    async def materialize_async(
        self,
        window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Do nothing: no run computes the table.

        Args:
            window: Ignored.
            dag: Ignored.
            metadata: Ignored.

        Returns:
            ``None``.
        """
        return None
