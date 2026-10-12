"""SqlView: a view authored as an Ibis expression over its upstreams' declared schemas."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import ibis
from interloper.asset import Upstream, View
from interloper.destination.database import TableReference
from interloper.errors import ConfigError

from interloper_sql.view.types import IBIS_DIALECTS, ibis_schema

if TYPE_CHECKING:
    from interloper.asset import Asset
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


class IbisDefinition:
    """A view definition that compiles an Ibis expression for a dialect."""

    def __init__(self, expression: ibis.Table) -> None:
        """Wrap an expression.

        Args:
            expression: The view's query as an Ibis table expression.
        """
        self.expression = expression

    def compile(self, dialect: str) -> str:
        """Render the expression as SQL for a warehouse dialect.

        Args:
            dialect: The destination's dialect name.

        Returns:
            The SQL text.

        Raises:
            ConfigError: If the dialect has no Ibis counterpart.
        """
        if dialect not in IBIS_DIALECTS:
            raise ConfigError(f"No Ibis dialect for '{dialect}'")
        return str(ibis.to_sql(self.expression, dialect=IBIS_DIALECTS[dialect]))


class SqlView(View):
    """A view whose ``data()`` receives Ibis tables and returns an Ibis expression.

    Each upstream's ``data`` is an ``ibis.Table`` named after the table the
    destination references and typed from the upstream's declared schema,
    so the expression is checked as it is built. The result must carry the
    view's declared columns with their declared types; the destination then
    compiles it for its own dialect.
    """

    async def _read_upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Turn every table reference into a typed Ibis table.

        Args:
            name: The ``data()`` parameter the upstreams are built for.
            targets: The bound upstream assets.
            dag: Forwarded to `View._read_upstreams`.
            partition_or_window: Forwarded.
            metadata: Forwarded.

        Returns:
            One `Upstream` per target, its ``data`` an ``ibis.Table``.

        Raises:
            ConfigError: If an upstream declares no schema, or its reference is not a table.
        """
        upstreams = await super()._read_upstreams(name, targets, dag, partition_or_window, metadata)
        tables: list[Upstream] = []
        for upstream in upstreams:
            reference = upstream.data
            if not isinstance(reference, TableReference):
                raise ConfigError(f"Upstream '{upstream.asset.qualified_key}' is not a table: {reference!r}")
            if reference.schema is None:
                raise ConfigError(f"Upstream '{upstream.asset.qualified_key}' declares no schema to type its table")
            *outer, table = reference.parts
            location = {}
            if outer:
                location["database"] = outer[-1]
            if len(outer) > 1:
                location["catalog"] = outer[-2]
            tables.append(
                Upstream(asset=upstream.asset, data=ibis.table(ibis_schema(reference.schema), name=table, **location))
            )
        return tables

    def _definition(self, result: Any) -> IbisDefinition:
        """Check the expression against the declared schema and wrap it.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The compilable definition.

        Raises:
            ConfigError: If the result is not an Ibis table, the view
                declares no schema, or the expression's columns differ from
                the declared ones.
        """
        if not isinstance(result, ibis.Table):
            raise ConfigError(f"View '{self.qualified_key}' returned {type(result).__name__}, not an Ibis table")
        if self.schema is None:
            raise ConfigError(f"View '{self.qualified_key}' declares no schema")
        declared = {name: str(dtype) for name, dtype in ibis_schema(self.schema).items()}
        produced = {name: str(dtype) for name, dtype in result.schema().items()}
        if declared != produced:
            missing = sorted(set(declared) - set(produced))
            extra = sorted(set(produced) - set(declared))
            retyped = sorted(name for name in declared.keys() & produced.keys() if declared[name] != produced[name])
            raise ConfigError(
                f"View '{self.qualified_key}' declares columns its expression does not produce: "
                f"missing {missing}, extra {extra}, retyped {retyped}"
            )
        return IbisDefinition(result)


def union(upstreams: Sequence[Upstream], **columns: Callable[[Upstream], Any]) -> ibis.Table:
    """The UNION ALL of every upstream's table, optionally stamping a column per upstream.

    Args:
        upstreams: The bound upstreams of a many-valued relation, each carrying an ``ibis.Table``.
        **columns: Column name to a function of the upstream giving that
            upstream's constant value, such as the account it came from.

    Returns:
        One table over every upstream.

    Raises:
        ConfigError: If no upstream is bound; a view over nothing has no columns.
    """
    if not upstreams:
        raise ConfigError("no upstream bound: a view needs at least one")
    tables = []
    for upstream in upstreams:
        stamps = {}
        for name, value in columns.items():
            stamped = value(upstream)
            is_integer = isinstance(stamped, int) and not isinstance(stamped, bool)
            stamps[name] = ibis.literal(stamped, type="int64") if is_integer else ibis.literal(stamped)
        tables.append(upstream.data.mutate(**stamps))
    return ibis.union(*tables, distinct=False)
