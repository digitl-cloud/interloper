"""ClickHouse partition keys that line up with interloper partitions."""

from __future__ import annotations

from interloper.errors import ConfigError
from interloper.partitioning import PartitionConfig, TimeGranularity, TimePartitionConfig

from interloper_clickhouse.types import base_type


def partition_key(config: PartitionConfig | None, ref: str, column_type: str) -> str | None:
    """Return the ``PARTITION BY`` expression that makes one interloper partition one ClickHouse partition.

    A time partition covers a period, so its rows share the period start the
    expression computes: the day (the ``Date`` column itself, ``toDate`` of a
    ``DateTime``), ``toStartOfMonth``, ``toStartOfYear`` or ``toStartOfHour``.
    A ``String`` column is parsed first, so ISO dates held as text partition
    the same way. Any other partition is one value of the column.

    The expression is rendered over *ref*, so the same function renders the
    table's key (over the quoted column) and the key of a constant (over a
    ``CAST`` of it), which is how a partition's id is computed.

    Args:
        config: The asset's partitioning, or ``None`` for an unpartitioned asset.
        ref: The SQL the expression applies to: a quoted column, or a constant.
        column_type: The partition column's ClickHouse type.

    Returns:
        The expression, or ``None`` for an unpartitioned asset.

    Raises:
        ConfigError: If the column's type cannot carry the asset's time
            partitioning: hourly partitions on a ``Date``, or a time partition
            on a column that is neither a date, a datetime nor text.
    """
    if config is None:
        return None
    if not isinstance(config, TimePartitionConfig):
        return ref
    granularity = config.granularity
    kind = base_type(column_type)
    if kind in ("Date", "Date32"):
        if granularity is TimeGranularity.HOUR:
            raise ConfigError(
                f"Partition column '{config.column}' is a {kind}, which cannot hold hourly partitions; "
                "declare it as a datetime."
            )
        moment, day = ref, ref
    elif kind.startswith("DateTime"):
        moment, day = ref, f"toDate({ref})"
    elif kind == "String":
        moment = f"parseDateTime64BestEffort({ref}, 6, 'UTC')"
        day = f"toDate({moment})"
    else:
        raise ConfigError(
            f"Partition column '{config.column}' is a {kind}; time partitioning needs a date, a datetime or text."
        )
    return {
        TimeGranularity.HOUR: f"toStartOfHour({moment})",
        TimeGranularity.DAY: day,
        TimeGranularity.MONTH: f"toStartOfMonth({moment})",
        TimeGranularity.YEAR: f"toStartOfYear({moment})",
    }[granularity]


def same_key(actual: str, expected: str | None) -> bool:
    """Compare a table's partition key with the one the asset needs, ignoring quoting and spacing.

    ``system.tables`` reports the key as ClickHouse formats it, without the
    backticks this package renders and with its own spacing.

    Args:
        actual: The table's ``partition_key``, empty for an unpartitioned table.
        expected: The expression :func:`partition_key` renders, or ``None``.

    Returns:
        True when both name the same expression.
    """

    def normalize(expression: str) -> str:
        """Drop backticks and whitespace.

        Args:
            expression: A partition key expression.

        Returns:
            The expression without backticks or whitespace.
        """
        return "".join(expression.replace("`", "").split())

    return normalize(actual) == normalize(expected or "")
