"""What ``data()`` receives for each upstream: the upstream asset and what its destination holds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from interloper.destination.context import IOContext
from interloper.errors import AssetError, DataNotFoundError, format_exception
from interloper.events import EventBus, EventType, Step, StepKind
from interloper.representation import TableData
from interloper.telemetry import attributes as telemetry_attributes
from interloper.utils.concurrency import invoke

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.destination import Destination
    from interloper.partitioning import PartitionWindow


@dataclass(frozen=True)
class Upstream:
    """One upstream asset as handed to ``data()``, with what its destination holds for the run.

    Attributes:
        asset: The upstream asset the data was read from; its ``source``,
            ``identity`` and ``id`` tell the upstreams apart.
        data: The data read from the upstream's destination for the run's
            partition, in whatever representation that destination holds
            natively (rows from a file destination, a DataFrame from
            BigQuery). ``None`` when the upstream has nothing materialized
            where the destination looks (no table or object for that
            partition at all); a ``LOG`` warning names the upstream. An existing
            but empty partition is not this case: it arrives as whatever the
            destination returns for an empty read (an empty list or frame),
            not ``None``. For a view, the destination's reference to the
            upstream (a `TableReference` on a SQL warehouse) rather than
            data: a view reads nothing.
    """

    asset: Asset
    data: Any

    @property
    def rows(self) -> list[dict[str, Any]]:
        """The data as rows, whatever representation the destination handed back.

        The one line a consumer needs to be indifferent to where its upstream
        is stored: a DataFrame and a list of rows both come out as
        ``list[dict]``, and an upstream with nothing materialized comes out empty.

        Returns:
            One mapping per row; ``[]`` when `data` is ``None``.
        """
        if self.data is None:
            return []
        return TableData.from_data(self.data).rows

    @classmethod
    async def read(
        cls,
        upstream: Asset,
        reader: Asset,
        window: PartitionWindow | None,
        metadata: dict[str, Any],
        *,
        parameter: str,
    ) -> Upstream:
        """Read an upstream's data from its default destination for the window it consumes.

        Nothing materialized where the destination looks is not a failure: the
        upstream is handed over with ``data=None`` and a warning event, and the
        reader decides what that means. Any other failure fails the reader.

        Args:
            upstream: The asset whose data is read.
            reader: The asset reading it, whose identity the events carry.
            window: The window the reader consumes. A partitioned upstream is read for it as is,
                since the DAG only joins partitionings that accept the same windows (a snapshot
                reads its upstream snapshot's observation); an unpartitioned one is read whole.
            metadata: Run-level metadata carried onto the emitted events.
            parameter: The reader's ``data()`` parameter the upstream is bound
                to, which the missing-data warning names.

        Returns:
            The upstream with its data.

        Raises:
            AssetError: If the read fails for any reason other than missing data.
        """
        destination = upstream.default_destination
        window = window if upstream.partitioning is not None else None
        context = IOContext(asset=upstream, window=window, metadata=metadata, schema=upstream.schema)
        step = Step(
            StepKind.DEST_READ,
            reader.event_metadata(metadata, window, destination_key=destination.key),
            subject=f"'{upstream.key}'",
            attributes={telemetry_attributes.UPSTREAM_KEY: upstream.key},
        )
        data: Any = None
        try:
            with step:
                data = await invoke(destination.read, context)
        except DataNotFoundError:
            EventBus.emit(
                EventType.LOG,
                metadata=reader.event_metadata(
                    metadata,
                    window,
                    level="WARNING",
                    message=(
                        f"Asset '{reader.key}' found no data in upstream '{upstream.qualified_key}' "
                        f"for parameter '{parameter}' at {window}; it is passed with data=None"
                    ),
                ),
            )
            return cls(asset=upstream, data=None)
        except Exception as error:
            raise AssetError(
                f"Failed to load data from upstream asset '{upstream.key}': {format_exception(error)}"
            ) from error
        return cls(asset=upstream, data=data)

    @classmethod
    def reference(cls, upstream: Asset, destination: Destination) -> Upstream:
        """Refer to an upstream's table instead of reading it, for a kind that reads no data.

        Args:
            upstream: The asset referred to.
            destination: The destination whose handle on the upstream is wanted.

        Returns:
            The upstream with the destination's reference as its data.
        """
        return cls(asset=upstream, data=destination.reference(upstream))
