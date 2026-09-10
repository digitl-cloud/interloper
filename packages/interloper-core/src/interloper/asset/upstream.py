"""The leg object handed to ``data()`` for upstream relations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from interloper.representation import Representation

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True)
class Upstream:
    """One upstream asset as handed to ``data()``, with what its destination holds for the run.

    Attributes:
        asset: The upstream asset the data was read from; its ``source``,
            ``identity`` and ``id`` tell the legs apart.
        data: The data read from the upstream's destination for the run's
            partition, in whatever representation that destination holds
            natively (records from a file destination, a DataFrame from
            BigQuery). ``None`` when the upstream has nothing materialized
            where the destination looks (no table or object for that
            partition at all); a ``LOG`` warning names the leg. An existing
            but empty partition is not this case: it arrives as whatever the
            destination returns for an empty read (an empty list or frame),
            not ``None``.
    """

    asset: Asset
    data: Any

    @property
    def records(self) -> list[dict[str, Any]]:
        """The data as records, whatever representation the destination handed back.

        The one line a consumer needs to be indifferent to where its upstream
        is stored: a DataFrame and a list of rows both come out as
        ``list[dict]``, and a leg with nothing materialized comes out empty.

        Returns:
            One mapping per row; ``[]`` when :attr:`data` is ``None``.
        """
        if self.data is None:
            return []
        return Representation.of(self.data).to_records(self.data)
