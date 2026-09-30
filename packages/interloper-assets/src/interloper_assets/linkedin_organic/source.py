import datetime as dt
from typing import Any, Literal

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic import Field

from interloper_assets.linkedin_organic import constants
from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.schemas import (
    FollowersStats,
    Industries,
    JobFunctions,
    PageStats,
    Seniorities,
    ShareStats,
)


# -- NORMALIZER ----------------------------------------------------------------
class LinkedinOrganicNormalizer(DataFrameNormalizer):
    """Flatten LinkedIn organic statistics and type their epoch instants.

    LinkedIn serializes ``timeRange.start`` / ``timeRange.end`` as milliseconds
    since the epoch. The conformer would read a bare integer as nanoseconds, so
    the listed columns are converted to UTC datetimes here.

    Attributes:
        epoch_columns: Normalized column names holding epoch timestamps; a
            column absent from the frame is skipped.
        epoch_unit: The unit of the epoch timestamps, ``"ms"`` for LinkedIn.
    """

    epoch_columns: list[str] = Field(default_factory=list)
    epoch_unit: Literal["s", "ms"] = "ms"

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize the rows, then convert the epoch columns to UTC instants.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = super().normalize(data)
        for column in self.epoch_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], unit=self.epoch_unit, utc=True)
        return df


# -- HELPERS -------------------------------------------------------------------
def _with_date(rows: list[dict[str, Any]], date: dt.date) -> list[dict[str, Any]]:
    """Stamp the partition date onto rows whose payload carries no day.

    Used by every asset: the statistics are dated only by the request (or by
    epoch-millisecond instants), the taxonomies not at all.

    Args:
        rows: The rows as the API returned them.
        date: The partition day to stamp.

    Returns:
        The rows, each with a ``date`` key.
    """
    return [{**row, "date": date} for row in rows]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Social Media"],
    normalizer=LinkedinOrganicNormalizer(
        flatten_max_level=3,
        epoch_columns=["time_range_start", "time_range_end"],
    ),
    icon="devicon:linkedin",
)
class LinkedinOrganic(il.Source):
    """LinkedIn Organization page organic analytics integration."""

    connection: LinkedinOrganicConnection

    organization_id: str = il.FetchField(
        provider="connection.organizations",
        label_key="name",
        value_key="id",
        description="LinkedIn Organization page ID",
        discriminator=True,
    )

    # -- INTERNALS -------------------------------------------------------------
    @property
    def _organization_urn(self) -> str:
        """The percent-encoded organization URN Rest.li 2.0 expects in a raw query.

        Returns:
            The encoded URN, e.g. ``urn%3Ali%3Aorganization%3A123``.
        """
        return f"urn%3Ali%3Aorganization%3A{self.organization_id}"

    async def _daily_statistics(self, path: str, finder: str, date: dt.date) -> list[dict[str, Any]]:
        """Fetch the DAY-granularity statistics of one UTC day from a time-bound finder.

        Used by ``page_stats`` and ``share_stats``, whose finders share the
        ``q={finder}&{finder}={urn}&timeIntervals=(...)`` shape. The query string
        is written out raw because Rest.li 2.0 rejects percent-encoded ``(``,
        ``:`` and ``,``.

        Args:
            path: The statistics endpoint relative to the API base URL.
            finder: The finder name, also the name of the organization parameter.
            date: The UTC day to fetch.

        Returns:
            The statistics elements for that day.
        """
        start = dt.datetime.combine(date, dt.time(), tzinfo=dt.timezone.utc)
        start_millis = int(start.timestamp() * 1000)
        end_millis = int((start + dt.timedelta(days=1)).timestamp() * 1000)
        query = "&".join(
            [
                f"q={finder}",
                f"{finder}={self._organization_urn}",
                f"timeIntervals=(timeGranularityType:DAY,timeRange:(start:{start_millis},end:{end_millis}))",
            ]
        )
        response = await self.connection.client.get(f"{path}?{query}")
        response.raise_for_status()
        return response.json().get("elements", [])

    async def _taxonomy(self, path: str) -> list[dict[str, Any]]:
        """Fetch every element of a standardized-data taxonomy, following its ``next`` links.

        Used by ``industries``, ``job_functions`` and ``seniorities``. The
        ``href`` of a ``paging.links`` entry is host-relative (``/rest/...``), so
        it is resolved against the API host rather than the ``/rest`` base URL.

        Args:
            path: The taxonomy endpoint relative to the API base URL.

        Returns:
            The elements of every page, in page order.
        """
        elements: list[dict[str, Any]] = []
        url = path
        while True:
            response = await self.connection.client.get(url)
            response.raise_for_status()
            body = response.json()
            elements.extend(body.get("elements", []))
            links = (body.get("paging") or {}).get("links") or []
            href = next((link.get("href") for link in links if link.get("rel") == "next"), None)
            if not href:
                return elements
            url = href if href.startswith("http") else f"{constants.API_URL}{href}"

    # -- STATISTICS ------------------------------------------------------------
    @il.asset(schema=FollowersStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def followers_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime follower counts of the organization page by function, geography, industry, seniority and more.

        Each partition holds the counts as they stood when it was loaded, not the followers gained that day.
        """
        query = f"q=organizationalEntity&organizationalEntity={self._organization_urn}"
        response = await self.connection.client.get(f"/organizationalEntityFollowerStatistics?{query}")
        response.raise_for_status()
        return _with_date(response.json().get("elements", []), context.partition_date)

    @il.asset(schema=PageStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def page_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily views of the organization page per tab and device, and custom button clicks."""
        rows = await self._daily_statistics("/organizationPageStatistics", "organization", context.partition_date)
        return _with_date(rows, context.partition_date)

    @il.asset(schema=ShareStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def share_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily organic impressions, clicks, likes, comments, shares and engagement over all the page's posts."""
        rows = await self._daily_statistics(
            "/organizationalEntityShareStatistics", "organizationalEntity", context.partition_date
        )
        return _with_date(rows, context.partition_date)

    # -- TAXONOMIES ------------------------------------------------------------
    @il.asset(schema=Industries, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def industries(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's industry taxonomy, to resolve the industry URNs of the follower statistics."""
        rows = await self._taxonomy("/industryTaxonomyVersions/DEFAULT/industries")
        return _with_date(rows, context.partition_date)

    @il.asset(schema=JobFunctions, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def job_functions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's job function taxonomy, to resolve the function URNs of the follower statistics."""
        rows = await self._taxonomy("/functions")
        return _with_date(rows, context.partition_date)

    @il.asset(schema=Seniorities, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def seniorities(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's seniority taxonomy, to resolve the seniority URNs of the follower statistics."""
        rows = await self._taxonomy("/seniorities")
        return _with_date(rows, context.partition_date)
