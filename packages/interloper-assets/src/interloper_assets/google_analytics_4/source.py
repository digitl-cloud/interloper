"""GA4 export: the BigQuery export of a Google Analytics 4 property, referenced where it lands."""

from __future__ import annotations

from typing import Any

import interloper as il

from interloper_assets.google_analytics_4 import schemas


@il.source(tags=["Analytics"], icon="logos:google-analytics", maturity="alpha")
class GoogleAnalytics4Export(il.Source):
    """The BigQuery export of a Google Analytics 4 property.

    Google writes the export itself, one ``events_YYYYMMDD`` table per day in
    the ``analytics_<property id>`` dataset of the project the property
    exports to. ``events`` references those tables; nothing is fetched or
    written. The daily fact is a view over them, BigQuery-only, since the
    sharded export exists nowhere else.
    """

    property_id: str = il.InputField(
        description="The GA4 property id; the export lands in the dataset analytics_<property id>",
        discriminator=True,
    )

    events = il.reference("events_*", key="events", schema=schemas.Events, tags=["Event"])

    @il.view(schema=schemas.FactDailyEvents, tags=["Fact"], dialect="bigquery")
    def fact_daily_events(self, events: il.Upstream) -> str:
        """Events and distinct users per day and event name, intraday tables left out."""
        return (
            "SELECT PARSE_DATE('%Y%m%d', event_date) AS date, event_name, "
            "COUNT(*) AS events, COUNT(DISTINCT user_pseudo_id) AS users "
            f"FROM {events.data.name} WHERE _TABLE_SUFFIX NOT LIKE 'intraday_%' "
            "GROUP BY 1, 2"
        )

    def model_post_init(self, context: Any) -> None:
        """Default the dataset to the property's export dataset before the assets inherit it.

        Args:
            context: Pydantic's post-init context, forwarded untouched.
        """
        if not self.dataset:
            self.dataset = f"analytics_{self.property_id}"
        super().model_post_init(context)

    def asset_table(self, asset: il.Asset) -> str:
        """Name the view by its key alone, since each property already has a dataset of its own.

        Args:
            asset: The asset to name a table for; only its ``key`` is read.

        Returns:
            The asset's key, without the property id suffix.
        """
        return asset.key
