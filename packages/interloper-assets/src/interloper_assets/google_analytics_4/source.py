"""GA4 export: the BigQuery export of a Google Analytics 4 property, referenced where it lands."""

from __future__ import annotations

import interloper as il

from interloper_assets.google_analytics_4 import schemas


@il.source(
    tags=["Analytics"],
    icon="logos:google-analytics",
    maturity="alpha",
    relations={"destinations": il.Relation("destination", "bigquery_destination", many=True, optional=True)},
)
class GoogleAnalytics4Export(il.Source):
    """The BigQuery export of a Google Analytics 4 property.

    Google writes the export itself, one ``events_YYYYMMDD`` table per day in
    the ``analytics_<property id>`` dataset of the project the property
    exports to. ``events`` references those tables, which are read and never
    written. The daily fact is a view over them; it is created in the
    source's own dataset, not in Google's export dataset, one per property.
    The source binds BigQuery destinations only, since the sharded export
    exists nowhere else.
    """

    property_id: str = il.InputField(
        description="The GA4 property id; the export lands in the dataset analytics_<property id>",
        discriminator=True,
    )

    events = il.reference(
        "events_*",
        key="events",
        dataset="analytics_{property_id}",  # noqa: RUF027
        schema=schemas.Events,
        tags=["Event"],
    )

    @il.view(schema=schemas.FactDailyEvents, tags=["Fact"], dialect="bigquery")
    def fact_daily_events(self, events: il.Upstream) -> str:
        """Events and distinct users per day and event name, intraday tables left out."""
        return (
            "SELECT PARSE_DATE('%Y%m%d', event_date) AS date, event_name, "
            "COUNT(*) AS events, COUNT(DISTINCT user_pseudo_id) AS users "
            f"FROM {events.data.name} WHERE _TABLE_SUFFIX NOT LIKE 'intraday_%' "
            "GROUP BY 1, 2"
        )
