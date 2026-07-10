
import interloper as il

from interloper_assets.facebook_insights.connection import FacebookInsightsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    resources={"connection": FacebookInsightsConnection},
    tags=["Social Media"],
    icon="logos:facebook",
)
class FacebookInsights(il.Source):
    """Facebook Page and Post Insights integration."""

    page_id: str = il.FetchField(
        provider="connection.pages",
        label_key="name",
        value_key="id",
        description="Facebook Page to retrieve insights for",
    )
