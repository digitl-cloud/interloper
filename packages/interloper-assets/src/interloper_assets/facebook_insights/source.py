
import interloper as il

from interloper_assets.facebook_insights.connection import FacebookInsightsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["Social Media"],
    icon="logos:facebook",
)
class FacebookInsights(il.Source):
    """Facebook Page and Post Insights integration."""

    connection: FacebookInsightsConnection

    page_id: str = il.FetchField(
        provider="connection.pages",
        label_key="name",
        value_key="id",
        description="Facebook Page to retrieve insights for",
        discriminator=True,
    )
