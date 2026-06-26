import interloper as il

from interloper_assets.instagram_insights.connection import InstagramInsightsConnection


@il.source(
    resources={"connection": InstagramInsightsConnection},
    tags=["Social Media"],
    icon="skill-icons:instagram",
)
class InstagramInsights(il.Source):
    """Instagram Business and Creator account insights integration."""

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Instagram Business or Creator account ID",
    )
