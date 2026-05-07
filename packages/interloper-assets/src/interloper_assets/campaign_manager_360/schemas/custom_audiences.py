from interloper.schema import Schema
from pydantic import Field


class CustomAudiences(Schema):
    """Remarketing audience lists with population rules and lifecycle attributes."""

    id: str | None = Field(description="Unique identifier for the audience")
    account_id: str | None = Field(description="Identifier for the account associated with the audience")
    advertiser_id: str | None = Field(description="Identifier for the advertiser associated with the audience")
    name: str | None = Field(description="Name of the audience")
    description: str | None = Field(description="Description of the audience")
    active: bool | None = Field(description="Indicates whether the audience is active")
    list_size: str | None = Field(description="Size of the audience list")
    life_span: str | None = Field(description="Lifespan of the audience")
    list_source: str | None = Field(description="Source of the audience list")
    kind: str | None = Field(description="Type or category of the audience")
    advertiser_id_dimension_value_dimension_name: str | None = Field(description="Dimension name for the advertiser ID")
    advertiser_id_dimension_value_value: str | None = Field(description="Value of the advertiser ID dimension")
    advertiser_id_dimension_value_kind: str | None = Field(description="Kind of the advertiser ID dimension value")
    advertiser_id_dimension_value_etag: str | None = Field(description="ETag for the advertiser ID dimension value")
    list_population_rule_floodlight_activity_id: str | None = Field(description="Floodlight activity ID for the list population rule")
    list_population_rule_floodlight_activity_name: str | None = Field(description="Floodlight activity name for the list population rule")
    list_population_rule_list_population_clauses: str | None = Field(description="Clauses for the list population rule")
