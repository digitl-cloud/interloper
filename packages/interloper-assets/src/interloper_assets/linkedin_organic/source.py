
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": LinkedinOrganicConnection},
    tags=["Social Media"],
    normalizer=DataFrameNormalizer(flatten_max_level=3),
    icon="devicon:linkedin",
)
class LinkedinOrganic(il.Source):
    """LinkedIn Organization page organic analytics integration."""

    organization_id: str = il.FetchField(
        provider="connection.organizations",
        label_key="name",
        value_key="id",
        description="LinkedIn Organization page ID",
    )
