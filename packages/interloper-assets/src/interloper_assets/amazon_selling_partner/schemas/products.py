from interloper.schema import Schema
from pydantic import Field


class Products(Schema):
    """Product identifier lookup: one row per ASIN with its external identifiers.

    Sourced from the Data Kiosk vendor analytics ``sourcingView`` group-by keys.
    """

    asin: str | None = Field(default=None, description="Amazon Standard Identification Number for the product.")
    parent_asin: str | None = Field(
        default=None,
        description=(
            "Generic overall parent ASIN (non-purchaseable) for different variations of a product. "
            "Different variations have a different ASIN but the same parent ASIN."
        ),
    )
    ean: str | None = Field(default=None, description="European Article Number")
    upc: str | None = Field(default=None, description="Universal Product Code")
    isbn_13: str | None = Field(
        default=None,
        description="13-digit International Standard Book Number. Only applicable to products that are books.",
    )
