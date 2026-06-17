import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ActionsBySku(Schema):
    """Listing of each action at the individual SKU level (adv_action_list_sku_pm_only report)"""

    action_date: dt.datetime | None = Field(default=None, description="Date when the action occurred.")
    action_id: str | None = Field(default=None, description="Unique identifier for the action.")
    action_tracker: str | None = Field(default=None, description="Name of the action tracker used for the product action.")
    at_id: str | None = Field(default=None, description="Identifier of the action tracker used for the product action.")
    batch_date: dt.date | None = Field(default=None, description="Date when the batch process occurred.")
    category: str | None = Field(default=None, description="Category of the product.")
    category_list: str | None = Field(default=None, description="List of categories associated with the product.")
    cost: float | None = Field(default=None, description="Cost associated with the product action.")
    media_partner: str | None = Field(default=None, description="Name of the media partner associated with the product action.")
    mp_id: str | None = Field(default=None, description="Identifier of the media partner associated with the product action.")
    oid: str | None = Field(default=None, description="Order identifier associated with the action.")
    product: str | None = Field(default=None, description="Name of the product.")
    product_category: str | None = Field(default=None, description="Detailed category of the product.")
    promo_code: str | None = Field(default=None, description="Promotional code applied to the product action.")
    quantity: int | None = Field(default=None, description="Quantity of the product in the action.")
    rate: float | None = Field(default=None, description="Rate applied to the product action.")
    ref_url: str | None = Field(default=None, description="Referring URL for the product action.")
    revenue: float | None = Field(default=None, description="Revenue generated from the product action.")
    sku: str | None = Field(default=None, description="Stock Keeping Unit identifier for the product.")
    status: str | None = Field(default=None, description="Status of the product action.")
    subcategory: str | None = Field(default=None, description="Subcategory of the product.")
    tax: float | None = Field(default=None, description="Tax applied to the product action.")
    date: dt.date | None = Field(default=None, description="Partition date")
