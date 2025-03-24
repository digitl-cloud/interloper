import datetime as dt
from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class AdvertiserTransactions(itlp.TableSchema):
    """
    The Advertiser Transactions report provides insights into their transactional activities,
    including clicks, commissions, sales, and associated parameters.
    It includes key metrics such as sale amount, transaction date, and commission amount.
    """

    date: dt.date = field(metadata={"description": "The date of the report"})
    advertiser_country: str = field(
        metadata={"description": "The country of the advertiser associated with the transaction"}
    )
    advertiser_id: int = field(metadata={"description": "The ID of the advertiser associated with the transaction"})
    amend_reason: str = field(metadata={"description": "The reason for the amendment of the transaction"})
    amended: bool = field(metadata={"description": "Indicates if the transaction has been amended"})
    campaign: str = field(metadata={"description": "The campaign associated with the transaction"})
    click_date: dt.datetime = field(metadata={"description": "The date of the click for the transaction"})
    click_device: str = field(metadata={"description": "The device used for the click associated with the transaction"})
    click_ref: str = field(
        metadata={"description": "The reference number of the click associated with the transaction"}
    )
    commission_amount: float = field(metadata={"description": "The amount of commission for the transaction"})
    commission_currency: str = field(metadata={"description": "The currency of the commission for the transaction"})
    commission_status: str = field(metadata={"description": "The status of the commission for the transaction"})
    custom_parameters: str = field(metadata={"description": "The custom parameters associated with the transaction"})
    customer_country: str = field(
        metadata={"description": "The country of the customer associated with the transaction"}
    )
    decline_reason: str = field(metadata={"description": "The reason for the decline of the transaction"})
    id: int = field(metadata={"description": "The unique identifier for the transaction"})
    ip_hash: str = field(metadata={"description": "The hashed IP address associated with the transaction"})
    lapse_time: int = field(metadata={"description": "The lapse time for the transaction"})
    network_fee_amount: float = field(metadata={"description": "The amount of network fee for the transaction"})
    network_fee_currency: str = field(metadata={"description": "The currency of the network fee for the transaction"})
    old_commission_amount: float = field(
        metadata={"description": "The previous amount of commission for the transaction"}
    )
    old_commission_currency: str = field(
        metadata={"description": "The previous currency of the commission for the transaction"}
    )
    old_sale_amount: float = field(metadata={"description": "The previous amount of sale for the transaction"})
    old_sale_currency: str = field(metadata={"description": "The previous currency of the sale for the transaction"})
    order_ref: str = field(
        metadata={"description": "The reference number of the order associated with the transaction"}
    )
    paid_to_publisher: bool = field(
        metadata={"description": "Indicates if the transaction has been paid to the publisher"}
    )
    payment_id: int = field(metadata={"description": "The ID of the payment associated with the transaction"})
    publisher_id: int = field(metadata={"description": "The ID of the publisher associated with the transaction"})
    publisher_url: str = field(metadata={"description": "The URL of the publisher associated with the transaction"})
    sale_amount: float = field(metadata={"description": "The amount of sale for the transaction"})
    sale_currency: str = field(metadata={"description": "The currency of the sale for the transaction"})
    site_name: str = field(metadata={"description": "The name of the site associated with the transaction"})
    transaction_date: dt.datetime = field(metadata={"description": "The date of the transaction"})
    transaction_device: str = field(metadata={"description": "The device used for the transaction"})
    transaction_parts: str = field(metadata={"description": "The parts of the transaction"})
    transaction_query_id: int = field(metadata={"description": "The ID of the query associated with the transaction"})
    type: str = field(metadata={"description": "The type of the transaction"})
    url: str = field(metadata={"description": "The URL associated with the transaction"})
    validation_date: dt.datetime = field(metadata={"description": "The date of the validation for the transaction"})
    voucher_code: str = field(metadata={"description": "The voucher code used for the transaction"})
    voucher_code_used: bool = field(
        metadata={"description": "Indicates if a voucher code was used for the transaction"}
    )
