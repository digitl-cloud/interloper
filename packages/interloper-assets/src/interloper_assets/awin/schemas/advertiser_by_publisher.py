import datetime as dt
from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class AdvertiserByPublishers(itlp.TableSchema):
    """
    The Advertiser by Publishers report provides insights into their performance across publishers within the Awin network.
    It includes key metrics such as clicks, impressions, confirmed items, pending items, declined items, bonus items,
    and associated commission values.
    """  # noqa: E501

    date: dt.date = field(metadata={"description": "The date of the report"})
    advertiser_id: int = field(metadata={"description": "The ID of the advertiser"})
    advertiser_name: str = field(metadata={"description": "The name of the advertiser"})
    bonus_comm: float = field(metadata={"description": "The commission of bonus items"})
    bonus_no: int = field(metadata={"description": "The number of bonus items"})
    bonus_value: float = field(metadata={"description": "The value of bonus items"})
    clicks: int = field(metadata={"description": "The number of clicks"})
    confirmed_comm: float = field(metadata={"description": "The commission of confirmed items"})
    confirmed_no: int = field(metadata={"description": "The number of confirmed items"})
    confirmed_value: float = field(metadata={"description": "The value of confirmed items"})
    currency: str = field(metadata={"description": "The currency used by the advertiser"})
    declined_comm: float = field(metadata={"description": "The commission of declined items"})
    declined_no: int = field(metadata={"description": "The number of declined items"})
    declined_value: float = field(metadata={"description": "The value of declined items"})
    impressions: int = field(metadata={"description": "The number of impressions"})
    pending_comm: float = field(metadata={"description": "The commission of pending items"})
    pending_no: int = field(metadata={"description": "The number of pending items"})
    pending_value: float = field(metadata={"description": "The value of pending items"})
    publisher_id: int = field(metadata={"description": "The ID of the publisher"})
    publisher_name: str = field(metadata={"description": "The name of the publisher"})
    region: str = field(metadata={"description": "The region of the advertiser"})
    tags: str = field(metadata={"description": "The tags associated with the advertiser"})
    total_comm: float = field(metadata={"description": "The total commission of items"})
    total_no: int = field(metadata={"description": "The total number of items"})
    total_value: float = field(metadata={"description": "The total value of items"})
