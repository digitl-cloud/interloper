import datetime as dt
import logging

import interloper as itlp
from interloper_assets import AmazonSellingPartnerMarketplace, amazon_selling_partner

itlp.basic_logging(logging.DEBUG)

amazon_selling_partner = amazon_selling_partner(
    location="EU",
    io={"file": itlp.FileIO("data")},
)

data = amazon_selling_partner.vendor_traffic.run(
    marketplace=AmazonSellingPartnerMarketplace.GERMANY,
    date=dt.date(2025, 2, 1),
)
print(data)
