import datetime as dt
import logging

import interloper as itlp
from interloper_assets import criteo

itlp.basic_logging(logging.DEBUG)

data = criteo.campaigns.run(advertiser_id="1176", date=dt.date(2024, 1, 1))

print(data)
